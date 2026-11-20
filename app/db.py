"""SQLite storage layer and schema for the incremental materialized-view engine.

The base fact table is append-with-version, not mutable rows.  Every fact
*event* is an immutable operation (insert / update / delete) stamped with a
monotonic ``fact_seq``.  The logical value of a fact is obtained by taking its
latest event at or below a watermark.

Materialized views are partitioned by ``day`` (the partition key is derived from
``occurred_at``).  Every view partition row records the fact watermark it was
built from and a checksum over its aggregate rows, which lets ``verify`` detect
any partition that drifted from its base data without recomputing everything.

Consistency model
-----------------
A maintenance run computes all affected partitions into a staging area and then
swaps them **and** bumps ``view_meta.version`` in one SQLite transaction.  A
query pins a version (implicitly the committed one) and reads only partition
rows whose version is ``<= pinned``; WAL gives the reader a consistent snapshot.
So during a backfill every query sees either the complete old version or the
complete new version, never a mix.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.environ.get(
    "MV_DB_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "mv.db")
)

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- Immutable stream of fact events.  key + seq identifies one version of a fact.
CREATE TABLE IF NOT EXISTS fact_event (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    key         TEXT    NOT NULL,
    day         TEXT    NOT NULL,          -- partition the fact version belongs to
    category    TEXT    NOT NULL,
    op          TEXT    NOT NULL CHECK (op IN ('upsert', 'delete')),
    value       REAL,                      -- NULL for delete
    occurred_at TEXT    NOT NULL,
    ingested_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_key_seq ON fact_event(key, seq);
CREATE INDEX IF NOT EXISTS idx_fact_day_seq ON fact_event(day, seq);

-- One row per materialized view.
CREATE TABLE IF NOT EXISTS view_meta (
    name        TEXT PRIMARY KEY,
    version     INTEGER NOT NULL DEFAULT 0,
    watermark   INTEGER NOT NULL DEFAULT 0, -- highest fact_seq materialized
    updated_at  TEXT,
    low_water   TEXT,                       -- oldest pending partition day
    high_water  TEXT                        -- newest observed partition day
);

-- A committed materialized partition.  Multiple versions are retained so in
-- flight readers can keep reading the version they pinned; superseded versions
-- are garbage-collected after the new version commits.
CREATE TABLE IF NOT EXISTS view_partition (
    name        TEXT NOT NULL,
    day         TEXT NOT NULL,
    version     INTEGER NOT NULL,
    watermark   INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    fact_sum    REAL NOT NULL,
    checksum    TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (name, day, version)
);

-- Staging area written by a maintenance task before the atomic swap.
CREATE TABLE IF NOT EXISTS view_partition_staging (
    task_id     TEXT NOT NULL,
    name        TEXT NOT NULL,
    day         TEXT NOT NULL,
    version     INTEGER NOT NULL,
    watermark   INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    fact_sum    REAL NOT NULL,
    checksum    TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (task_id, day)
);

-- Staged aggregate rows, one per (day, category).
CREATE TABLE IF NOT EXISTS mv_partition_row_staging (
    task_id  TEXT NOT NULL,
    name     TEXT NOT NULL,
    day      TEXT NOT NULL,
    category TEXT NOT NULL,
    cnt      INTEGER NOT NULL,
    total    REAL NOT NULL,
    PRIMARY KEY (task_id, day, category)
);

-- Committed aggregate rows, keyed by the view version that produced them.
CREATE TABLE IF NOT EXISTS mv_partition_row (
    name     TEXT NOT NULL,
    day      TEXT NOT NULL,
    version  INTEGER NOT NULL,
    category TEXT NOT NULL,
    cnt      INTEGER NOT NULL,
    total    REAL NOT NULL,
    PRIMARY KEY (name, day, version, category)
);

-- Dirty partition markers produced by late fact writes.
CREATE TABLE IF NOT EXISTS dirty_partition (
    name       TEXT NOT NULL,
    day        TEXT NOT NULL,
    fact_floor INTEGER NOT NULL,           -- min seq that touched the partition
    fact_ceil  INTEGER NOT NULL,           -- max seq that touched the partition
    marked_at  TEXT NOT NULL,
    PRIMARY KEY (name, day)
);

-- Maintenance task ledger: version, watermark, retry and lock in one place.
CREATE TABLE IF NOT EXISTS maintenance_job (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,            -- queued|running|success|failed|interrupted|aborted|noop
    attempt      INTEGER NOT NULL DEFAULT 1,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    version_from INTEGER NOT NULL,
    version_to   INTEGER NOT NULL,
    watermark    INTEGER NOT NULL,         -- fact_seq ceiling this job may see
    partitions   TEXT NOT NULL,            -- JSON array of days
    swapped      TEXT NOT NULL DEFAULT '[]',
    reused       TEXT NOT NULL DEFAULT '[]', -- staging reused after an interruption
    progress     TEXT NOT NULL DEFAULT '[]',
    fail_plan    TEXT NOT NULL DEFAULT '{}', -- injected failures per day (for tests)
    crash_at     INTEGER,                  -- simulate crash after N partitions
    slow_ms      INTEGER NOT NULL DEFAULT 0,
    error        TEXT,
    duration_ms  INTEGER,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    finished_at  TEXT,
    heartbeat_at TEXT,
    lock_owner   TEXT,
    lock_token   TEXT,
    lock_expires TEXT,
    retry_of     TEXT,
    resumed_from TEXT,
    staging_task TEXT,                       -- task id whose staging rows may be reused
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_job_created ON maintenance_job(created_at DESC);

-- Single-row lock table for the whole maintenance subsystem.
CREATE TABLE IF NOT EXISTS maintenance_lock (
    name        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    token       TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at  TEXT NOT NULL
);

-- Per-partition circuit breaker.
CREATE TABLE IF NOT EXISTS partition_failure (
    name        TEXT NOT NULL,
    day         TEXT NOT NULL,
    fail_count  INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    quarantined INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (name, day)
);

-- Immutable event log; powers the UI and proves interruption / retry behaviour.
CREATE TABLE IF NOT EXISTS maintenance_event (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    day        TEXT,
    detail     TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_maint_event_job ON maintenance_event(job_id, id);
"""

VIEW_DEFS = {
    "sales_daily": {
        "label": "日销售额 / 订单数",
        "description": "按 occurred_at 日期分区，聚合金额与笔数，支持迟到更新与删除",
        "partition_by": "day",
        "measures": ["cnt", "total"],
    }
}


def connect(path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE so writers serialise and readers keep a consistent view."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db(path: str | None = None) -> None:
    resolved = path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(resolved)), exist_ok=True)
    conn = connect(resolved)
    try:
        conn.executescript(SCHEMA)
        for name in VIEW_DEFS:
            conn.execute(
                "INSERT OR IGNORE INTO view_meta(name, version) VALUES (?, 0)", (name,)
            )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(f"initialised {os.path.abspath(DB_PATH)}")
