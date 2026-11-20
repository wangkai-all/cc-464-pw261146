"""Incremental materialized-view engine.

Lifecycle of one maintenance run::

    trigger()                      # snapshot watermark + affected partitions
       -> acquire global lock      # one maintenance writer at a time
       -> compute each partition   # into staging, keyed by task_id
       -> retry failed partitions
       -> atomic swap + version bump (single transaction)
       -> release lock

If the process dies mid-run the lock expires, staging survives, and
``resume_job`` reuses the already-computed partitions.  Fact writes that land
while a run is in flight are stamped with a newer ``fact_seq`` and simply
remain dirty for the next run; they can never leak into an in-flight version
because every read is bounded by the job's watermark.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from . import db

LOCK_TTL_SECONDS = float(os.environ.get("MV_LOCK_TTL", "30"))
CRASH_LOCK_TTL_SECONDS = float(os.environ.get("MV_CRASH_LOCK_TTL", "1.5"))


class EngineError(Exception):
    status = 400


class LockBusy(EngineError):
    status = 409


class InjectedFailure(EngineError):
    status = 500


class ConcurrentVersion(EngineError):
    status = 409


class SimulatedCrash(Exception):
    """Raised to mimic a worker dying; staging and the lock row remain."""


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime | None = None) -> str:
    return (ts or now()).isoformat(timespec="seconds")


def day_of(occurred_at: str) -> str:
    day = occurred_at[:10]
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise EngineError(
            f"occurred_at must start with YYYY-MM-DD: {occurred_at!r}"
        ) from exc
    return day


def checksum(rows: list[tuple[str, int, float]]) -> str:
    payload = "|".join(f"{cat}:{cnt}:{total:.6f}" for cat, cnt, total in sorted(rows))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Fact ingestion
# ---------------------------------------------------------------------------


def record_facts(conn, facts: list[dict], views: list[str] | None = None) -> dict:
    """Append fact events and mark every touched partition dirty.

    A late update that moves a fact across day boundaries dirties both the old
    and the new partition.  Nothing is recomputed here: dirty markers plus the
    event sequence are the whole change-capture mechanism.
    """
    if not facts:
        raise EngineError("no facts supplied")
    views = views or list(db.VIEW_DEFS)
    inserted: list[dict] = []
    dirty_days: set[str] = set()
    ts = iso()
    with db.transaction(conn):
        for fact in facts:
            key = str(fact.get("key") or "").strip()
            op = fact.get("op", "upsert")
            if not key:
                raise EngineError("fact.key is required")
            if op not in ("upsert", "delete"):
                raise EngineError("fact.op must be upsert or delete")

            prev = conn.execute(
                "SELECT day, category, occurred_at FROM fact_event "
                "WHERE key=? ORDER BY seq DESC LIMIT 1",
                (key,),
            ).fetchone()

            if op == "delete":
                if prev is None:
                    raise EngineError(f"cannot delete unknown key {key!r}")
                occurred_at = fact.get("occurred_at") or prev["occurred_at"]
                category = fact.get("category") or prev["category"]
                value = None
            else:
                occurred_at = (
                    fact.get("occurred_at")
                    or (prev["occurred_at"] if prev is not None else None)
                    or ts
                )
                category = str(fact.get("category") or (prev and prev["category"]) or "unknown")
                if "value" not in fact:
                    raise EngineError(f"fact.value is required for upsert of {key!r}")
                value = float(fact["value"])

            day = day_of(occurred_at)
            if prev is not None and prev["day"] != day:
                dirty_days.add(prev["day"])
            dirty_days.add(day)

            cur = conn.execute(
                """INSERT INTO fact_event(key, day, category, op, value, occurred_at, ingested_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (key, day, category, op, value, occurred_at, ts),
            )
            inserted.append({"seq": cur.lastrowid, "key": key, "day": day, "op": op})

        floor, ceil = inserted[0]["seq"], inserted[-1]["seq"]
        for view in views:
            for day in dirty_days:
                _mark_dirty(conn, view, day, floor, ceil, ts)
    return {
        "inserted": inserted,
        "dirty_days": sorted(dirty_days),
        "watermark": inserted[-1]["seq"],
    }


def _mark_dirty(conn, view: str, day: str, floor: int, ceil: int, ts: str) -> None:
    conn.execute(
        """INSERT INTO dirty_partition(name, day, fact_floor, fact_ceil, marked_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(name, day) DO UPDATE SET
               fact_floor = MIN(dirty_partition.fact_floor, excluded.fact_floor),
               fact_ceil  = MAX(dirty_partition.fact_ceil, excluded.fact_ceil),
               marked_at  = excluded.marked_at""",
        (view, day, floor, ceil, ts),
    )


def effective_facts(conn, watermark: int) -> dict[str, dict]:
    """Logical facts at a watermark: newest event per key, deletes removed."""
    rows = conn.execute(
        """SELECT e.* FROM fact_event e
           JOIN (SELECT key, MAX(seq) AS seq FROM fact_event WHERE seq<=? GROUP BY key) l
             ON e.seq = l.seq
           WHERE e.op='upsert'""",
        (watermark,),
    ).fetchall()
    return {r["key"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------


def acquire_lock(conn, owner: str, ttl: float = LOCK_TTL_SECONDS,
                 wait: float = 0.0) -> str:
    """Take the maintenance lock.  Expired locks (crashed holder) are stolen."""
    token = uuid.uuid4().hex
    deadline = time.monotonic() + wait
    while True:
        with db.transaction(conn):
            row = conn.execute(
                "SELECT * FROM maintenance_lock WHERE name='maintenance'"
            ).fetchone()
            current = now()
            if row is None or datetime.fromisoformat(row["expires_at"]) <= current:
                conn.execute(
                    """INSERT INTO maintenance_lock(name, owner, token, acquired_at, expires_at)
                       VALUES ('maintenance', ?, ?, ?, ?)
                       ON CONFLICT(name) DO UPDATE SET
                           owner=excluded.owner, token=excluded.token,
                           acquired_at=excluded.acquired_at, expires_at=excluded.expires_at""",
                    (owner, token, iso(current), iso(current + timedelta(seconds=ttl))),
                )
                return token
            holder, busy_until = row["owner"], row["expires_at"]
        if time.monotonic() >= deadline:
            raise LockBusy(f"maintenance lock held by {holder} until {busy_until}")
        time.sleep(0.05)


def refresh_lock(conn, owner: str, token: str, ttl: float = LOCK_TTL_SECONDS) -> None:
    with db.transaction(conn):
        conn.execute(
            "UPDATE maintenance_lock SET owner=?, expires_at=? "
            "WHERE name='maintenance' AND token=?",
            (owner, iso(now() + timedelta(seconds=ttl)), token),
        )


def release_lock(conn, token: str | None = None) -> None:
    with db.transaction(conn):
        if token:
            conn.execute(
                "DELETE FROM maintenance_lock WHERE name='maintenance' AND token=?", (token,)
            )
        else:
            conn.execute("DELETE FROM maintenance_lock WHERE name='maintenance'")
        conn.execute(
            "UPDATE maintenance_job SET lock_owner=NULL, lock_token=NULL, lock_expires=NULL "
            "WHERE status='running'"
        )


def lock_status(conn) -> dict | None:
    row = conn.execute("SELECT * FROM maintenance_lock WHERE name='maintenance'").fetchone()
    if row is None:
        return None
    return {
        "owner": row["owner"],
        "acquired_at": row["acquired_at"],
        "expires_at": row["expires_at"],
        "expired": datetime.fromisoformat(row["expires_at"]) <= now(),
    }


# ---------------------------------------------------------------------------
# Job creation / retry / resume
# ---------------------------------------------------------------------------


def _event(conn, job_id: str, kind: str, detail: str = "", day: str | None = None) -> None:
    conn.execute(
        "INSERT INTO maintenance_event(job_id, kind, day, detail, created_at) VALUES (?,?,?,?,?)",
        (job_id, kind, day, detail, iso()),
    )


def _affected_partitions(conn, name: str) -> list[str]:
    meta = conn.execute("SELECT * FROM view_meta WHERE name=?", (name,)).fetchone()
    days = {r["day"] for r in conn.execute(
        "SELECT day FROM dirty_partition WHERE name=?", (name,)
    ).fetchall()}
    if meta["version"] == 0:
        # First build has no committed partitions: materialize every fact day.
        days |= {r["day"] for r in conn.execute(
            "SELECT DISTINCT day FROM fact_event"
        ).fetchall()}
    return sorted(days)


def trigger_job(
    conn,
    name: str = "sales_daily",
    partitions: list[str] | None = None,
    max_attempts: int = 3,
    fail_plan: dict | None = None,
    slow_ms: int = 0,
    crash_at: int | None = None,
    owner: str = "api",
    dedupe: bool = True,
) -> dict:
    """Create (or dedupe against) a maintenance job.

    The job snapshots the current fact watermark and the set of affected
    partitions.  Concurrent duplicate triggers return the in-flight job instead
    of starting a second backfill.
    """
    if name not in db.VIEW_DEFS:
        raise EngineError(f"unknown view {name!r}")
    injected = bool(fail_plan) or crash_at is not None or slow_ms > 0
    with db.transaction(conn):
        if dedupe:
            matches = conn.execute(
                "SELECT * FROM maintenance_job WHERE name=? "
                "AND status IN ('queued','running') ORDER BY created_at LIMIT 1",
                (name,),
            ).fetchall()
            for existing in matches:
                if existing["status"] == "running":
                    _event(conn, existing["id"], "duplicate_trigger",
                           "duplicate trigger deduped (run in progress)")
                    return {"job": get_job(conn, existing["id"]), "duplicate": True}
                # A queued job that has not started yet may be superseded when
                # the caller asks for explicit test injection, so the crash /
                # failure options of the newer request are not silently lost.
                same_options = (
                    (json.loads(existing["fail_plan"] or "{}") or not injected)
                    and existing["crash_at"] == crash_at
                )
                if same_options:
                    _event(conn, existing["id"], "duplicate_trigger",
                           "duplicate trigger deduped")
                    return {"job": get_job(conn, existing["id"]), "duplicate": True}
                conn.execute(
                    "UPDATE maintenance_job SET status='superseded', finished_at=?, "
                    "error='superseded by a newer trigger' WHERE id=?",
                    (iso(), existing["id"]),
                )
                _event(conn, existing["id"], "superseded",
                       "superseded by a newer trigger with explicit options")

        days = sorted(set(partitions)) if partitions else _affected_partitions(conn, name)
        meta = conn.execute("SELECT * FROM view_meta WHERE name=?", (name,)).fetchone()
        watermark = conn.execute(
            "SELECT COALESCE(MAX(seq),0) AS s FROM fact_event"
        ).fetchone()["s"]
        job_id = uuid.uuid4().hex[:12]
        status = "queued" if days else "noop"
        version_to = meta["version"] + 1 if days else meta["version"]
        conn.execute(
            """INSERT INTO maintenance_job(
                   id, name, status, attempt, max_attempts, version_from, version_to,
                   watermark, partitions, fail_plan, crash_at, slow_ms, created_at,
                   finished_at, duration_ms)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                job_id, name, status, 1, max_attempts, meta["version"], version_to,
                watermark, json.dumps(days), json.dumps(fail_plan or {}), crash_at,
                slow_ms, iso(), iso() if status == "noop" else None,
                0 if status == "noop" else None,
            ),
        )
        _event(
            conn, job_id, "triggered",
            f"watermark={watermark} partitions={len(days)} "
            f"version {meta['version']}->{version_to}",
        )
        for day in days:
            _event(conn, job_id, "partition_pending", day=day)
    return {"job": get_job(conn, job_id), "duplicate": False}


def get_job(conn, job_id: str) -> dict:
    row = conn.execute("SELECT * FROM maintenance_job WHERE id=?", (job_id,)).fetchone()
    if row is None:
        raise EngineError(f"job {job_id!r} not found")
    return job_to_dict(row)


def job_to_dict(row) -> dict:
    job = dict(row)
    for key in ("partitions", "swapped", "reused", "progress"):
        job[key] = json.loads(job.get(key) or "[]")
    job["fail_plan"] = json.loads(job.get("fail_plan") or "{}")
    return job


def retry_job(conn, job_id: str, owner: str = "api") -> dict:
    """Retry a failed/interrupted job as a new attempt, preserving the watermark.

    The failure plan is *not* inherited: an operator retry should get a clean
    attempt rather than deterministically hitting the same injected failure.
    """
    with db.transaction(conn):
        job = get_job(conn, job_id)
        if job["status"] not in ("failed", "interrupted"):
            raise EngineError(f"job {job_id} is {job['status']}, cannot retry")
        if job["attempt"] >= job["max_attempts"]:
            raise EngineError(f"job {job_id} exhausted {job['max_attempts']} attempts")
        new_id = uuid.uuid4().hex[:12]
        conn.execute(
            """INSERT INTO maintenance_job(
                   id, name, status, attempt, max_attempts, version_from, version_to,
                   watermark, partitions, fail_plan, slow_ms, created_at,
                   retry_of, staging_task)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                new_id, job["name"], "queued", job["attempt"] + 1, job["max_attempts"],
                job["version_from"], job["version_from"] + 1, job["watermark"],
                json.dumps(job["partitions"]), "{}",
                job["slow_ms"], iso(), job_id, job_id,
            ),
        )
        conn.execute(
            "UPDATE maintenance_job SET status='retried', finished_at=? WHERE id=?",
            (iso(), job_id),
        )
        _event(conn, new_id, "retry", f"retry of {job_id} attempt {job['attempt'] + 1}")
    return {"job": get_job(conn, new_id), "duplicate": False}


def resume_job(conn, job_id: str, owner: str = "resume") -> dict:
    """Resume an interrupted job, reusing staging already computed by it."""
    with db.transaction(conn):
        job = get_job(conn, job_id)
        if job["status"] != "interrupted":
            raise EngineError(
                f"job {job_id} is {job['status']}, only interrupted jobs resume"
            )
        new_id = uuid.uuid4().hex[:12]
        conn.execute(
            """INSERT INTO maintenance_job(
                   id, name, status, attempt, max_attempts, version_from, version_to,
                   watermark, partitions, fail_plan, slow_ms, created_at,
                   resumed_from, staging_task)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                new_id, job["name"], "queued", job["attempt"], job["max_attempts"],
                job["version_from"], job["version_to"], job["watermark"],
                json.dumps(job["partitions"]), json.dumps(job["fail_plan"]), 0,
                iso(), job_id, job_id,
            ),
        )
        _event(conn, new_id, "resume", f"resume of interrupted {job_id}")
    return {"job": get_job(conn, new_id), "duplicate": False}


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def compute_partition(conn, day: str, watermark: int) -> dict:
    """Aggregate the effective facts belonging to one day at a watermark.

    For every key that has *any* event in this partition, the globally newest
    event at or below the watermark decides the key's fate:

    * deleted            -> excluded everywhere,
    * moved to another day -> excluded from this day,
    * still here         -> aggregated with its current category/value.

    Looking at the global latest event (not the day-local latest) is what makes
    cross-partition moves and deletes correct.
    """
    rows = conn.execute(
        """WITH touched AS (
               SELECT DISTINCT key FROM fact_event WHERE day=? AND seq<=?
           ),
           latest AS (
               SELECT e.key, MAX(e.seq) AS seq
               FROM fact_event e
               JOIN touched t ON t.key = e.key
               WHERE e.seq <= ?
               GROUP BY e.key
           )
           SELECT e.category AS category, COUNT(*) AS cnt,
                  COALESCE(SUM(e.value), 0) AS total
           FROM fact_event e
           JOIN latest l ON e.seq = l.seq
           WHERE e.op='upsert' AND e.day=?
           GROUP BY e.category
           ORDER BY e.category""",
        (day, watermark, watermark, day),
    ).fetchall()
    aggregates = [(r["category"], int(r["cnt"]), float(r["total"])) for r in rows]
    return {
        "day": day,
        "rows": aggregates,
        "row_count": sum(cnt for _, cnt, _ in aggregates),
        "fact_sum": sum(total for _, _, total in aggregates),
        "checksum": checksum(aggregates),
    }


def _abort_as_interrupted(conn, job_id: str, reason: str, duration: int) -> None:
    """Leave staging + a soon-expiring lock behind, as a dying worker would."""
    with db.transaction(conn):
        conn.execute(
            "UPDATE maintenance_job SET status='interrupted', finished_at=?, error=?, "
            "duration_ms=? WHERE id=?",
            (iso(), reason, duration, job_id),
        )
        conn.execute(
            "UPDATE maintenance_lock SET expires_at=? WHERE name='maintenance'",
            (iso(now() + timedelta(seconds=CRASH_LOCK_TTL_SECONDS)),),
        )
        _event(conn, job_id, "crashed", reason)


def _check_cancel(conn, job_id: str) -> None:
    row = conn.execute(
        "SELECT cancel_requested FROM maintenance_job WHERE id=?", (job_id,)
    ).fetchone()
    if row and row["cancel_requested"]:
        raise SimulatedCrash("cancel requested")


def run_job(conn, job_id: str, owner: str = "worker", lock_wait: float = 0.0) -> dict:
    """Execute one queued job.  Raises LockBusy if another worker holds the lock."""
    job = get_job(conn, job_id)
    if job["status"] != "queued":
        raise EngineError(f"job {job_id} is {job['status']}, cannot run")
    if not job["partitions"]:
        return {"job": job, "swapped": [], "reused": []}

    token = acquire_lock(conn, owner=owner, wait=lock_wait)
    started = time.monotonic()
    reused: list[str] = []
    swapped: list[str] = []
    progress: list[dict] = []
    crashed = False
    try:
        with db.transaction(conn):
            conn.execute(
                """UPDATE maintenance_job SET status='running', started_at=?, heartbeat_at=?,
                       lock_owner=?, lock_token=?, lock_expires=?
                   WHERE id=?""",
                (iso(), iso(), owner, token,
                 iso(now() + timedelta(seconds=LOCK_TTL_SECONDS)), job_id),
            )
            _event(conn, job_id, "started", f"lock {token[:8]} owner={owner}")

        for index, day in enumerate(job["partitions"]):
            try:
                _check_cancel(conn, job_id)
            except SimulatedCrash as exc:
                crashed = True
                _abort_as_interrupted(
                    conn, job_id, str(exc),
                    int((time.monotonic() - started) * 1000),
                )
                raise
            if job["crash_at"] is not None and index >= job["crash_at"]:
                crashed = True
                _abort_as_interrupted(
                    conn, job_id, f"simulated crash after {index} partitions",
                    int((time.monotonic() - started) * 1000),
                )
                raise SimulatedCrash(day)

            refresh_lock(conn, owner, token)

            staged = None
            if job["staging_task"]:
                staged = conn.execute(
                    "SELECT * FROM view_partition_staging WHERE task_id=? AND day=?",
                    (job["staging_task"], day),
                ).fetchone()
            if staged is not None:
                reused.append(day)
                with db.transaction(conn):
                    # Copy the surviving staging rows under the new task id so
                    # the atomic swap can commit them uniformly.
                    conn.execute(
                        """INSERT OR REPLACE INTO view_partition_staging(
                               task_id, name, day, version, watermark, row_count, fact_sum,
                               checksum, computed_at)
                           SELECT ?, name, day, ?, ?, row_count, fact_sum, checksum, computed_at
                           FROM view_partition_staging WHERE task_id=? AND day=?""",
                        (job_id, job["version_to"], job["watermark"],
                         job["staging_task"], day),
                    )
                    conn.execute(
                        "DELETE FROM mv_partition_row_staging WHERE task_id=? AND day=?",
                        (job_id, day),
                    )
                    conn.execute(
                        """INSERT INTO mv_partition_row_staging(
                               task_id, name, day, category, cnt, total)
                           SELECT ?, name, day, category, cnt, total
                           FROM mv_partition_row_staging WHERE task_id=? AND day=?""",
                        (job_id, job["staging_task"], day),
                    )
                    _event(conn, job_id, "partition_reused", day=day,
                           detail=f"reused staging checksum={staged['checksum']}")
                progress.append({"day": day, "status": "reused",
                                 "attempt": job["attempt"]})
            else:
                result = _compute_with_retry(conn, job, day, progress)
                with db.transaction(conn):
                    conn.execute(
                        """INSERT OR REPLACE INTO view_partition_staging(
                               task_id, name, day, version, watermark, row_count, fact_sum,
                               checksum, computed_at)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (job_id, job["name"], day, job["version_to"], job["watermark"],
                         result["row_count"], result["fact_sum"], result["checksum"], iso()),
                    )
                    conn.execute(
                        "DELETE FROM mv_partition_row_staging WHERE task_id=? AND day=?",
                        (job_id, day),
                    )
                    conn.executemany(
                        """INSERT INTO mv_partition_row_staging(
                               task_id, name, day, category, cnt, total)
                           VALUES (?,?,?,?,?,?)""",
                        [(job_id, job["name"], day, cat, cnt, total)
                         for cat, cnt, total in result["rows"]],
                    )
                    _event(conn, job_id, "partition_staged", day=day,
                           detail=f"rows={result['row_count']} checksum={result['checksum']}")

            if job["slow_ms"]:
                time.sleep(job["slow_ms"] / 1000.0)
                refresh_lock(conn, owner, token)

            with db.transaction(conn):
                conn.execute(
                    "UPDATE maintenance_job SET progress=?, heartbeat_at=? WHERE id=?",
                    (json.dumps(progress), iso(), job_id),
                )

        _swap(conn, job, job_id, started, swapped, reused)
    except SimulatedCrash:
        raise
    except Exception as exc:
        duration = int((time.monotonic() - started) * 1000)
        with db.transaction(conn):
            current = conn.execute(
                "SELECT status FROM maintenance_job WHERE id=?", (job_id,)
            ).fetchone()
            if current is not None and current["status"] not in ("interrupted", "success"):
                conn.execute(
                    "UPDATE maintenance_job SET status='failed', error=?, finished_at=?, "
                    "duration_ms=? WHERE id=?",
                    (str(exc)[:500], iso(), duration, job_id),
                )
                _event(conn, job_id, "failed", str(exc)[:300])
        raise
    finally:
        if not crashed:
            row = conn.execute(
                "SELECT token FROM maintenance_lock WHERE name='maintenance'"
            ).fetchone()
            if row is not None and row["token"] == token:
                release_lock(conn, token)

    return {"job": get_job(conn, job_id), "swapped": swapped, "reused": reused}


def _swap(conn, job: dict, job_id: str, started: float,
          swapped: list[str], reused: list[str]) -> None:
    """Commit every staged partition and the version bump in one transaction."""
    with db.transaction(conn):
        fresh = get_job(conn, job_id)
        meta = conn.execute("SELECT * FROM view_meta WHERE name=?", (job["name"],)).fetchone()
        if meta["version"] != fresh["version_from"]:
            raise ConcurrentVersion(
                f"view version moved {fresh['version_from']}->{meta['version']} during run"
            )
        new_version = fresh["version_from"] + 1
        for day in fresh["partitions"]:
            st = conn.execute(
                "SELECT * FROM view_partition_staging WHERE task_id=? AND day=?",
                (job_id, day),
            ).fetchone()
            if st is None:
                raise EngineError(f"staging missing for {day}")
            conn.execute(
                """INSERT INTO view_partition(name, day, version, watermark, row_count,
                       fact_sum, checksum, computed_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(name, day, version) DO UPDATE SET
                       watermark=excluded.watermark, row_count=excluded.row_count,
                       fact_sum=excluded.fact_sum, checksum=excluded.checksum,
                       computed_at=excluded.computed_at""",
                (job["name"], day, new_version, fresh["watermark"], st["row_count"],
                 st["fact_sum"], st["checksum"], st["computed_at"]),
            )
            conn.execute(
                "DELETE FROM mv_partition_row WHERE name=? AND day=? AND version=?",
                (job["name"], day, new_version),
            )
            staged_rows = conn.execute(
                "SELECT category, cnt, total FROM mv_partition_row_staging "
                "WHERE task_id=? AND day=?",
                (job_id, day),
            ).fetchall()
            conn.executemany(
                """INSERT INTO mv_partition_row(name, day, version, category, cnt, total)
                   VALUES (?,?,?,?,?,?)""",
                [(job["name"], day, new_version, r["category"], r["cnt"], r["total"])
                 for r in staged_rows],
            )
            swapped.append(day)

        # Clear only dirty markers covered by the job watermark; newer changes
        # remain pending for the next run.
        for day in fresh["partitions"]:
            conn.execute(
                "DELETE FROM dirty_partition WHERE name=? AND day=? AND fact_ceil<=?",
                (job["name"], day, fresh["watermark"]),
            )
            conn.execute(
                "UPDATE dirty_partition SET fact_floor=MAX(fact_floor, ?) "
                "WHERE name=? AND day=? AND fact_ceil>?",
                (fresh["watermark"] + 1, job["name"], day, fresh["watermark"]),
            )
            conn.execute(
                "DELETE FROM partition_failure WHERE name=? AND day=?",
                (job["name"], day),
            )

        # Retain the two newest generations *per day* so a slow reader can still
        # pin the version it started on.  Trimming per day matters: unaffected
        # partitions legitimately stay many versions behind the global version.
        for table in ("view_partition", "mv_partition_row"):
            per_day = conn.execute(
                f"SELECT DISTINCT day, version FROM {table} WHERE name=? "
                "ORDER BY day, version DESC",
                (job["name"],),
            ).fetchall()
            keep: dict[str, list[int]] = {}
            for r in per_day:
                keep.setdefault(r["day"], []).append(r["version"])
            stale = [
                (job["name"], day, version)
                for day, versions in keep.items()
                for version in versions[2:]
            ]
            if stale:
                conn.executemany(
                    f"DELETE FROM {table} WHERE name=? AND day=? AND version=?", stale
                )
        conn.execute("DELETE FROM view_partition_staging WHERE task_id=?", (job_id,))
        conn.execute("DELETE FROM mv_partition_row_staging WHERE task_id=?", (job_id,))

        high = conn.execute("SELECT MAX(day) AS d FROM fact_event").fetchone()["d"]
        low = conn.execute(
            "SELECT MIN(day) AS d FROM dirty_partition WHERE name=?", (job["name"],)
        ).fetchone()["d"]
        duration = int((time.monotonic() - started) * 1000)
        conn.execute(
            """UPDATE view_meta SET version=?, watermark=?, updated_at=?,
                   low_water=?, high_water=? WHERE name=?""",
            (new_version, fresh["watermark"], iso(), low, high, job["name"]),
        )
        conn.execute(
            """UPDATE maintenance_job SET status='success', version_to=?, swapped=?,
                   reused=?, finished_at=?, duration_ms=?, error=NULL WHERE id=?""",
            (new_version, json.dumps(swapped), json.dumps(reused), iso(), duration, job_id),
        )
        _event(
            conn, job_id, "swapped",
            f"version {fresh['version_from']}->{new_version} "
            f"partitions={len(swapped)} watermark={fresh['watermark']}",
        )


def _compute_with_retry(conn, job: dict, day: str, progress: list[dict]) -> dict:
    last_error = None
    for attempt in range(1, job["max_attempts"] + 1):
        plan = dict(job["fail_plan"])
        remaining = int(plan.get(day, 0))
        if remaining > 0:
            plan[day] = remaining - 1
            last_error = f"injected failure on {day} (attempt {attempt})"
            with db.transaction(conn):
                conn.execute("UPDATE maintenance_job SET fail_plan=? WHERE id=?",
                             (json.dumps(plan), job["id"]))
                _register_partition_failure(conn, job["name"], day, last_error)
                _event(conn, job["id"], "partition_failed", day=day,
                       detail=f"injected failure attempt {attempt}")
            job["fail_plan"] = plan
            progress.append({"day": day, "status": "failed", "attempt": attempt,
                             "error": last_error})
            with db.transaction(conn):
                conn.execute("UPDATE maintenance_job SET progress=? WHERE id=?",
                             (json.dumps(progress), job["id"]))
            time.sleep(0.05)
            continue
        try:
            result = compute_partition(conn, day, job["watermark"])
        except Exception as exc:
            last_error = str(exc)
            with db.transaction(conn):
                _register_partition_failure(conn, job["name"], day, last_error)
                _event(conn, job["id"], "partition_failed", day=day,
                       detail=last_error[:200])
            progress.append({"day": day, "status": "failed", "attempt": attempt,
                             "error": last_error})
            continue
        with db.transaction(conn):
            conn.execute("DELETE FROM partition_failure WHERE name=? AND day=?",
                         (job["name"], day))
            _event(conn, job["id"], "partition_computed", day=day,
                   detail=f"rows={result['row_count']} checksum={result['checksum']}")
        progress.append({"day": day, "status": "computed", "attempt": attempt})
        with db.transaction(conn):
            conn.execute("UPDATE maintenance_job SET progress=? WHERE id=?",
                         (json.dumps(progress), job["id"]))
        return result
    raise InjectedFailure(
        f"partition {day} failed after {job['max_attempts']} attempts: {last_error}"
    )


def _register_partition_failure(conn, name: str, day: str, error: str) -> None:
    conn.execute(
        """INSERT INTO partition_failure(name, day, fail_count, last_error, updated_at)
           VALUES (?,?,1,?,?)
           ON CONFLICT(name, day) DO UPDATE SET
               fail_count=partition_failure.fail_count+1,
               last_error=excluded.last_error,
               updated_at=excluded.updated_at""",
        (name, day, error[:500], iso()),
    )


def run_next(conn, owner: str = "worker", lock_wait: float = 0.0) -> dict | None:
    row = conn.execute(
        "SELECT id FROM maintenance_job WHERE status='queued' "
        "ORDER BY created_at, rowid LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return run_job(conn, row["id"], owner=owner, lock_wait=lock_wait)


# ---------------------------------------------------------------------------
# Query path
# ---------------------------------------------------------------------------


def view_meta(conn, name: str = "sales_daily") -> dict:
    row = conn.execute("SELECT * FROM view_meta WHERE name=?", (name,)).fetchone()
    if row is None:
        raise EngineError(f"unknown view {name!r}")
    meta = dict(row)
    meta["pending_partitions"] = [
        dict(r) for r in conn.execute(
            "SELECT day, fact_floor, fact_ceil FROM dirty_partition WHERE name=? ORDER BY day",
            (name,),
        ).fetchall()
    ]
    meta["committed_partitions"] = [
        r["day"] for r in conn.execute(
            "SELECT day FROM view_partition WHERE name=? AND version=? ORDER BY day",
            (name, meta["version"]),
        ).fetchall()
    ]
    meta["partition_generations"] = [
        dict(r) for r in conn.execute(
            "SELECT day, version, watermark, row_count, fact_sum, checksum, computed_at "
            "FROM view_partition WHERE name=? ORDER BY day DESC", (name,)
        ).fetchall()
    ]
    return meta


def query_view(conn, name: str = "sales_daily", version: int | None = None,
               day: str | None = None) -> dict:
    """Read a consistent snapshot of the view.

    ``version`` pins a generation.  Without it the currently committed version
    is used.  The read runs inside one transaction so it is a single point in
    time; because the swap is atomic it can never mix partitions from two
    versions.
    """
    conn.execute("BEGIN")
    try:
        meta = conn.execute("SELECT * FROM view_meta WHERE name=?", (name,)).fetchone()
        if meta is None:
            raise EngineError(f"unknown view {name!r}")
        pinned = int(version) if version is not None else meta["version"]
        day_filter = " AND day = ?" if day else ""
        sql = f"""SELECT day, version, category, cnt, total
                  FROM mv_partition_row
                  WHERE name=? AND version = (
                      SELECT MAX(v2.version) FROM mv_partition_row v2
                      WHERE v2.name = mv_partition_row.name
                        AND v2.day = mv_partition_row.day
                        AND v2.version <= ?)
                    {day_filter}
                  ORDER BY day, category"""
        args: list = [name, pinned]
        if day:
            args.append(day)
        rows = conn.execute(sql, args).fetchall()
        by_day: dict[str, dict] = {}
        for r in rows:
            bucket = by_day.setdefault(
                r["day"],
                {"day": r["day"], "version": r["version"], "cnt": 0, "total": 0.0,
                 "categories": []},
            )
            bucket["cnt"] += r["cnt"]
            bucket["total"] += r["total"]
            bucket["categories"].append(
                {"category": r["category"], "cnt": r["cnt"], "total": r["total"]}
            )
        days = sorted(by_day.values(), key=lambda d: d["day"])
        for d in days:
            d["total"] = round(d["total"], 6)
        return {
            "view": name,
            "requested_version": int(version) if version is not None else None,
            "version": pinned,
            "committed_version": meta["version"],
            "watermark": meta["watermark"],
            "stale": pinned != meta["version"],
            "total_cnt": sum(d["cnt"] for d in days),
            "total_amount": round(sum(d["total"] for d in days), 6),
            "days": days,
        }
    finally:
        conn.execute("COMMIT")


def verify_view(conn, name: str = "sales_daily") -> dict:
    """Recompute every committed partition and check internal consistency.

    Two independent checks run per partition:

    1. the stored checksum matches a fresh recomputation from the fact stream
       at the stored watermark (detects base-data drift), and
    2. the persisted aggregate rows match the partition metadata's row count
       and fact sum (detects a partition whose rows went missing, which a
       checksum-only comparison cannot see).
    """
    conn.execute("BEGIN")
    try:
        parts = conn.execute(
            "SELECT * FROM view_partition WHERE name=? ORDER BY day", (name,)
        ).fetchall()
        results, drift = [], []
        for part in parts:
            expected = compute_partition(conn, part["day"], part["watermark"])
            rows = conn.execute(
                "SELECT category, cnt, total FROM mv_partition_row "
                "WHERE name=? AND day=? AND version=?",
                (name, part["day"], part["version"]),
            ).fetchall()
            stored_cnt = sum(r["cnt"] for r in rows)
            stored_sum = round(sum(r["total"] for r in rows), 6)
            rows_ok = (
                stored_cnt == part["row_count"]
                and abs(stored_sum - part["fact_sum"]) < 1e-6
                and checksum([(r["category"], r["cnt"], r["total"]) for r in rows])
                == part["checksum"]
            )
            checksum_ok = expected["checksum"] == part["checksum"]
            ok = checksum_ok and rows_ok
            results.append({
                "day": part["day"], "version": part["version"],
                "watermark": part["watermark"], "checksum": part["checksum"],
                "expected_checksum": expected["checksum"],
                "checksum_ok": checksum_ok, "rows_ok": rows_ok, "ok": ok,
                "stored_sum": round(part["fact_sum"], 6),
                "expected_sum": round(expected["fact_sum"], 6),
            })
            if not ok:
                drift.append(part["day"])
        return {"view": name, "partitions": results, "drift": drift, "ok": not drift}
    finally:
        conn.execute("COMMIT")


def job_events(conn, job_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM maintenance_event WHERE job_id=? ORDER BY id", (job_id,)
    ).fetchall()]


def list_jobs(conn, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM maintenance_job ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [job_to_dict(r) for r in rows]


def stats(conn) -> dict:
    fact = conn.execute(
        "SELECT COUNT(*) AS events, COUNT(DISTINCT key) AS keys, "
        "COALESCE(MAX(seq),0) AS watermark FROM fact_event"
    ).fetchone()
    return {
        "fact_events": fact["events"],
        "fact_keys": fact["keys"],
        "fact_watermark": fact["watermark"],
        "view": view_meta(conn),
        "lock": lock_status(conn),
        "server_time": iso(),
    }
