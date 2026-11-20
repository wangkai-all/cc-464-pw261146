"""End-to-end tests for the incremental materialized-view engine.

Run with:  python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone

from app import db, engine


def _days(n: int = 3) -> list[str]:
    today = datetime.now(timezone.utc).date()
    return [(today - timedelta(days=n - 1 - i)).isoformat() for i in range(n)]


class EngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        db.init_db(self.tmp.name)
        self.conn = db.connect(self.tmp.name)
        self.days = _days(5)
        self.facts = [
            {"key": f"{d}-{i}", "category": "books" if i % 2 == 0 else "games",
             "value": 10.0 + i, "occurred_at": f"{d}T10:00:00+00:00"}
            for d in self.days for i in range(3)
        ]

    def tearDown(self) -> None:
        self.conn.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.tmp.name + suffix)
            except FileNotFoundError:
                pass

    # -- helpers -------------------------------------------------------
    def build(self, **kw) -> dict:
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn, **kw)
        return engine.run_job(self.conn, job["job"]["id"])

    def day_total(self, day: str) -> float:
        q = engine.query_view(self.conn, day=day)
        return round(q["total_amount"], 4)

    def day_count(self, day: str) -> int:
        return engine.query_view(self.conn, day=day)["total_cnt"]

    # -- basic materialization ----------------------------------------
    def test_initial_build_covers_all_partitions(self):
        result = self.build()
        self.assertEqual(result["job"]["status"], "success")
        self.assertEqual(sorted(result["swapped"]), sorted(self.days))
        q = engine.query_view(self.conn)
        self.assertEqual(q["version"], 1)
        self.assertEqual(q["total_cnt"], 15)
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_late_update_only_touches_affected_partition(self):
        self.build()
        target = self.days[2]
        engine.record_facts(self.conn, [
            {"key": f"{target}-0", "value": 500.0}])
        pending = engine.view_meta(self.conn)["pending_partitions"]
        self.assertEqual([p["day"] for p in pending], [target])

        job = engine.trigger_job(self.conn)
        self.assertEqual(job["job"]["partitions"], [target])
        run = engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(run["swapped"], [target])
        # -0 held 10.0, replaced by 500.0 -> day total 33 - 10 + 500
        self.assertEqual(self.day_total(target), 523.0)
        # untouched partitions were not recomputed into a new version
        gens = engine.view_meta(self.conn)["partition_generations"]
        other = [g for g in gens if g["day"] == self.days[0]]
        self.assertEqual(other[0]["version"], 1)
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_late_delete_removes_row(self):
        self.build()
        day = self.days[1]
        before = self.day_count(day)
        engine.record_facts(self.conn, [{"key": f"{day}-0", "op": "delete"}])
        job = engine.trigger_job(self.conn)
        engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(self.day_count(day), before - 1)
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_cross_partition_move_dirties_both(self):
        self.build()
        src, dst = self.days[0], self.days[-1]
        src_before, dst_before = self.day_count(src), self.day_count(dst)
        engine.record_facts(self.conn, [
            {"key": f"{src}-0", "value": 77.0, "occurred_at": f"{dst}T09:00:00+00:00"}])
        self.assertEqual(
            sorted(p["day"] for p in engine.view_meta(self.conn)["pending_partitions"]),
            sorted([src, dst]))
        job = engine.trigger_job(self.conn)
        engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(self.day_count(src), src_before - 1)
        self.assertEqual(self.day_count(dst), dst_before + 1)
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_no_changes_produces_noop(self):
        self.build()
        job = engine.trigger_job(self.conn)
        self.assertEqual(job["job"]["status"], "noop")
        self.assertEqual(job["job"]["partitions"], [])
        self.assertEqual(engine.view_meta(self.conn)["version"], 1)

    # -- versioning & watermark ----------------------------------------
    def test_facts_newer_than_watermark_do_not_leak(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn)
        watermark = job["job"]["watermark"]
        # a write lands after the job snapshotted its watermark
        engine.record_facts(self.conn, [{"key": f"{self.days[0]}-0", "value": 12345.0}])
        engine.run_job(self.conn, job["job"]["id"])
        q = engine.query_view(self.conn)
        # value 10.0 for -0 must still be the old one
        self.assertNotIn(12345.0, [d["total"] for d in q["days"]])
        pending = engine.view_meta(self.conn)["pending_partitions"]
        self.assertEqual([p["day"] for p in pending], [self.days[0]])
        # next run picks it up
        job2 = engine.trigger_job(self.conn)
        engine.run_job(self.conn, job2["job"]["id"])
        self.assertEqual(self.day_total(self.days[0]), 33.0 - 10.0 + 12345.0)

    def test_old_version_remains_queryable(self):
        self.build()
        v1 = engine.query_view(self.conn)
        day = self.days[3]
        engine.record_facts(self.conn, [{"key": f"{day}-0", "value": 999.0}])
        job = engine.trigger_job(self.conn)
        engine.run_job(self.conn, job["job"]["id"])
        v2 = engine.query_view(self.conn)
        self.assertEqual(v2["version"], 2)
        old = engine.query_view(self.conn, version=1)
        self.assertEqual(old["version"], 1)
        self.assertTrue(old["stale"])
        self.assertEqual(old["total_amount"], v1["total_amount"])
        self.assertNotEqual(v2["total_amount"], v1["total_amount"])

    def test_queries_never_see_mixed_versions(self):
        """Hammer the view while a backfill swaps; totals must match a version."""
        self.build()
        baseline = engine.query_view(self.conn)
        target = self.days[4]
        engine.record_facts(self.conn, [{"key": f"{target}-0", "value": 5000.0}])
        job = engine.trigger_job(self.conn, slow_ms=40)

        observed = []
        stop = threading.Event()

        def reader():
            conn = db.connect(self.tmp.name)
            try:
                while not stop.is_set():
                    q = engine.query_view(conn)
                    observed.append((q["version"], q["total_cnt"], round(q["total_amount"], 4)))
            finally:
                conn.close()

        t = threading.Thread(target=reader)
        t.start()
        try:
            engine.run_job(self.conn, job["job"]["id"])
        finally:
            stop.set()
            t.join()

        expected_old = (1, baseline["total_cnt"], round(baseline["total_amount"], 4))
        expected_new = engine.query_view(self.conn)
        expected_new = (2, expected_new["total_cnt"],
                        round(expected_new["total_amount"], 4))
        self.assertTrue(observed)
        for row in observed:
            self.assertIn(row, (expected_old, expected_new), f"mixed snapshot {row}")

    # -- locking & duplicates ------------------------------------------
    def test_duplicate_trigger_is_deduped(self):
        engine.record_facts(self.conn, self.facts)
        first = engine.trigger_job(self.conn)
        second = engine.trigger_job(self.conn)
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["job"]["id"], second["job"]["id"])
        self.assertEqual(
            len(engine.list_jobs(self.conn)), 1)

    def test_queued_job_superseded_by_injected_trigger(self):
        """A queued job must not swallow an explicit crash/failure option."""
        engine.record_facts(self.conn, self.facts)
        plain = engine.trigger_job(self.conn)
        injected = engine.trigger_job(self.conn, crash_at=1)
        self.assertFalse(injected["duplicate"])
        self.assertNotEqual(injected["job"]["id"], plain["job"]["id"])
        self.assertEqual(injected["job"]["crash_at"], 1)
        self.assertEqual(engine.get_job(self.conn, plain["job"]["id"])["status"],
                         "superseded")

    def test_running_job_is_never_superseded(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn)
        with db.transaction(self.conn):
            self.conn.execute("UPDATE maintenance_job SET status='running' WHERE id=?",
                              (job["job"]["id"],))
        again = engine.trigger_job(self.conn, crash_at=1)
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["job"]["id"], job["job"]["id"])

    def test_duplicate_execution_is_serialized_by_lock(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn)
        engine.run_job(self.conn, job["job"]["id"])
        # a second run of the same job must be rejected (status is now success)
        with self.assertRaises(engine.EngineError):
            engine.run_job(self.conn, job["job"]["id"])

    def test_lock_blocks_concurrent_worker(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn)
        token = engine.acquire_lock(self.conn, "other-worker")
        try:
            with self.assertRaises(engine.LockBusy):
                engine.run_job(self.conn, job["job"]["id"])
        finally:
            engine.release_lock(self.conn, token)
        # after release the queued job runs fine
        result = engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(result["job"]["status"], "success")

    def test_expired_lock_is_stolen(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn)
        engine.acquire_lock(self.conn, "dead-worker", ttl=0.05)
        time.sleep(0.1)
        result = engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(result["job"]["status"], "success")

    # -- failure & retry ------------------------------------------------
    def test_transient_failures_are_retried_within_job(self):
        engine.record_facts(self.conn, self.facts)
        day = self.days[0]
        job = engine.trigger_job(self.conn, fail_plan={day: 2}, max_attempts=3)
        result = engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(result["job"]["status"], "success")
        first = [p for p in result["job"]["progress"] if p["day"] == day]
        self.assertEqual([p["status"] for p in first], ["failed", "failed", "computed"])
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_exhausted_job_fails_without_bumping_version(self):
        engine.record_facts(self.conn, self.facts)
        day = self.days[1]
        job = engine.trigger_job(self.conn, fail_plan={day: 99}, max_attempts=3)
        with self.assertRaises(engine.InjectedFailure):
            engine.run_job(self.conn, job["job"]["id"])
        failed = engine.get_job(self.conn, job["job"]["id"])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(engine.view_meta(self.conn)["version"], 0)
        # dirty partitions are preserved for a later attempt
        pending = [p["day"] for p in engine.view_meta(self.conn)["pending_partitions"]]
        self.assertEqual(sorted(pending), sorted(self.days))

    def test_operator_retry_starts_clean_attempt(self):
        engine.record_facts(self.conn, self.facts)
        day = self.days[2]
        job = engine.trigger_job(self.conn, fail_plan={day: 99}, max_attempts=3)
        with self.assertRaises(engine.InjectedFailure):
            engine.run_job(self.conn, job["job"]["id"])
        retry = engine.retry_job(self.conn, job["job"]["id"])
        self.assertEqual(retry["job"]["attempt"], 2)
        self.assertEqual(retry["job"]["fail_plan"], {})
        result = engine.run_job(self.conn, retry["job"]["id"])
        self.assertEqual(result["job"]["status"], "success")
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_retry_exhaustion_is_rejected(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn, fail_plan={self.days[0]: 99},
                                 max_attempts=1)
        with self.assertRaises(engine.InjectedFailure):
            engine.run_job(self.conn, job["job"]["id"])
        with self.assertRaises(engine.EngineError):
            engine.retry_job(self.conn, job["job"]["id"])

    # -- interruption & recovery ---------------------------------------
    def test_crash_keeps_old_version_and_staging(self):
        self.build()
        old = engine.query_view(self.conn)
        day = self.days[1]
        engine.record_facts(self.conn, [{"key": f"{day}-0", "value": 321.0}])
        job = engine.trigger_job(self.conn, crash_at=0)
        with self.assertRaises(engine.SimulatedCrash):
            engine.run_job(self.conn, job["job"]["id"])
        crashed = engine.get_job(self.conn, job["job"]["id"])
        self.assertEqual(crashed["status"], "interrupted")
        # committed view untouched
        now = engine.query_view(self.conn)
        self.assertEqual(now["version"], old["version"])
        self.assertEqual(now["total_amount"], old["total_amount"])
        # successful digest still validates
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_crash_resume_reuses_staging(self):
        self.build()
        target = self.days[2]
        engine.record_facts(self.conn, [
            {"key": f"{d}-0", "value": 50.0 + i}
            for i, d in enumerate([self.days[2], self.days[3], self.days[4]])])
        job = engine.trigger_job(self.conn, crash_at=2)
        with self.assertRaises(engine.SimulatedCrash):
            engine.run_job(self.conn, job["job"]["id"])
        staged = [r["day"] for r in self.conn.execute(
            "SELECT day FROM view_partition_staging WHERE task_id=? ORDER BY day",
            (job["job"]["id"],))]
        self.assertEqual(staged, sorted([self.days[2], self.days[3]]))

        time.sleep(0.05)
        resumed = engine.resume_job(self.conn, job["job"]["id"])
        result = engine.run_job(self.conn, resumed["job"]["id"], lock_wait=3)
        self.assertEqual(result["job"]["status"], "success")
        self.assertEqual(sorted(result["reused"]), staged)
        self.assertEqual(sorted(result["swapped"]), sorted(job["job"]["partitions"]))
        self.assertEqual(engine.view_meta(self.conn)["version"], 2)
        self.assertTrue(engine.verify_view(self.conn)["ok"])

    def test_resume_requires_interrupted_job(self):
        self.build()
        job = engine.trigger_job(self.conn)  # noop
        with self.assertRaises(engine.EngineError):
            engine.resume_job(self.conn, job["job"]["id"])

    def test_cancel_mid_run_interrupts(self):
        engine.record_facts(self.conn, self.facts)
        job = engine.trigger_job(self.conn, slow_ms=30)
        with db.transaction(self.conn):
            self.conn.execute("UPDATE maintenance_job SET cancel_requested=1 WHERE id=?",
                              (job["job"]["id"],))
        with self.assertRaises(engine.SimulatedCrash):
            engine.run_job(self.conn, job["job"]["id"])
        self.assertEqual(engine.get_job(self.conn, job["job"]["id"])["status"], "interrupted")
        self.assertEqual(engine.view_meta(self.conn)["version"], 0)

    # -- verify --------------------------------------------------------
    def test_verify_detects_tampering(self):
        self.build()
        self.assertTrue(engine.verify_view(self.conn)["ok"])
        with db.transaction(self.conn):
            self.conn.execute(
                "UPDATE view_partition SET checksum='deadbeef' WHERE day=?",
                (self.days[0],))
        report = engine.verify_view(self.conn)
        self.assertFalse(report["ok"])
        self.assertEqual(report["drift"], [self.days[0]])

    def test_verify_detects_missing_partition_rows(self):
        """A partition whose aggregate rows vanished must fail verification."""
        self.build()
        day = self.days[1]
        with db.transaction(self.conn):
            self.conn.execute("DELETE FROM mv_partition_row WHERE day=?", (day,))
        report = engine.verify_view(self.conn)
        self.assertFalse(report["ok"])
        self.assertIn(day, report["drift"])
        self.assertFalse(next(p for p in report["partitions"] if p["day"] == day)["rows_ok"])

    def test_repeated_partial_backfills_keep_every_partition(self):
        """Regression: per-day generation trimming must not drop sibling rows.

        Each partition has several category rows; trimming previous generations
        by row rather than by (day, version) once deleted whole partitions.
        After many rounds, every partition must still be queryable and complete.
        """
        self.build()
        for round_no in range(4):
            day = self.days[round_no % len(self.days)]
            engine.record_facts(self.conn, [{"key": f"{day}-0", "value": 100.0 + round_no}])
            job = engine.trigger_job(self.conn)
            engine.run_job(self.conn, job["job"]["id"])
            q = engine.query_view(self.conn)
            self.assertEqual(sorted(d["day"] for d in q["days"]), sorted(self.days),
                             f"partitions missing after round {round_no}")
            self.assertTrue(engine.verify_view(self.conn)["ok"],
                            f"verify failed after round {round_no}")
        self.assertEqual(engine.view_meta(self.conn)["version"], 5)


if __name__ == "__main__":
    unittest.main()
