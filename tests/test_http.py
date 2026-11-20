"""HTTP-level tests: boot the real server on a random port against a temp DB."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app import db, engine, server


class HttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls.tmp.close()
        db.init_db(cls.tmp.name)
        cls.orig_db_path = db.DB_PATH
        db.DB_PATH = cls.tmp.name
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        cls.httpd.daemon_threads = True
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.httpd.server_close()
        db.DB_PATH = cls.orig_db_path
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(cls.tmp.name + suffix)
            except FileNotFoundError:
                pass

    def call(self, path: str, method: str = "GET", body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(req, timeout=10) as res:
                return res.status, json.loads(res.read() or b"{}")
        except HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def test_full_workflow_over_http(self):
        status, _ = self.call("/api/reset", "POST", {})
        self.assertEqual(status, 200)

        status, seeded = self.call("/api/seed", "POST", {"day_count": 3, "per_day": 3})
        self.assertEqual(status, 200)
        self.assertEqual(seeded["seeded"], 9)

        status, triggered = self.call("/api/trigger", "POST", {})
        self.assertEqual(status, 200)
        job_id = triggered["job"]["id"]

        status, dup = self.call("/api/trigger", "POST", {})
        self.assertTrue(dup["duplicate"])

        status, run = self.call("/api/run", "POST", {})
        self.assertEqual(run["job"]["status"], "success")
        self.assertEqual(len(run["swapped"]), 3)

        status, view = self.call("/api/view")
        self.assertEqual(view["version"], 1)
        self.assertEqual(view["total_cnt"], 9)

        status, report = self.call("/api/verify")
        self.assertTrue(report["ok"])

        status, job = self.call(f"/api/job?id={job_id}")
        kinds = [e["kind"] for e in job["events"]]
        self.assertIn("triggered", kinds)
        self.assertIn("swapped", kinds)

        # late update -> single dirty partition
        status, late = self.call("/api/chaos", "POST", {"scenario": "late_update"})
        self.assertEqual(len(late["dirty_days"]), 1)
        status, state = self.call("/api/state")
        self.assertEqual(len(state["view"]["pending_partitions"]), 1)

    def test_http_error_shapes(self):
        status, err = self.call("/api/trigger", "POST", {"name": "nope"})
        self.assertEqual(status, 400)
        self.assertIn("error", err)

        status, err = self.call("/api/facts", "POST", {"facts": []})
        self.assertEqual(status, 400)

        status, err = self.call("/api/job?id=missing")
        self.assertEqual(status, 400)

        status, err = self.call("/api/run-job", "POST", {"id": "nope"})
        self.assertEqual(status, 400)

    def test_crash_and_resume_over_http(self):
        self.call("/api/reset", "POST", {})
        self.call("/api/seed", "POST", {"day_count": 4, "per_day": 2, "seed": 3})
        status, trig = self.call("/api/trigger", "POST", {"crash_at": 2})
        job_id = trig["job"]["id"]
        status, res = self.call("/api/run-job", "POST", {"id": job_id})
        self.assertEqual(status, 202)
        self.assertTrue(res["interrupted"])

        status, job = self.call(f"/api/job?id={job_id}")
        self.assertEqual(job["job"]["status"], "interrupted")

        status, resumed = self.call("/api/resume", "POST", {"id": job_id})
        self.assertEqual(status, 200)
        new_id = resumed["job"]["id"]

        # bypass the crash lock quickly and finish
        status, out = self.call("/api/release-lock", "POST", {})
        self.assertEqual(status, 200)
        status, run = self.call("/api/run-job", "POST", {"id": new_id})
        self.assertEqual(run["job"]["status"], "success")
        self.assertEqual(len(run["reused"]), 2)

        status, report = self.call("/api/verify")
        self.assertTrue(report["ok"])

    def test_static_assets_served(self):
        for path, needle in (("/", b"<html"), ("/static/app.js", b"refresh"),
                             ("/static/style.css", b"--accent")):
            with urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=10) as res:
                self.assertEqual(res.status, 200)
                self.assertIn(needle, res.read())


if __name__ == "__main__":
    unittest.main()
