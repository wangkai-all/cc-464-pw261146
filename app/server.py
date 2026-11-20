"""Minimal JSON HTTP front-end for the incremental MV engine (stdlib only)."""

from __future__ import annotations

import json
import os
import random
import sys
import threading
import traceback
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import db, engine

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


class Handler(BaseHTTPRequestHandler):
    server_version = "MVEngine/1.0"
    protocol_version = "HTTP/1.1"

    # -- helpers ---------------------------------------------------------
    def _send(self, status: int, body: bytes, ctype: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, payload) -> None:
        self._send(status, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise engine.EngineError(f"invalid JSON body: {exc}") from exc

    def log_message(self, fmt, *args):  # keep the console readable
        if os.environ.get("MV_HTTP_LOG"):
            super().log_message(fmt, *args)

    # -- routing ---------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)
        path, query = parsed.path, parse_qs(parsed.query)
        try:
            if path in ("/", "/index.html"):
                return self._static("index.html")
            if path == "/static/app.js":
                return self._static("app.js")
            if path == "/static/style.css":
                return self._static("style.css")
            conn = db.connect()
            try:
                if path == "/api/state":
                    return self._json(200, _state(conn))
                if path == "/api/meta":
                    return self._json(200, {"views": db.VIEW_DEFS})
                if path == "/api/view":
                    name = _q(query, "name", "sales_daily")
                    version = query.get("version", [None])[0]
                    day = query.get("day", [None])[0]
                    return self._json(200, engine.query_view(
                        conn, name, int(version) if version else None, day))
                if path == "/api/verify":
                    return self._json(200, engine.verify_view(
                        conn, _q(query, "name", "sales_daily")))
                if path == "/api/jobs":
                    return self._json(200, {"jobs": engine.list_jobs(conn)})
                if path == "/api/job":
                    job_id = query.get("id", [None])[0]
                    if not job_id:
                        raise engine.EngineError("id query parameter is required")
                    return self._json(200, {
                        "job": engine.get_job(conn, job_id),
                        "events": engine.job_events(conn, job_id),
                    })
                if path == "/api/facts":
                    limit = int(_q(query, "limit", "50"))
                    rows = conn.execute(
                        "SELECT seq, key, day, category, op, value, occurred_at, ingested_at "
                        "FROM fact_event ORDER BY seq DESC LIMIT ?", (limit,)
                    ).fetchall()
                    return self._json(200, {"facts": [dict(r) for r in rows]})
                raise engine.EngineError(f"no route for {path}")
            finally:
                conn.close()
        except engine.EngineError as exc:
            return self._json(exc.status, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - unexpected
            traceback.print_exc()
            return self._json(500, {"error": str(exc)})

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            body = self._read_json()
            conn = db.connect()
            try:
                if path == "/api/facts":
                    result = engine.record_facts(conn, body.get("facts") or [])
                    return self._json(200, result)
                if path == "/api/trigger":
                    result = engine.trigger_job(
                        conn,
                        name=body.get("name", "sales_daily"),
                        partitions=body.get("partitions"),
                        max_attempts=int(body.get("max_attempts", 3)),
                        fail_plan=body.get("fail_plan") or {},
                        slow_ms=int(body.get("slow_ms", 0)),
                        crash_at=body.get("crash_at"),
                        owner=body.get("owner", "api"),
                        dedupe=bool(body.get("dedupe", True)),
                    )
                    return self._json(200, result)
                if path == "/api/run":
                    job = engine.run_next(
                        conn, owner=body.get("owner", "worker"),
                        lock_wait=float(body.get("lock_wait", 0)),
                    )
                    if job is None:
                        return self._json(200, {"ran": False, "job": None})
                    return self._json(200, {"ran": True, **job})
                if path == "/api/run-job":
                    job_id = body.get("id")
                    if not job_id:
                        raise engine.EngineError("id is required")
                    return self._json(200, engine.run_job(
                        conn, job_id, owner=body.get("owner", "worker"),
                        lock_wait=float(body.get("lock_wait", 0))))
                if path == "/api/retry":
                    job_id = body.get("id")
                    if not job_id:
                        raise engine.EngineError("id is required")
                    return self._json(200, engine.retry_job(conn, job_id))
                if path == "/api/resume":
                    job_id = body.get("id")
                    if not job_id:
                        raise engine.EngineError("id is required")
                    return self._json(200, engine.resume_job(conn, job_id))
                if path == "/api/release-lock":
                    engine.release_lock(conn)
                    return self._json(200, {"released": True})
                if path == "/api/seed":
                    return self._json(200, _seed(conn, body))
                if path == "/api/reset":
                    return self._json(200, _reset(conn))
                if path == "/api/chaos":
                    return self._json(200, _chaos(conn, body))
                raise engine.EngineError(f"no route for {path}")
            finally:
                conn.close()
        except engine.SimulatedCrash as exc:
            return self._json(202, {"interrupted": True, "detail": str(exc)})
        except engine.EngineError as exc:
            return self._json(exc.status, {"error": str(exc), "type": type(exc).__name__})
        except Exception as exc:  # pragma: no cover - unexpected
            traceback.print_exc()
            return self._json(500, {"error": str(exc)})

    def _static(self, filename: str) -> None:
        path = os.path.join(STATIC_DIR, filename)
        ctype = {"index.html": "text/html; charset=utf-8",
                 "app.js": "application/javascript; charset=utf-8",
                 "style.css": "text/css; charset=utf-8"}[filename]
        with open(path, "rb") as fh:
            self._send(200, fh.read(), ctype)


def _q(query: dict, key: str, default: str) -> str:
    return (query.get(key) or [default])[0]


def _state(conn) -> dict:
    state = engine.stats(conn)
    state["jobs"] = engine.list_jobs(conn, limit=10)
    state["views"] = db.VIEW_DEFS
    return state


# ---------------------------------------------------------------------------
# Demo helpers (seed / reset / chaos) — they are what the UI buttons call.
# ---------------------------------------------------------------------------


def _seed(conn, body: dict) -> dict:
    days = body.get("days") or _recent_days(int(body.get("day_count", 5)))
    per_day = int(body.get("per_day", 6))
    categories = body.get("categories") or ["books", "games", "tools"]
    rng = random.Random(body.get("seed", 7))
    facts = []
    for day in days:
        for i in range(per_day):
            facts.append({
                "key": f"{day}-{i:03d}",
                "category": rng.choice(categories),
                "value": round(rng.uniform(5, 95), 2),
                "occurred_at": f"{day}T{rng.randint(0, 23):02d}:00:00+00:00",
            })
    result = engine.record_facts(conn, facts)
    return {"seeded": len(facts), "days": days, **result}


def _recent_days(count: int) -> list[str]:
    today = datetime.now(timezone.utc).date()
    return [(today - timedelta(days=count - 1 - i)).isoformat() for i in range(count)]


def _reset(conn) -> dict:
    tables = [
        "fact_event", "view_meta", "view_partition", "view_partition_staging",
        "mv_partition_row", "mv_partition_row_staging", "dirty_partition",
        "maintenance_job", "maintenance_lock", "partition_failure", "maintenance_event",
    ]
    with db.transaction(conn):
        for table in tables:
            conn.execute(f"DELETE FROM {table}")
        conn.execute("DELETE FROM sqlite_sequence")
        for name in db.VIEW_DEFS:
            conn.execute("INSERT INTO view_meta(name, version) VALUES (?,0)", (name,))
    return {"reset": True}


def _chaos(conn, body: dict) -> dict:
    """Inject a scenario in one call: late facts + trigger (+ optional run)."""
    scenario = body.get("scenario", "late_update")
    days = body.get("days") or _recent_days(5)
    facts: list[dict] = []
    if scenario == "late_update":
        day = days[len(days) // 2]
        facts = [{"key": f"{day}-000", "category": "books", "value": 500.0,
                  "occurred_at": f"{day}T12:00:00+00:00"}]
    elif scenario == "late_delete":
        day = days[0]
        facts = [{"key": f"{day}-001", "op": "delete"}]
    elif scenario == "move_day":
        day = days[0]
        facts = [{"key": f"{day}-000", "value": 777.0,
                  "occurred_at": f"{days[-1]}T09:00:00+00:00"}]
    elif scenario == "new_partition":
        extra = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
        facts = [{"key": f"{extra}-900", "category": "tools", "value": 42.0,
                  "occurred_at": f"{extra}T01:00:00+00:00"}]
    else:
        raise engine.EngineError(f"unknown scenario {scenario!r}")
    result = engine.record_facts(conn, facts)
    return {"scenario": scenario, **result}


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    db.init_db()
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    print(f"MV engine on http://{host}:{port}  (db={os.path.abspath(db.DB_PATH)})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    host = os.environ.get("MV_HOST", "127.0.0.1")
    port = int(os.environ.get("MV_PORT", sys.argv[1] if len(sys.argv) > 1 else 8765))
    serve(host, port)
