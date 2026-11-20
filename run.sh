#!/usr/bin/env bash
# Start the incremental materialized-view maintenance console.
set -euo pipefail
cd "$(dirname "$0")"
PORT="${1:-8765}"
MV_DB_PATH="${MV_DB_PATH:-$PWD/data/mv.db}" exec python3 -m app.server "$PORT"
