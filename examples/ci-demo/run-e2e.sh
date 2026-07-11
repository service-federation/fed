#!/usr/bin/env bash
# End-to-end exercise of the demo project against a real Docker daemon.
# Run from anywhere: FED=/path/to/fed examples/ci-demo/run-e2e.sh
set -euo pipefail
cd "$(dirname "$0")"
FED="${FED:-fed}"

fail() { echo "FAIL: $1" >&2; exit 1; }

cleanup() {
  "$FED" stop >/dev/null 2>&1 || true
  [ -n "${SQUATTER_PID:-}" ] && kill "$SQUATTER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Regression for the dual-stack allocator fix: a node-style [::] listener
# squats on the app's default port. fed must detect it and hand the app a
# different port instead of letting it crash with EADDRINUSE.
node -e 'require("net").createServer().listen({host: "::", port: 3000}, () => console.log("squatting [::]:3000"))' &
SQUATTER_PID=$!
sleep 1

echo "== fed start =="
"$FED" start

APP_PORT=$("$FED" ports list --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["APP_PORT"])')
DEV_DB_PORT=$("$FED" ports list --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["DB_PORT"])')
echo "allocated APP_PORT=$APP_PORT DB_PORT=$DEV_DB_PORT"
[ "$APP_PORT" != "3000" ] || fail "allocator handed out port 3000 despite the [::] squatter"

echo "== app responds =="
curl -fsS "http://localhost:$APP_PORT" | grep -q ok || fail "app did not respond on allocated port $APP_PORT"

echo "== status shows app running =="
"$FED" status --json | python3 -c '
import json, sys
status = json.load(sys.stdin)
assert status["app"]["status"] in ("running", "healthy"), status
assert status["postgres"]["status"] in ("running", "healthy"), status
' || fail "status does not show app+postgres running"

echo "== isolated integration-test =="
OUT=$("$FED" integration-test 2>&1) || { echo "$OUT"; fail "integration-test failed"; }
echo "$OUT" | grep -q "connected to postgres on port" || { echo "$OUT"; fail "itest did not connect"; }
ISO_DB_PORT=$(echo "$OUT" | grep -o "connected to postgres on port [0-9]*" | grep -o "[0-9]*$")
[ "$ISO_DB_PORT" != "$DEV_DB_PORT" ] || fail "isolated run reused the dev stack's DB port"

echo "== dev stack survived the isolated run =="
curl -fsS "http://localhost:$APP_PORT" | grep -q ok || fail "dev stack died during isolated run"

echo "== fed stop cleans up =="
"$FED" stop
# Scope to this project's containers (by published port) so the script also
# passes on a dev machine where other fed stacks are running.
LEFTOVER=$(docker ps --filter "publish=$DEV_DB_PORT" --format '{{.Names}}')
[ -z "$LEFTOVER" ] || fail "containers left running after stop: $LEFTOVER"

echo "e2e demo: all checks passed"
