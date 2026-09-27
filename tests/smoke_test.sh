#!/usr/bin/env bash
# Smoke test for lead-dispatch-system.
# Runs the CLI against a throwaway database and asserts basic behaviour.
# Usage: bash tests/smoke_test.sh [python-binary]
set -u

PY="${1:-python3}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI="$REPO_DIR/lead_dispatch_system.py"

TMPDIR_SMOKE="$(mktemp -d)"
export LEAD_DISPATCH_DB="$TMPDIR_SMOKE/smoke.db"
# Disable send throttle for speed; no real sends happen here.
export SMTP_MIN_GAP_SECONDS=0

pass=0
fail=0

check() {  # check <description> <expected_exit> -- command...
    local desc="$1"; local want="$2"; shift 2
    local out rc
    out="$("$@" 2>&1)"; rc=$?
    if [ "$rc" -eq "$want" ]; then
        pass=$((pass+1)); echo "PASS: $desc (exit $rc)"
    else
        fail=$((fail+1)); echo "FAIL: $desc — want exit $want, got $rc"
        echo "--- output ---"; printf '%s\n' "$out" | head -20; echo "--------------"
    fi
}

check_contains() {  # check_contains <description> <needle> -- command...
    local desc="$1"; local needle="$2"; shift 2
    local out
    out="$("$@" 2>&1)"
    if printf '%s\n' "$out" | grep -qF "$needle"; then
        pass=$((pass+1)); echo "PASS: $desc"
    else
        fail=$((fail+1)); echo "FAIL: $desc — output missing '$needle'"
        echo "--- output ---"; printf '%s\n' "$out" | head -20; echo "--------------"
    fi
}

echo "== lead-dispatch-system smoke test (db: $LEAD_DISPATCH_DB) =="

# 1. Help / status surface
check "--help exits 0" 0 "$PY" "$CLI" --help
check_contains "--help documents send-email" "send-email" "$PY" "$CLI" --help
check "stats exits 0 on empty db" 0 "$PY" "$CLI" stats
check "status alias exits 0" 0 "$PY" "$CLI" status

# 2. Input validation: invalid email rejected when adding a worker
check "add-worker rejects bad email" 0 "$PY" "$CLI" add-worker \
    --name "Test Worker" --skills "plumbing" --email "not-an-email"
check_contains "bad email produces clear error" "Invalid email format" \
    "$PY" "$CLI" add-worker --name "Test Worker" --skills "plumbing" --email "not-an-email"

# 3. Valid worker import + stats reflect it
"$PY" "$CLI" add-worker --name "Smoke Tester" --skills "plumbing,electrical" \
    --phone "+911234567890" --email "smoke@example.com" >/dev/null 2>&1
check_contains "stats shows imported worker" "Active:" "$PY" "$CLI" stats

# 4. send-email with no SMTP creds fails cleanly (no traceback, no send).
#    Insert one lead directly so we reach the creds check.
unset SMTP_USER SMTP_PASS SMTP_HOST
"$PY" - <<'EOF'
import os, sqlite3, datetime
conn = sqlite3.connect(os.environ["LEAD_DISPATCH_DB"])
conn.execute(
    "INSERT INTO leads (name, category, email, created_at, status)"
    " VALUES ('Acme Co', 'plumbing', 'acme@example.com', ?, 'new')",
    (datetime.datetime.utcnow().isoformat(),))
conn.commit(); conn.close()
EOF
check "send-email without creds exits non-zero" 1 "$PY" "$CLI" send-email 1 \
    --city "Mumbai" --service "plumbing"
check_contains "missing creds give actionable error" "SMTP credentials not configured" \
    "$PY" "$CLI" send-email 1 --city "Mumbai" --service "plumbing"
out="$("$PY" "$CLI" send-email 1 --city "Mumbai" --service "plumbing" 2>&1 || true)"
if printf '%s\n' "$out" | grep -q "Traceback"; then
    fail=$((fail+1)); echo "FAIL: stack trace leaked to user"
else
    pass=$((pass+1)); echo "PASS: no stack trace in user output"
fi

# 5. Structured error for unknown lead id
check "send-email to missing lead exits non-zero" 1 "$PY" "$CLI" send-email 99999 \
    --city "Mumbai" --service "plumbing" || true

rm -rf "$TMPDIR_SMOKE"

echo ""
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
