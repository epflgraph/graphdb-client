#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -x "${ROOT_DIR}/.venv.graphdb/bin/graphdb" ]]; then
  GRAPHDB_CMD=("${ROOT_DIR}/.venv.graphdb/bin/graphdb")
elif command -v graphdb >/dev/null 2>&1; then
  GRAPHDB_CMD=("graphdb")
else
  GRAPHDB_CMD=("python" "-m" "graphdb.cli.main")
fi

run_graphdb() {
  PYTHONPATH="${ROOT_DIR}" "${GRAPHDB_CMD[@]}" "$@"
}

section() {
  printf "\n============================================================\n"
  printf "%s\n" "$1"
  printf "============================================================\n"
}

section "1) Help and available inspect flags"
run_graphdb inspect -h

section "2) Raw query + debug panel + snapshot + fingerprint + timing success"
run_graphdb inspect \
  --env test \
  --query "SELECT id, email, created_at FROM users WHERE active = 1 ORDER BY created_at DESC LIMIT :limit" \
  --description "Fetch active users" \
  --title "Active Users Query" \
  --params-json '{"limit":100,"password":"should-not-leak"}' \
  --debug \
  --snapshot \
  --show-fingerprint \
  --time-demo

section "3) Canonical SQL and one-line preview"
run_graphdb inspect \
  --env test \
  --query "SELECT id, email FROM users WHERE active = 1 ORDER BY id DESC LIMIT 100" \
  --show-canonical \
  --show-one-line \
  --one-line-len 70

section "4) Build query from parts (SQLQuery.from_parts)"
run_graphdb inspect \
  --env prod \
  --select "id, title" \
  --from "articles" \
  --where "published = 1" \
  --title "Articles Query"

section "5) Copyable output mode"
run_graphdb inspect \
  --env test \
  --query "SELECT object_id, object_name FROM data_object WHERE object_type = 'person'" \
  --copyable \
  --title "Copyable SQL"

section "6) Fingerprint including parameters"
run_graphdb inspect \
  --env test \
  --query "SELECT * FROM users WHERE id = :id" \
  --params-json '{"id":7,"token":"abc123"}' \
  --show-fingerprint \
  --fingerprint-with-params

section "7) Parameter redaction off (for debugging only)"
run_graphdb inspect \
  --env test \
  --query "SELECT 1" \
  --params-json '{"password":"plain-text-demo","id":2}' \
  --no-redact-params \
  --debug

section "8) Preset metadata fields (elapsed/rows/error)"
run_graphdb inspect \
  --env xaas_coresrv \
  --query "SELECT COUNT(*) FROM users" \
  --elapsed-ms 3.14 \
  --row-count 42 \
  --error "simulated warning" \
  --debug \
  --title "Preset Metadata Demo"

section "9) Timing failure capture demo"
run_graphdb inspect \
  --env test \
  --query "SELECT * FROM users" \
  --time-fail-demo \
  --debug \
  --title "Failure Timing Demo"

section "10) Alternate rendering style/theme"
run_graphdb inspect \
  --env test \
  --query "SELECT id, email FROM users WHERE active = 1" \
  --box-style rounded \
  --theme monokai \
  --title "Styled Render Demo"

section "Done"
