#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export BUN_INSTALL="$HOME/.bun"
export PATH="$BUN_INSTALL/bin:$HOME/.local/bin:$PATH"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

if [[ -z "${EPFL_RCP_API_KEY:-}" ]]; then
  echo "Error: EPFL_RCP_API_KEY is not set. Add it to .env or export it first." >&2
  exit 1
fi

exec "$HOME/.bun/bin/opencode" "$@"
