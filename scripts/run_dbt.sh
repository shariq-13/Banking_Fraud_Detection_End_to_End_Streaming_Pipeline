#!/usr/bin/env bash
# Run dbt with the Snowflake settings from .env.
# Usage (from project root):
#   ./scripts/run_dbt.sh debug     # test the connection
#   ./scripts/run_dbt.sh build     # run models and tests (default)
#   ./scripts/run_dbt.sh run       # run models only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
set -a
source "$ROOT/.env"
set +a

export DBT_PROFILES_DIR="$ROOT/dbt_fraud"
cd "$ROOT/dbt_fraud"
dbt "${@:-build}"
