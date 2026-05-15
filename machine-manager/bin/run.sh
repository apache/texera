#!/usr/bin/env bash
# Start machine-manager on this host.
#
# Usage:
#   MACHINE_MANAGER_TOKEN=<token> ./bin/run.sh
#
# First run will create a venv and install dependencies.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV_DIR="${VENV_DIR:-.venv}"
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install --upgrade pip
  "$VENV_DIR/bin/pip" install -e .
fi

exec "$VENV_DIR/bin/python" -m machine_manager.server
