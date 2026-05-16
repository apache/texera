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

# Pick the Python interpreter the /python endpoint will use for running user
# code. Prefer an existing data-science venv (sklearn / pandas / matplotlib)
# so analysis scripts work out of the box; fall back to the manager's own
# venv if nothing better is available.
if [[ -z "${MACHINE_MANAGER_PYTHON:-}" ]]; then
  for candidate in \
    "$HOME/IdeaProjects/texera/.venv/bin/python" \
    "$VENV_DIR/bin/python"; do
    if [[ -x "$candidate" ]]; then
      export MACHINE_MANAGER_PYTHON="$candidate"
      break
    fi
  done
fi
echo "[machine-manager] /python interpreter = ${MACHINE_MANAGER_PYTHON:-<default>}"

exec "$VENV_DIR/bin/python" -m machine_manager.server
