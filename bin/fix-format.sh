#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------
# fix-format.sh
# -------------------------------------------------------------
# Runs code formatters for the Texera repository.
#
# Usage:
#   bin/fix-format.sh               # Format all (Scala, Frontend, Python)
#   bin/fix-format.sh --scala       # Format Scala only
#   bin/fix-format.sh --frontend    # Format Frontend only
#   bin/fix-format.sh --python      # Format Python only
# -------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


source "$SCRIPT_DIR/utils/texera-logging.sh"

TEXERA_HOME="$(/usr/bin/env bash "$SCRIPT_DIR/utils/resolve-texera-home.sh")"
if [[ -z "${TEXERA_HOME:-}" ]]; then
  exit 1
fi

# --- Key directories ---
FRONTEND_DIR="$TEXERA_HOME/frontend"
AMBER_PY_DIR="$TEXERA_HOME/amber/src/main/python"

[[ -d "$FRONTEND_DIR" ]] || { tx_error "Frontend directory not found: $FRONTEND_DIR"; exit 1; }
[[ -d "$AMBER_PY_DIR"  ]] || { tx_error "Amber Python directory not found: $AMBER_PY_DIR"; exit 1; }

# --- Argument parsing ---
TARGET="${1:-all}"
run_scala=false
run_frontend=false
run_python=false

case "$TARGET" in
  --scala)    run_scala=true ;;
  --frontend) run_frontend=true ;;
  --python)   run_python=true ;;
  ""|--all|all) run_scala=true; run_frontend=true; run_python=true ;;
  *)
    tx_error "Unknown option: $TARGET"
    echo "Usage: bin/fix-format.sh [--scala | --frontend | --python | --all]"
    exit 1
    ;;
esac

# --- 1) Scala formatting ---
if $run_scala; then
  tx_info "Running sbt scalafmtAll at repo root..."
  if ! command -v sbt >/dev/null 2>&1; then
    tx_error "sbt not found. Please install sbt."
    exit 1
  fi
  (
    cd "$TEXERA_HOME"
    sbt scalafmtAll
  )
  tx_success "Scala formatting completed."
fi

# --- 2) Frontend formatting ---
if $run_frontend; then
  tx_info "Running yarn format:fix in frontend..."
  if ! command -v yarn >/dev/null 2>&1; then
    tx_error "yarn not found. Please install Yarn."
    exit 1
  fi
  (
    cd "$FRONTEND_DIR"
    yarn format:fix
  )
  tx_success "Frontend formatting completed."
fi

# --- 3) Python formatting ---
if $run_python; then
  tx_info "Running ruff in amber/src/main/python..."
  if ! command -v ruff >/dev/null 2>&1; then
    tx_error "ruff not found. Install with: pip install ruff"
    exit 1
  fi
  (
    cd "$AMBER_PY_DIR"
    ruff format .
  )
  tx_success "Python formatting completed."
fi

tx_success "✅ Formatting tasks completed successfully!"