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
# resolve-texera-home.sh
# -------------------------------------------------------------
# Determines TEXERA_HOME using the following priority:
#   1. TEXERA_HOME environment variable (if already set)
#   2. git repository root (if inside a git repo)
#   3. parent directory if current dir is 'bin'
#   4. current working directory (.)
#
# Prints the resolved TEXERA_HOME to stdout.
# Logs human-readable messages via texera-logging.sh.
#
# Intended usage:
#   TEXERA_HOME="$(bin/resolve-texera-home.sh)"
#
# Exits with code 1 if resolution fails.
# -------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Load logging helper ---
# shellcheck source=bin/texera-logging.sh
source "$SCRIPT_DIR/texera-logging.sh"

resolve_texera_home() {
  # 1. TEXERA_HOME environment variable
  if [[ -n "${TEXERA_HOME:-}" ]]; then
    tx_info "TEXERA_HOME found in environment: $TEXERA_HOME"
    echo "$TEXERA_HOME"
    return 0
  fi

  # 2. Git repository root (if any)
  if git -C . rev-parse --show-toplevel >/dev/null 2>&1; then
    local root
    root="$(git rev-parse --show-toplevel)"
    tx_info "TEXERA_HOME resolved via git repository root: $root"
    echo "$root"
    return 0
  fi

  # 3. Parent directory if current folder is 'bin'
  local cwd cwd_basename
  cwd="$(pwd)"
  cwd_basename="$(basename "$cwd")"
  if [[ "$cwd_basename" == "bin" ]]; then
    local parent
    parent="$(dirname "$cwd")"
    tx_info "TEXERA_HOME resolved as parent of bin/: $parent"
    echo "$parent"
    return 0
  fi

  # 4. Fallback to current working directory
  tx_warn "Falling back to current working directory as TEXERA_HOME: $cwd"
  echo "$cwd"
}

# --- Main execution ---
if ! resolve_texera_home; then
  tx_error "Failed to resolve TEXERA_HOME."
  exit 1
fi