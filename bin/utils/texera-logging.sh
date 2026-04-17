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
# texera-logging.sh
# -------------------------------------------------------------
# Shared logging utilities for all Texera bash scripts.
#
# Features:
#   • Colored, consistent logs prefixed with "Texera ▶"
#   • Logs go to STDERR by default (so STDOUT stays clean)
#   • Easy to override prefix or disable colors
#
# Env vars:
#   TEXERA_LOG_PREFIX="Texera ▸"   # change prefix
#   NO_COLOR=1                     # disable color
#   TEXERA_LOG_TO_STDERR=0         # send to STDOUT instead
# -------------------------------------------------------------

# Prevent direct execution
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf 'Texera ▶ This file must be sourced, not executed.\n' >&2
  exit 1
fi

# --- Color setup ---
_use_color=true
if [[ -n "${NO_COLOR:-}" ]]; then
  _use_color=false
fi

if $_use_color; then
  readonly TL_COLOR_BLUE=$'\033[34m'
  readonly TL_COLOR_GREEN=$'\033[32m'
  readonly TL_COLOR_YELLOW=$'\033[33m'
  readonly TL_COLOR_RED=$'\033[31m'
  readonly TL_COLOR_BOLD=$'\033[1m'
  readonly TL_COLOR_RESET=$'\033[0m'
else
  readonly TL_COLOR_BLUE="" TL_COLOR_GREEN="" TL_COLOR_YELLOW="" TL_COLOR_RED="" TL_COLOR_BOLD="" TL_COLOR_RESET=""
fi

# --- Prefix & output stream ---
readonly TEXERA_LOG_PREFIX="${TEXERA_LOG_PREFIX:-Texera ▶}"
readonly TL_PREFIX="${TL_COLOR_BOLD}${TEXERA_LOG_PREFIX}${TL_COLOR_RESET}"
readonly _TL_TO_STDERR="${TEXERA_LOG_TO_STDERR:-1}"

# --- Core emitter ---
_tx_emit() {
  local _color="$1"; shift
  local _level="$1"; shift
  if [[ "$_TL_TO_STDERR" == "1" ]]; then
    printf '%s %s[%s]%s %s\n' "$TL_PREFIX" "$_color" "$_level" "$TL_COLOR_RESET" "$*" >&2
  else
    printf '%s %s[%s]%s %s\n' "$TL_PREFIX" "$_color" "$_level" "$TL_COLOR_RESET" "$*"
  fi
}

# --- Public API ---
tx_info()    { _tx_emit "$TL_COLOR_BLUE"   "INFO" "$*"; }
tx_success() { _tx_emit "$TL_COLOR_GREEN"  "SUCCESS" "$*"; }
tx_warn()    { _tx_emit "$TL_COLOR_YELLOW" "WARN" "$*"; }
tx_error()   { _tx_emit "$TL_COLOR_RED"    "ERROR" "$*"; }

export -f tx_info tx_success tx_warn tx_error