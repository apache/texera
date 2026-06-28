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

# Smoke tests for bin/single-node.sh. Run from the repo root:
#   bash bin/single-node/tests/test_single_node_sh.sh
# Exits 0 if every check passes, 1 otherwise.
#
# Kept deliberately small: actually pulling images + booting docker is
# out of scope for unit CI. We cover the things that regress quietly —
# script syntax, the subcommand dispatch, graceful refusal on missing
# args, and clean failure when docker is absent.

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPT="$REPO_ROOT/bin/single-node.sh"

PASS=0
FAIL=0

_pass() { printf "  \e[32m✓\e[0m %s\n" "$1"; PASS=$((PASS+1)); }
_fail() {
    printf "  \e[31m✗\e[0m %s\n" "$1"
    [[ $# -ge 2 ]] && printf "      %s\n" "$2"
    FAIL=$((FAIL+1))
}

# 1) bash -n: syntax-check the entry wrapper and every shell file under
#    bin/single-node/. `bash -n` on the wrapper alone would only see the
#    one-line exec, so internal helpers it routes to must be checked
#    explicitly. Uses find because macOS /bin/bash 3.2 lacks `globstar`.
syntax_ok=true
syntax_err=""
while IFS= read -r -d '' f; do
    if ! bash -n "$f" 2>/tmp/.single-node-syntax.err; then
        syntax_ok=false
        syntax_err+="\n  $f: $(cat /tmp/.single-node-syntax.err)"
    fi
done < <(printf '%s\0' "$SCRIPT"; find "$REPO_ROOT/bin/single-node" -type f -name '*.sh' -print0)
rm -f /tmp/.single-node-syntax.err
if $syntax_ok; then
    _pass "bash -n bin/single-node.sh"
else
    _fail "bash -n bin/single-node.sh" "$(printf '%b' "$syntax_err")"
fi

# 2) `--help` prints usage.
help_out=$("$SCRIPT" --help 2>&1)
if [[ "$help_out" == *"single-node.sh"* && "$help_out" == *"Subcommands"* ]]; then
    _pass "--help shows usage"
else
    _fail "--help didn't show usage" "$(echo "$help_out" | head -3)"
fi

# 3) Unknown subcommand exits non-zero with a clear hint pointing at
#    --help (NOT a wall of help text dumped to stderr).
out=$("$SCRIPT" definitely-not-a-real-subcommand 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown subcommand"* && "$out" == *"--help"* ]]; then
    _pass "unknown subcommand exits non-zero with --help hint"
else
    _fail "unknown subcommand didn't refuse cleanly" "rc=$rc out=$out"
fi

# 4) `logs` with no argument refuses cleanly with a usage line that
#    references the wrapper (NOT the engine path — main.sh must not
#    leak into user-facing errors).
out=$("$SCRIPT" logs 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"usage: bin/single-node.sh logs"* ]] \
                 && [[ "$out" != *"main.sh"* ]]; then
    _pass "logs without arg refuses cleanly (no main.sh leak)"
else
    _fail "logs without arg should refuse cleanly" "rc=$rc out=$out"
fi

# 5) `down` with an unknown flag refuses cleanly. Catches drifts where
#    someone adds a new flag and forgets to widen the case-switch.
out=$("$SCRIPT" down --not-a-real-flag 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown flag"* ]]; then
    _pass "down rejects unknown flags"
else
    _fail "down should reject unknown flags" "rc=$rc out=$out"
fi

# 5b) `up` with an unknown flag refuses cleanly (same drift guard).
out=$("$SCRIPT" up --not-a-real-flag 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown flag"* ]]; then
    _pass "up rejects unknown flags"
else
    _fail "up should reject unknown flags" "rc=$rc out=$out"
fi

# 5c) `up --with-examples` passes flag parsing (gets through to the
#     docker pre-flight, which is the expected failure point when
#     docker isn't installed — that's NOT a flag-parse error). The
#     accepted flag must not bounce off the unknown-flag arm.
out=$("$SCRIPT" up --with-examples 2>&1)
rc=$?
if [[ "$out" != *"unknown flag"* ]]; then
    _pass "up --with-examples is accepted by flag parser"
else
    _fail "up --with-examples should be accepted" "rc=$rc out=$out"
fi

# 6) Regression: when docker is not running, every subcommand that
#    actually talks to docker (up / down / status / logs) must abort
#    cleanly with a "Docker daemon" hint rather than crashing or
#    silently doing nothing. Skipped if docker is currently up.
if docker info >/dev/null 2>&1; then
    _pass "skip: docker daemon is up — pre-flight error path not exercised"
else
    out=$("$SCRIPT" status 2>&1)
    rc=$?
    if (( rc != 0 )) && [[ "$out" == *"Docker"* || "$out" == *"docker"* ]]; then
        _pass "status with no docker daemon aborts cleanly"
    else
        _fail "status with no docker daemon should abort" "rc=$rc out=$out"
    fi
fi

printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
(( FAIL == 0 ))
