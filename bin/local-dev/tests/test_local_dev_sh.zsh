#!/usr/bin/env zsh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0

# Smoke tests for bin/local-dev.sh. Run from the repo root or anywhere:
#   zsh bin/local-dev/tests/test_local_dev_sh.zsh
# Exits 0 if every check passes, 1 otherwise.
#
# Kept deliberately small: bringing up the actual stack needs Docker /
# sbt / a Mac and is out of scope for CI here. We cover the things that
# regress quietly — script syntax, version-detection, the subcommand
# dispatch, and graceful failure on garbage input.

set -u

REPO_ROOT="$(cd "$(dirname "${(%):-%x}")/../../.." && pwd)"
SCRIPT="$REPO_ROOT/bin/local-dev.sh"

PASS=0
FAIL=0

_pass() { printf "  \e[32m✓\e[0m %s\n" "$1"; PASS=$((PASS+1)); }
_fail() {
    printf "  \e[31m✗\e[0m %s\n" "$1"
    [[ $# -ge 2 ]] && printf "      %s\n" "$2"
    FAIL=$((FAIL+1))
}

# 1) zsh -n: syntax check. Catches everything from typos to unbalanced
#    heredocs without executing a line of the script.
if zsh -n "$SCRIPT" 2>/tmp/.local-dev-syntax.err; then
    _pass "zsh -n bin/local-dev.sh"
else
    _fail "zsh -n bin/local-dev.sh" "$(cat /tmp/.local-dev-syntax.err)"
fi
rm -f /tmp/.local-dev-syntax.err

# 2) `version` subcommand returns the same string we'd extract by hand
#    from build.sbt. This is the single source of truth that all the
#    dist / launcher / canary-jar paths in the script and the TUI build
#    off of, so we'd rather catch a regression here.
script_version=$("$SCRIPT" version 2>/dev/null | head -1 | tr -d '[:space:]')
sbt_version=$(
    grep -E '^[[:space:]]*ThisBuild[[:space:]]*/[[:space:]]*version[[:space:]]*:=[[:space:]]*"' \
        "$REPO_ROOT/build.sbt" 2>/dev/null \
        | head -1 \
        | sed -E 's/.*"([^"]+)".*/\1/' \
        | tr -d '[:space:]'
)
if [[ -n "$script_version" && "$script_version" == "$sbt_version" ]]; then
    _pass "version matches build.sbt ($script_version)"
else
    _fail "version mismatch" "script=$script_version  build.sbt=$sbt_version"
fi

# 3) TEXERA_VERSION env var should override.
override=$(TEXERA_VERSION="9.9.9-TEST" "$SCRIPT" version 2>/dev/null | head -1 | tr -d '[:space:]')
if [[ "$override" == "9.9.9-TEST" ]]; then
    _pass "TEXERA_VERSION env var overrides build.sbt"
else
    _fail "env override didn't take" "got: $override"
fi

# 4) `--help` prints usage.
help_out=$("$SCRIPT" --help 2>&1 | head -20)
if [[ "$help_out" == *"local-dev.sh"* && "$help_out" == *"Subcommands"* ]]; then
    _pass "--help shows usage"
else
    _fail "--help didn't show usage" "$(echo "$help_out" | head -3)"
fi

# 5) An unknown service name routes through cmd_update_one and exits
#    non-zero rather than silently doing nothing.
out=$("$SCRIPT" definitely-not-a-real-service 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown service"* || "$out" == *"Unknown service"* ]]; then
    _pass "unknown service exits non-zero with clear error"
else
    _fail "unknown service didn't error properly" "rc=$rc out=$out"
fi

# 6) `start` with no service name fails immediately (zsh parameter expansion
#    `${1:?...}` exits with the message).
out=$("$SCRIPT" start 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"need service name"* ]]; then
    _pass "start without arg refuses cleanly"
else
    _fail "start without arg should refuse" "rc=$rc out=$out"
fi

printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
(( FAIL == 0 ))
