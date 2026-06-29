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

# Smoke tests for bin/local-dev.sh. Run from the repo root:
#   bash bin/local-dev/tests/test_local_dev_sh.sh
# Exits 0 if every check passes, 1 otherwise.
#
# Kept deliberately small: bringing up the actual stack needs Docker /
# sbt / a Mac and is out of scope for CI here. We cover the things that
# regress quietly — script syntax, version-detection, the subcommand
# dispatch, and graceful failure on garbage input.

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPT="$REPO_ROOT/bin/local-dev.sh"

PASS=0
FAIL=0

_pass() { printf "  \e[32m✓\e[0m %s\n" "$1"; PASS=$((PASS+1)); }
_fail() {
    printf "  \e[31m✗\e[0m %s\n" "$1"
    [[ $# -ge 2 ]] && printf "      %s\n" "$2"
    FAIL=$((FAIL+1))
}

# 1) bash -n: syntax-check the entry wrapper and every shell file under
#    bin/local-dev/. `bash -n` on the wrapper alone would only see the
#    one-line exec, so internal helpers it routes to must be checked
#    explicitly. Catches typos and unbalanced heredocs without executing
#    a line. Uses find because macOS /bin/bash 3.2 lacks `globstar`.
syntax_ok=true
syntax_err=""
while IFS= read -r -d '' f; do
    if ! bash -n "$f" 2>/tmp/.local-dev-syntax.err; then
        syntax_ok=false
        syntax_err+="\n  $f: $(cat /tmp/.local-dev-syntax.err)"
    fi
done < <(printf '%s\0' "$SCRIPT"; find "$REPO_ROOT/bin/local-dev" -type f -name '*.sh' -print0)
rm -f /tmp/.local-dev-syntax.err
if $syntax_ok; then
    _pass "bash -n bin/local-dev.sh"
else
    _fail "bash -n bin/local-dev.sh" "$(printf '%b' "$syntax_err")"
fi

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

# 7) No-arg invocation must be non-interactive (= `status`). Previously the
#    default launched the TUI, which made the script unsafe to drop into
#    cron jobs or CI smoke tests. Anything that prints the banner without
#    hanging counts.
out=$("$SCRIPT" 2>&1 | head -5)
rc=$?
if (( rc == 0 )) && [[ "$out" == *"Texera Local Dev"* ]]; then
    _pass "no-arg invocation prints status (non-interactive)"
else
    _fail "no-arg invocation didn't print status" "rc=$rc out=$(echo "$out" | head -1)"
fi

# 8) `-i` without a TTY must refuse cleanly, not crash or hang. We pipe
#    stdin from /dev/null so the TTY check fires. Avoid piping into `head`
#    here — that masks the script's exit code under zsh.
out=$("$SCRIPT" -i </dev/null 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"requires a TTY"* || "$out" == *"requires Python"* ]]; then
    _pass "-i refuses cleanly without a TTY or Python"
else
    _fail "-i didn't refuse cleanly" "rc=$rc out=$(echo "$out" | head -1)"
fi

# 9) Regression: when no Python on the candidate list has textual, the
#    error message + install hint must actually print. The bug it
#    guards against: zsh's `set -e` aborts the script silently when a
#    command substitution's command exits non-zero (`var=$(returns 1)`),
#    so the install-hint code below the assignment never ran. `script`
#    allocates a pty so the TTY check passes and we get to the python
#    check.
#
# `script` has two incompatible invocation styles:
#   macOS BSD:  script -q OUT_FILE CMD ARGS...
#   util-linux: script -qc "CMD ARGS..."  OUT_FILE
# Probe with `script --version`: util-linux supports it, BSD doesn't.
if command -v script >/dev/null 2>&1; then
    bad_py="/usr/bin/python3"
    if [[ -x "$bad_py" ]] && ! "$bad_py" -c "import textual" >/dev/null 2>&1; then
        if script --version >/dev/null 2>&1; then
            # util-linux dialect
            out=$(env -i HOME="$HOME" PATH=/usr/bin:/bin TERM="${TERM:-xterm}" \
                script -qc "$SCRIPT -i; echo __rc=\$?" /dev/null </dev/null 2>&1)
        else
            # macOS BSD dialect
            out=$(env -i HOME="$HOME" PATH=/usr/bin:/bin TERM="${TERM:-xterm}" \
                script -q /dev/null sh -c "$SCRIPT -i; echo __rc=\$?" </dev/null 2>&1)
        fi
        if [[ "$out" == *"requires Python"* && "$out" == *"install Python"* && "$out" == *"__rc=1"* ]]; then
            _pass "-i with no textual prints the install hint (regression for zsh set -e bug)"
        else
            _fail "-i with no textual didn't print install hint" \
                "got: $(echo "$out" | head -3 | tr '\n' '|')"
        fi
    else
        _pass "skip: no textual-less python available to test against"
    fi
else
    _pass "skip: 'script' not on PATH"
fi

# 10) Regression: dual-zip selection in target/universal/. Closes #5991.
#     Leftover `<svc>-1.2.0-incubating.zip` next to a fresh
#     `<svc>-1.3.0-incubating-SNAPSHOT.zip` used to break `unzip -oq <glob>`:
#     the shell expanded the unquoted glob to two filenames, unzip read the
#     second as a member to extract from the first, exit 11, and the script
#     silently logged "not produced — skipping". Both call sites
#     (`build_all` + `cmd_auto`) must now pick the newest match via
#     `ls -t <glob> | head -1` and feed unzip a single file.
n_naked=$(grep -hE '^[[:space:]]*if unzip -oq \$\{zip_glob\}' \
    "$REPO_ROOT"/bin/local-dev/*.sh 2>/dev/null | wc -l)
n_picker=$(grep -hE 'ls -t \$\{?zip_glob\}?.*head -1' \
    "$REPO_ROOT"/bin/local-dev/*.sh 2>/dev/null | wc -l)
if (( n_naked == 0 )) && (( n_picker >= 2 )); then
    _pass "unzip step picks newest dist zip (regression for #5991)"
else
    _fail "unzip step is missing the newest-zip picker" \
        "naked-glob unzip count=$n_naked  picker count=$n_picker  (expected naked=0, picker>=2)"
fi

# 11) Regression: jOOQ codegen runs at sbt-build time and connects to
#     postgres (common/dao's jooqGenerate sourceGenerator). On a fresh
#     checkout the generated dir is empty (not git-tracked), so if
#     postgres isn't reachable when sbt runs, the build fails. Both
#     cmd_up and cmd_auto must run a postgres-ready step BEFORE the
#     sbt build is launched. Closes #6007.
MAIN_SH="$REPO_ROOT/bin/local-dev/main.sh"
for fn in cmd_up cmd_auto; do
    result=$(awk -v fn="$fn" '
        BEGIN { in_fn = 0; depth = 0; schema_at = 0; build_at = 0 }
        !in_fn && index($0, fn "()") == 1 { in_fn = 1; depth = 1; next }
        in_fn {
            for (i = 1; i <= length($0); i++) {
                c = substr($0, i, 1)
                if (c == "{") depth++
                else if (c == "}") depth--
            }
            if (schema_at == 0 && match($0, /infra_ensure_db_schema|ensure_postgres_for_build/))
                schema_at = NR
            if (build_at == 0 && match($0, /build_all|sbt[[:space:]]+-no-colors[[:space:]]+dist/))
                build_at = NR
            if (depth == 0) {
                printf "schema=%d build=%d", schema_at, build_at
                exit
            }
        }
    ' "$MAIN_SH")
    schema_at=$(echo "$result" | sed -n 's/.*schema=\([0-9]*\).*/\1/p')
    build_at=$(echo "$result" | sed -n 's/.*build=\([0-9]*\).*/\1/p')
    if (( schema_at > 0 )) && (( build_at > 0 )) && (( schema_at < build_at )); then
        _pass "$fn: postgres readiness check precedes sbt build (regression for #6007)"
    else
        _fail "$fn: postgres readiness must precede sbt build (regression for #6007)" \
            "schema_at=$schema_at  build_at=$build_at"
    fi
done

printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
(( FAIL == 0 ))
