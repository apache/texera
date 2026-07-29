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
# Kept deliberately small: bringing up the actual stack needs Docker, sbt and
# the rest of the toolchain, which is out of scope for CI here. We cover the
# things that regress quietly — script syntax, version-detection, the
# subcommand dispatch, and graceful failure on garbage input.
#
# Runs on macOS and Linux alike (CI's `infra` job covers both). Anything that
# can only hold on one of them is guarded by a `uname` check or by probing for
# the tool involved, and reports a skip rather than a pass.

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

# 5) An unknown subcommand exits non-zero with a clear error rather than
#    silently doing nothing, and the single-service form rejects unknown
#    service names the same way.
out=$("$SCRIPT" definitely-not-a-real-service 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown subcommand"* ]]; then
    _pass "unknown subcommand exits non-zero with clear error"
else
    _fail "unknown subcommand didn't error properly" "rc=$rc out=$out"
fi
# Isolated state dir: a full-stack `up` (re)persists the deploy pointer during
# startup even when the flag parse later fails, so never point these at the
# real deployment's state.
_up_state=$(mktemp -d 2>/dev/null || mktemp -d -t ldup)
out=$(TEXERA_LOCAL_DEV_DIR="$_up_state" "$SCRIPT" up definitely-not-a-real-service 2>&1)
rc=$?
if (( rc != 0 )) && [[ "$out" == *"unknown service"* ]]; then
    _pass "up <unknown-service> exits non-zero with clear error"
else
    _fail "up <unknown-service> didn't error properly" "rc=$rc out=$out"
fi

# 6) `start`/`stop` were replaced by `up <service>` / `down <service>` — they
#    must refuse with a pointer to the new spelling, not silently no-op.
for verb in start stop; do
    out=$("$SCRIPT" "$verb" texera-web 2>&1)
    rc=$?
    if (( rc != 0 )) && [[ "$out" == *"up <service>"* ]]; then
        _pass "$verb points to up/down <service>"
    else
        _fail "$verb should refuse with the new spelling" "rc=$rc out=$out"
    fi
done

# 6b) Flag hygiene on the new surface: --no-build is gone (renamed), the
#     full-stack-only knobs are rejected in the single-service form, and at
#     most one service is accepted.
out=$(TEXERA_LOCAL_DEV_DIR="$_up_state" "$SCRIPT" up --no-build 2>&1); rc=$?
if (( rc == 2 )) && [[ "$out" == *"--skip-build"* ]]; then
    _pass "up --no-build refuses and names --skip-build"
else
    _fail "up --no-build not rejected with rename hint" "rc=$rc out=$out"
fi
out=$(TEXERA_LOCAL_DEV_DIR="$_up_state" "$SCRIPT" up texera-web --fresh 2>&1); rc=$?
if (( rc == 2 )) && [[ "$out" == *"full-stack"* ]]; then
    _pass "up <svc> --fresh refuses (full-stack only)"
else
    _fail "up <svc> --fresh not rejected" "rc=$rc out=$out"
fi
out=$(TEXERA_LOCAL_DEV_DIR="$_up_state" "$SCRIPT" up texera-web frontend 2>&1); rc=$?
if (( rc == 2 )) && [[ "$out" == *"at most one service"* ]]; then
    _pass "up refuses two positional services"
else
    _fail "up accepted two services" "rc=$rc out=$out"
fi
out=$(TEXERA_LOCAL_DEV_DIR="$_up_state" "$SCRIPT" down texera-web --skip=frontend 2>&1); rc=$?
if (( rc == 2 )) && [[ "$out" == *"full-stack"* ]]; then
    _pass "down <svc> --skip refuses (full-stack only)"
else
    _fail "down <svc> --skip not rejected" "rc=$rc out=$out"
fi
rm -rf "$_up_state"

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

# 12) `status --json` emits a single machine-readable JSON object — the stable
#     contract for agents/scripts that would otherwise grep the dashboard.
#     Must parse, expose running/total/services, list every service exactly
#     once, and stay internally consistent (len(services)==total, running<=total).
if command -v python3 >/dev/null 2>&1; then
    json_out=$("$SCRIPT" status --json 2>/dev/null)
    if printf '%s' "$json_out" | python3 -c '
import sys, json
d = json.load(sys.stdin)
assert isinstance(d["services"], list), "services not a list"
assert isinstance(d["running"], int) and isinstance(d["total"], int)
assert d["total"] == len(d["services"]), "total != len(services)"
assert 0 <= d["running"] <= d["total"], "running out of range"
names = {s["service"] for s in d["services"]}
need = {"texera-web", "frontend", "postgres"}
assert need <= names, f"missing services: {need - names}"
assert isinstance(d["elapsed_seconds"], int) and d["elapsed_seconds"] >= 0, "elapsed_seconds missing/bad"
for s in d["services"]:
    assert isinstance(s["port"], int), "port not int"
    assert s["type"] in {"jvm", "docker", "yarn", "bun"}, "bad service type"
    assert s["pid"] is None or isinstance(s["pid"], int), "pid not int|null"
' 2>/tmp/.local-dev-json.err; then
        _pass "status --json emits valid, consistent JSON with all services"
    else
        _fail "status --json invalid/inconsistent" \
            "$(tail -1 /tmp/.local-dev-json.err 2>/dev/null); out=$(printf '%s' "$json_out" | head -c 160)"
    fi
    rm -f /tmp/.local-dev-json.err

    # 13) Exit code mirrors health: 0 iff running == total, else 1. Lets an
    #     agent gate on `if status --json; then` without parsing the body.
    running=$(printf '%s' "$json_out" | python3 -c 'import sys,json;print(json.load(sys.stdin)["running"])' 2>/dev/null)
    total=$(printf '%s' "$json_out" | python3 -c 'import sys,json;print(json.load(sys.stdin)["total"])' 2>/dev/null)
    "$SCRIPT" status --json >/dev/null 2>&1; rc_json=$?
    if { [[ "$running" == "$total" ]] && (( rc_json == 0 )); } \
       || { [[ "$running" != "$total" ]] && (( rc_json == 1 )); }; then
        _pass "status --json exit code reflects health (running=$running total=$total rc=$rc_json)"
    else
        _fail "status --json exit code wrong" "running=$running total=$total rc=$rc_json"
    fi
else
    _pass "skip: python3 not on PATH (status --json shape check)"
fi

# 14) Negative: an unknown flag to `status` must refuse with rc 2 and a clear
#     message — bad input is not silently ignored.
out=$("$SCRIPT" status --definitely-bogus 2>&1)
rc=$?
if (( rc == 2 )) && [[ "$out" == *"unknown flag"* ]]; then
    _pass "status rejects unknown flag (rc=2, clear error)"
else
    _fail "status didn't reject unknown flag" "rc=$rc out=$(echo "$out" | head -1)"
fi

# 15) `--help` documents --json so the contract is discoverable.
help_out=$("$SCRIPT" --help 2>&1)
if [[ "$help_out" == *"--json"* ]]; then
    _pass "--help documents --json"
else
    _fail "--help doesn't mention --json"
fi

# 16) Regression: in non-TTY mode tui_spinner can't spin in place, so a long
#     silent step (sbt dist → log) must emit a heartbeat or it looks hung to a
#     non-interactive caller. Guard the sentinel inside the function body.
spinner_body=$(awk '/^tui_spinner\(\)/{f=1} f{print} f&&/^}/{exit}' "$REPO_ROOT/bin/local-dev/main.sh")
if [[ "$spinner_body" == *"! -t 1"* && "$spinner_body" == *"still running"* && "$spinner_body" == *"kill -0"* ]]; then
    _pass "tui_spinner emits a non-TTY heartbeat (no silent long-running steps)"
else
    _fail "tui_spinner missing non-TTY heartbeat loop"
fi

# 17) `up`, `down`, and `auto` accept --json (route the human stream to stderr,
#     emit the JSON summary on stdout). Structural guard — invoking them for
#     real would build/stop the stack, out of scope here.
for fn in cmd_up cmd_down cmd_auto; do
    body=$(awk -v fn="$fn" '$0 ~ "^" fn "\\(\\)" {f=1} f{print} f&&/^}/{exit}' \
        "$REPO_ROOT/bin/local-dev/main.sh")
    if [[ "$body" == *"--json"* && "$body" == *"emit_status_json"* ]]; then
        _pass "$fn accepts --json and emits JSON summary"
    else
        _fail "$fn doesn't wire up --json"
    fi
done

# 17b) `version --json` is machine-readable and carries elapsed_seconds — the
#      runtime field is part of every --json payload, not just status.
if command -v python3 >/dev/null 2>&1; then
    out=$("$SCRIPT" version --json 2>/dev/null)
    if printf '%s' "$out" | python3 -c '
import sys, json
d = json.load(sys.stdin)
assert d["version"], "version empty"
assert isinstance(d["elapsed_seconds"], int) and d["elapsed_seconds"] >= 0
' 2>/dev/null; then
        _pass "version --json emits version + elapsed_seconds"
    else
        _fail "version --json invalid" "out=$out"
    fi
else
    _pass "skip: python3 not on PATH (version --json check)"
fi

# 17c) The single-service `up <svc>` form must follow the active deployment
#      rather than resetting the persisted worktree pointer (only a full-stack
#      `up` re-decides the target). Structural: the startup peek skips the
#      reset/persist block when a positional service arg is present.
peek=$(sed -n '/self tree vs deploy source/,/^REPO_ROOT=/p' "$REPO_ROOT/bin/local-dev/main.sh")
if [[ "$peek" == *"_has_svc_arg"* ]]; then
    _pass "up <svc> leaves the deploy-source pointer alone"
else
    _fail "startup peek doesn't guard the pointer reset on up <svc>"
fi

# 18) Deploy-source: `--help` documents the worktree selectors.
help_out=$("$SCRIPT" --help 2>&1)
if [[ "$help_out" == *"--worktree="* && "$help_out" == *"--branch="* ]]; then
    _pass "--help documents --worktree / --branch deploy selectors"
else
    _fail "--help doesn't document deploy selectors"
fi

# Deploy-source tests use an ISOLATED STATE_DIR so they never read or clobber a
# real deployment's persisted pointer.
_ld_state=$(mktemp -d 2>/dev/null || mktemp -d -t ld)
if command -v python3 >/dev/null 2>&1; then
    _jq() { printf '%s' "$1" | python3 -c "import sys,json;print(json.load(sys.stdin)[\"$2\"])" 2>/dev/null; }

    # 19) status --json carries the deploy-source fields, defaulting to this
    #     checkout (no pointer ⇒ worktree == repo dir name, source == REPO_ROOT).
    out=$(TEXERA_LOCAL_DEV_DIR="$_ld_state" "$SCRIPT" status --json 2>/dev/null)
    wt=$(_jq "$out" worktree); src=$(_jq "$out" source)
    if [[ "$wt" == "$(basename "$REPO_ROOT")" && "$src" == "$REPO_ROOT" ]]; then
        _pass "status --json reports deploy source (self): worktree=$wt"
    else
        _fail "status --json deploy-source fields wrong" "worktree=$wt source=$src"
    fi

    # 20) A stale pointer (worktree gone) is dropped and we fall back to self.
    printf '%s\n' "/no/such/worktree/$$" > "$_ld_state/deploy-source"
    out=$(TEXERA_LOCAL_DEV_DIR="$_ld_state" "$SCRIPT" status --json 2>/dev/null)
    wt=$(_jq "$out" worktree)
    if [[ "$wt" == "$(basename "$REPO_ROOT")" && ! -f "$_ld_state/deploy-source" ]]; then
        _pass "stale deploy-source pointer is dropped, falls back to self"
    else
        _fail "stale pointer not handled" \
            "worktree=$wt pointer=$([[ -f "$_ld_state/deploy-source" ]] && echo present || echo gone)"
    fi

    # 21) A valid persisted pointer to a sibling worktree is honored: status
    #     reports THAT worktree's branch. Create a throwaway worktree, point at
    #     it, assert, then clean up.
    _wt_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldwt); rm -rf "$_wt_dir"
    _wt_branch="ld-test-$$-wt"
    if git -C "$REPO_ROOT" worktree add -q -b "$_wt_branch" "$_wt_dir" HEAD 2>/dev/null; then
        printf '%s\n' "$_wt_dir" > "$_ld_state/deploy-source"
        out=$(TEXERA_LOCAL_DEV_DIR="$_ld_state" "$SCRIPT" status --json 2>/dev/null)
        wt=$(_jq "$out" worktree); br=$(_jq "$out" branch)
        if [[ "$wt" == "$(basename "$_wt_dir")" && "$br" == "$_wt_branch" ]]; then
            _pass "persisted pointer deploys the sibling worktree (branch=$br)"
        else
            _fail "worktree pointer not honored" "worktree=$wt branch=$br"
        fi
        git -C "$REPO_ROOT" worktree remove --force "$_wt_dir" 2>/dev/null || true
        git -C "$REPO_ROOT" branch -D "$_wt_branch" 2>/dev/null || true
    else
        _pass "skip: could not create a temp worktree for the pointer test"
    fi
else
    _pass "skip: python3 not on PATH (deploy-source JSON checks)"
fi

# 22) Invalid --branch / --worktree fail fast (rc 1) with a clear message,
#     BEFORE any build/start (the resolution runs at startup).
out=$(TEXERA_LOCAL_DEV_DIR="$_ld_state" "$SCRIPT" up --branch=__no_such_branch__ 2>&1); rc=$?
if (( rc == 1 )) && [[ "$out" == *"no git worktree has branch"* ]]; then
    _pass "up --branch with no worktree fails fast (rc=1)"
else
    _fail "invalid --branch not rejected" "rc=$rc out=$(echo "$out" | head -1)"
fi
out=$(TEXERA_LOCAL_DEV_DIR="$_ld_state" "$SCRIPT" up --worktree=/no/such/dir 2>&1); rc=$?
if (( rc == 1 )) && [[ "$out" == *"not a valid texera worktree"* ]]; then
    _pass "up --worktree with bad path fails fast (rc=1)"
else
    _fail "invalid --worktree not rejected" "rc=$rc out=$(echo "$out" | head -1)"
fi
rm -rf "$_ld_state"

# 23) Tooling-drift boundary is surfaced: --help documents that the target's
#     bin/local-dev/** changes are NOT in effect, and _warn_tooling_drift is
#     wired into both cmd_up and cmd_auto (structural — firing it for real
#     would need a full `up`).
if [[ "$help_out" == *"NOT in effect"* ]]; then
    _pass "--help documents the tooling-runs-from-self boundary"
else
    _fail "--help doesn't document the tooling boundary"
fi
drift_body=$(awk '/^_warn_tooling_drift\(\)/{f=1} f{print} f&&/^}/{exit}' "$REPO_ROOT/bin/local-dev/main.sh")
if [[ "$drift_body" == *"diff -rq"* && "$drift_body" == *"bin/local-dev"* ]]; then
    _pass "_warn_tooling_drift diffs the target's bin/local-dev/"
else
    _fail "_warn_tooling_drift missing or doesn't diff bin/local-dev/"
fi
for fn in cmd_up cmd_auto; do
    body=$(awk -v fn="$fn" '$0 ~ "^" fn "\\(\\)" {f=1} f{print} f&&/^}/{exit}' \
        "$REPO_ROOT/bin/local-dev/main.sh")
    if [[ "$body" == *"_warn_tooling_drift"* ]]; then
        _pass "$fn calls _warn_tooling_drift"
    else
        _fail "$fn doesn't call _warn_tooling_drift"
    fi
done

# 24) build_all's CLI-only sbt knobs must never alter what the dist ships.
#     Three knobs are banned because each produced a dist that packaged fine
#     but could not run: `-Dsbt.pipelining=true` and the two `set every`
#     settings all drop inter-project dependency jars or the bin/<service>
#     launcher from the dist. Match against code only (comments name the
#     banned knobs to explain them).
build_body=$(awk '/^build_all\(\)/{f=1} f{print} f&&/^}/{exit}' \
    "$REPO_ROOT/bin/local-dev/main.sh")
build_code=$(printf '%s\n' "$build_body" | grep -vE '^[[:space:]]*#')
if [[ "$build_code" == *"-Dsbt.pipelining=true"* ]]; then
    _fail "build_all: '-Dsbt.pipelining=true' drops inter-project dependency jars from the dist"
else
    _pass "build_all: does not enable sbt pipelining"
fi
if [[ "$build_code" == *"doc / sources) := Seq.empty"* ]]; then
    _fail "build_all: 'set every (Compile / doc / sources) := Seq.empty' empties Compile/sources → no launcher"
else
    _pass "build_all: does not clobber Compile/sources via 'set every ... doc / sources'"
fi
if [[ "$build_code" == *"packageDoc / publishArtifact) := false"* ]]; then
    _fail "build_all: 'set every (Compile / packageDoc / publishArtifact) := false' drops dependency jars from the dist"
else
    _pass "build_all: does not disable publishArtifact via 'set every ... packageDoc'"
fi

# 25) --skip=<svc> flows into the sbt build: skipped JVM services drop out
#     of the per-project dist target list, and their running jars are left
#     alone in both the pre-bounce and unzip loops.
prebounce=$(printf '%s\n' "$build_body" | awk '
    /Stop any running JVMs BEFORE unzip/ {f=1}
    f {print}
    f && /unzipping dist artifacts/ {exit}')
if [[ "$prebounce" == *"is_skipped"* ]]; then
    _pass "build_all: pre-bounce loop honors --skip"
else
    _fail "build_all: pre-bounce loop kills --skip'd services"
fi
unzip_section=$(printf '%s\n' "$build_body" | awk '/unzipping dist artifacts/{f=1} f{print}')
if [[ "$unzip_section" == *"is_skipped"* ]]; then
    _pass "build_all: unzip loop honors --skip"
else
    _fail "build_all: unzip loop touches --skip'd services"
fi

# 26) build_all's auto short-circuit also checks that every unpacked
#     launcher is on disk, not just that source hashes match the last
#     build. Without this, an externally cleaned target/ leaves stamps
#     valid while target/<svc>-<version>/ is gone, so `up` skips build
#     + unzip and each JVM fails with "launcher missing".
if grep -qE '^all_launchers_present\(\) \{' "$REPO_ROOT/bin/local-dev/main.sh"; then
    _pass "all_launchers_present helper is defined"
else
    _fail "all_launchers_present helper missing"
fi
if [[ "$build_body" == *"all_launchers_present"* ]]; then
    _pass "build_all: auto short-circuit gated on all_launchers_present"
else
    _fail "build_all: auto short-circuit ignores missing launchers"
fi

# 27) sql/updates auto-apply (regression for the jOOQ compile failure after a
#     pull adds a new sql/updates/N.sql). Structural: the reconcile function
#     exists, is wired into infra_ensure_db_schema on BOTH paths (existing DB ->
#     apply pending; fresh bootstrap -> seed as applied), and records into
#     liquibase's databasechangelog so the official runner stays compatible.
if grep -qE '^infra_apply_sql_updates\(\) \{' "$MAIN_SH"; then
    _pass "infra_apply_sql_updates helper is defined"
else
    _fail "infra_apply_sql_updates helper missing"
fi
schema_body=$(awk '/^infra_ensure_db_schema\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ "$schema_body" == *"infra_apply_sql_updates"* ]] \
   && [[ "$schema_body" == *"infra_apply_sql_updates seed"* ]]; then
    _pass "infra_ensure_db_schema reconciles updates (apply on existing DB, seed on bootstrap)"
else
    _fail "infra_ensure_db_schema doesn't wire infra_apply_sql_updates on both paths"
fi
updates_body=$(awk '/^infra_apply_sql_updates\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ "$updates_body" == *"databasechangelog"* && "$updates_body" == *"ON_ERROR_STOP"* ]]; then
    _pass "infra_apply_sql_updates tracks via databasechangelog and fails fast on psql errors"
else
    _fail "infra_apply_sql_updates missing databasechangelog tracking or ON_ERROR_STOP"
fi

# 28) The changelog parser handles the real sql/changelog.xml: emits
#     id/author/path triples, skips the commented example changeset, and every
#     referenced update file exists on disk (repo-consistency check).
parse_fn=$(awk '/^parse_changelog_changesets\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
parsed=$(eval "$parse_fn"; parse_changelog_changesets "$REPO_ROOT/sql/changelog.xml")
n_parsed=$(printf '%s\n' "$parsed" | grep -c .)
example_skipped=true
printf '%s' "$parsed" | grep -q "^1	" && example_skipped=false
if (( n_parsed >= 5 )) && $example_skipped; then
    _pass "changelog parser: $n_parsed changesets, commented example skipped"
else
    _fail "changelog parser wrong" "n=$n_parsed parsed=$(printf '%s' "$parsed" | head -2 | tr '\n' '|')"
fi
missing_files=""
while IFS=$'\t' read -r _id _author _path; do
    [[ -n "$_path" && ! -f "$REPO_ROOT/$_path" ]] && missing_files+=" $_path"
done <<< "$parsed"
if [[ -z "$missing_files" ]]; then
    _pass "changelog: every referenced sql/updates file exists"
else
    _fail "changelog references missing files:$missing_files"
fi

# 29) Host LAN IP detection on Linux. The macOS probes (`route get default`,
#     `ipconfig getifaddr en0..en10`) do not exist there, so `up`/`auto` used to
#     abort with "could not detect a host LAN IP" on every Linux box (#7065).
#     Driven against a fake `ip` so the assertions don't depend on the test
#     machine's interfaces. The docker-bridge case is the one that matters most:
#     the whole point of HOST_LAN_IP is an address reachable from both the host
#     and the lakekeeper container, and 172.17.0.1 is reachable from neither
#     side the way we need.
lanip_fn=$(awk '/^_detect_host_lan_ip_linux\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ -z "$lanip_fn" ]]; then
    _fail "_detect_host_lan_ip_linux helper missing"
else
    _lan_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldlan)
    # Fake `ip`: answers `route show default` from ip.route and
    # `-o addr show [dev X] scope global` from ip.addr.
    cat > "$_lan_dir/ip" <<'FAKE_IP'
#!/usr/bin/env bash
mode=""; want_dev=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        route) mode=route ;;
        addr)  mode=addr ;;
        dev)   shift; want_dev="${1:-}" ;;
    esac
    shift
done
case "$mode" in
    route) cat "$0.route" 2>/dev/null ;;
    addr)
        if [[ -n "$want_dev" ]]; then
            awk -v d="$want_dev" '$2 == d' "$0.addr" 2>/dev/null
        else
            cat "$0.addr" 2>/dev/null
        fi
        ;;
esac
FAKE_IP
    chmod +x "$_lan_dir/ip"
    _lan_check() {  # $1=label  $2=expected stdout ("" = must fail)  $3=route  $4=addr
        printf '%s\n' "$3" > "$_lan_dir/ip.route"
        printf '%s\n' "$4" > "$_lan_dir/ip.addr"
        local got="" rc=0
        got=$(PATH="$_lan_dir:$PATH"; eval "$lanip_fn"; _detect_host_lan_ip_linux) || rc=$?
        if [[ -z "$2" ]]; then
            if (( rc != 0 )) && [[ -z "$got" ]]; then
                _pass "linux LAN IP: $1 (refuses)"
            else
                _fail "linux LAN IP: $1 should refuse" "rc=$rc got='$got'"
            fi
        elif [[ "$got" == "$2" ]]; then
            _pass "linux LAN IP: $1 → $got"
        else
            _fail "linux LAN IP: $1" "expected '$2' got '$got' (rc=$rc)"
        fi
    }
    _lan_check "default route interface wins" "10.10.10.30" \
        'default via 10.10.10.1 dev eth0 proto static metric 100' \
        '2: eth0    inet 10.10.10.30/24 brd 10.10.10.255 scope global eth0
3: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0'
    _lan_check "no default route: skips the docker bridge" "10.10.10.30" \
        '' \
        '3: docker0    inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0
2: eth0    inet 10.10.10.30/24 brd 10.10.10.255 scope global eth0'
    _lan_check "no default route: skips bridges, veth and tailscale" "192.168.1.42" \
        '' \
        '4: br-abc123    inet 172.18.0.1/16 scope global br-abc123
5: veth9f2    inet 172.19.0.1/16 scope global veth9f2
6: tailscale0    inet 100.101.102.103/32 scope global tailscale0
2: enp5s0    inet 192.168.1.42/24 scope global enp5s0'
    _lan_check "default route interface has no global v4: falls back to scan" "192.168.1.42" \
        'default via 10.0.0.1 dev ppp0 proto static' \
        '2: enp5s0    inet 192.168.1.42/24 scope global enp5s0'
    _lan_check "nothing but loopback" "" '' ''
    _lan_check "only excluded interfaces" "" \
        '' \
        '3: docker0    inet 172.17.0.1/16 scope global docker0'
    # Negative: no iproute2 at all must fail cleanly, not emit a bogus address.
    _lan_no_ip=$(mktemp -d 2>/dev/null || mktemp -d -t ldlan2)
    got=""; rc=0
    got=$(PATH="$_lan_no_ip"; eval "$lanip_fn"; _detect_host_lan_ip_linux) || rc=$?
    if (( rc != 0 )) && [[ -z "$got" ]]; then
        _pass "linux LAN IP: no \`ip\` on PATH (refuses)"
    else
        _fail "linux LAN IP: no \`ip\` should refuse" "rc=$rc got='$got'"
    fi
    rm -rf "$_lan_dir" "$_lan_no_ip"
fi

# 30) The dispatcher must keep the macOS probes for Darwin and only use the
#     Linux ones on Linux — the fix must not regress the platform that worked.
dispatch_fn=$(awk '/^_detect_host_lan_ip\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ "$dispatch_fn" == *"Darwin"* ]] \
   && [[ "$dispatch_fn" == *"_detect_host_lan_ip_darwin"* ]] \
   && [[ "$dispatch_fn" == *"_detect_host_lan_ip_linux"* ]]; then
    _pass "_detect_host_lan_ip dispatches per platform"
else
    _fail "_detect_host_lan_ip doesn't dispatch to both platform probes"
fi
# And the FATAL must stop naming only the macOS probes, since on Linux none of
# them ever ran.
if ! grep -q 'of \\`route get default\\` / en0-en10 had a non-loopback IPv4' "$MAIN_SH"; then
    _pass "host-LAN-IP failure message is not macOS-only"
else
    _fail "host-LAN-IP failure message still blames route get/en0-en10 on every platform"
fi

# 31) Artifact mtime must be readable with either stat dialect. GNU coreutils
#     rejects BSD's `stat -f "%Sm" -t FMT` outright (it reads -f as "file system
#     info" and the format strings as filenames), so on Linux the ARTIFACT MTIME
#     column rendered stat's error text instead of a timestamp.
mtime_fn=$(awk '/^_file_mtime_str\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ -z "$mtime_fn" ]]; then
    _fail "_file_mtime_str helper missing"
else
    _mt_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldmt)
    : > "$_mt_dir/artifact.jar"
    got=$(eval "$mtime_fn"; _file_mtime_str "$_mt_dir/artifact.jar")
    if [[ "$got" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
        _pass "_file_mtime_str formats YYYY-MM-DD HH:MM ($got)"
    else
        _fail "_file_mtime_str wrong format" "got '$got'"
    fi
    # Negative: a missing file must fail rather than print stat's diagnostics.
    got=""; rc=0
    got=$(eval "$mtime_fn"; _file_mtime_str "$_mt_dir/nope.jar" 2>&1) || rc=$?
    if (( rc != 0 )) && [[ -z "$got" ]]; then
        _pass "_file_mtime_str refuses a missing file quietly"
    else
        _fail "_file_mtime_str should fail quietly on a missing file" "rc=$rc got='$got'"
    fi
    got=""; rc=0
    got=$(eval "$mtime_fn"; _file_mtime_str 2>&1) || rc=$?
    if (( rc != 0 )) && [[ -z "$got" ]]; then
        _pass "_file_mtime_str refuses a missing argument quietly"
    else
        _fail "_file_mtime_str should fail quietly with no argument" "rc=$rc got='$got'"
    fi
    rm -rf "$_mt_dir"
fi
# The BSD spelling is allowed to survive inside _file_mtime_str — that's the
# whole point of the helper — but nowhere else, or the next call site silently
# breaks on Linux again.
main_outside_helper=$(awk '
    /^_file_mtime_str\(\)/ { skip = 1 }
    skip { if ($0 == "}") skip = 0; next }
    { print }
' "$MAIN_SH")
if ! printf '%s\n' "$main_outside_helper" | grep -qE 'stat -f "%Sm" -t'; then
    _pass "BSD stat is confined to _file_mtime_str"
else
    _fail "main.sh still calls BSD-only \`stat -f \"%Sm\" -t\` outside _file_mtime_str" \
        "$(printf '%s\n' "$main_outside_helper" | grep -nE 'stat -f "%Sm" -t' | head -3 | tr '\n' '|')"
fi

# 32) Port→PID lookup must not depend on lsof. It is standard on macOS but not
#     installed by default on Debian/Ubuntu/Fedora, and without it every native
#     service read as "stopped" — so `up` relaunched live services and `down`
#     silently no-opped. Tested against a real listener, with lsof shimmed out.
listen_fn=$(awk '/^listen_pid_for_port\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ -z "$listen_fn" ]]; then
    _fail "listen_pid_for_port helper missing"
elif ! command -v python3 >/dev/null 2>&1; then
    _pass "skip: python3 not on PATH (port listener check)"
else
    _lp_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldlp)
    # Bind an ephemeral port, print it, then hold it open until killed.
    python3 -c '
import socket, sys, time
s = socket.socket(); s.bind(("127.0.0.1", 0)); s.listen(1)
print(s.getsockname()[1], flush=True)
time.sleep(120)
' > "$_lp_dir/port" 2>/dev/null &
    _lp_pid=$!
    _lp_port=""
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        _lp_port=$(head -1 "$_lp_dir/port" 2>/dev/null)
        [[ -n "$_lp_port" ]] && break
        sleep 0.3
    done
    if [[ -z "$_lp_port" ]]; then
        _fail "port listener check: could not start a listener"
    else
        got=$(eval "$listen_fn"; listen_pid_for_port "$_lp_port")
        if [[ "$got" == "$_lp_pid" ]]; then
            _pass "listen_pid_for_port finds the listener ($_lp_port → $got)"
        else
            _fail "listen_pid_for_port didn't find the listener" \
                "port=$_lp_port expected=$_lp_pid got='$got'"
        fi
        # Same answer with lsof unavailable — the Linux-without-lsof box. The
        # fallback is `ss` from iproute2, which only exists on Linux; macOS has
        # no equivalent and ships lsof in the base system, so there is nothing
        # to fall back to (or to test) there.
        if command -v ss >/dev/null 2>&1; then
            printf '#!/bin/sh\nexit 127\n' > "$_lp_dir/lsof"; chmod +x "$_lp_dir/lsof"
            got=$(PATH="$_lp_dir:$PATH"; eval "$listen_fn"; listen_pid_for_port "$_lp_port")
            if [[ "$got" == "$_lp_pid" ]]; then
                _pass "listen_pid_for_port works without lsof ($got)"
            else
                _fail "listen_pid_for_port needs lsof" \
                    "port=$_lp_port expected=$_lp_pid got='$got'"
            fi
        else
            _pass "skip: no \`ss\` on this platform (lsof fallback is Linux-only)"
        fi
        # Negative: a port nobody listens on yields empty output, not noise.
        got=$(eval "$listen_fn"; listen_pid_for_port 1 2>&1)
        if [[ -z "$got" ]]; then
            _pass "listen_pid_for_port: unused port yields nothing"
        else
            _fail "listen_pid_for_port: unused port produced output" "got='$got'"
        fi
    fi
    kill "$_lp_pid" 2>/dev/null
    wait "$_lp_pid" 2>/dev/null
    rm -rf "$_lp_dir"
fi

# 33) Install hints have to be actionable on Linux: `docker-compose-plugin`
#     only exists in Docker's own apt repo (stock Ubuntu ships
#     `docker-compose-v2`), and node/yarn/bun had no Linux line at all.
hint_body=$(awk '/^_install_hint\(\)/{f=1} f{print} f&&/^}/{exit}' "$MAIN_SH")
if [[ "$hint_body" != *"docker-compose-plugin"* ]] \
   && [[ "$hint_body" == *"docker-compose-v2"* ]]; then
    _pass "docker hint names the distro compose package"
else
    _fail "docker hint still points at docker-compose-plugin"
fi
hint_missing=""
for tool in java python node yarn bun sbt docker; do
    case_body=$(printf '%s\n' "$hint_body" | awk -v t="$tool" '
        $0 ~ "^[[:space:]]*"t"\\)" {f=1}
        f {print}
        f && /;;/ {exit}')
    [[ "$case_body" == *"Linux"* ]] || hint_missing+=" $tool"
done
if [[ -z "$hint_missing" ]]; then
    _pass "every install hint has a Linux line"
else
    _fail "install hints with no Linux line:$hint_missing"
fi

# --------------------------------------------------------------------------
# Toolchain detection + consented install (#7066).
# Everything below goes through the *decision* helpers, which are pure: they
# print what would be done and never install anything. That separation is the
# reason this is testable in CI at all.
# --------------------------------------------------------------------------
_extract_fn() {  # $1=function name → its definition, or empty
    awk -v fn="$1" 'index($0, fn "()") == 1 {f=1} f{print} f && /^}/{exit}' "$MAIN_SH"
}

# 34) Which package manager we'd use. On Linux the first of apt-get/dnf/yum/
#     pacman/zypper on PATH wins; with none present it must refuse rather than
#     emit a bogus name that a caller would then try to run.
pm_fn=$(_extract_fn _pkg_manager)
if [[ -z "$pm_fn" ]]; then
    _fail "_pkg_manager helper missing"
else
    _pm_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldpm)
    _pm_check() {  # $1=label $2=expected ("" = must refuse) $3..=fake tools
        local label="$1" expect="$2"; shift 2
        rm -f "$_pm_dir"/*
        local t=""
        for t in "$@"; do printf '#!/bin/sh\nexit 0\n' > "$_pm_dir/$t"; chmod +x "$_pm_dir/$t"; done
        # PATH is stripped to the fakes so the host's real apt/dnf can't answer,
        # but _pkg_manager still needs `uname` to know the platform.
        ln -sf "$(command -v uname)" "$_pm_dir/uname"
        local got="" rc=0
        got=$(PATH="$_pm_dir"; eval "$pm_fn"; _pkg_manager) || rc=$?
        if [[ -z "$expect" ]]; then
            if (( rc != 0 )) && [[ -z "$got" ]]; then
                _pass "pkg manager: $label (refuses)"
            else
                _fail "pkg manager: $label should refuse" "rc=$rc got='$got'"
            fi
        elif [[ "$got" == "$expect" ]]; then
            _pass "pkg manager: $label → $got"
        else
            _fail "pkg manager: $label" "expected '$expect' got '$got'"
        fi
    }
    if [[ "$(uname -s)" == "Linux" ]]; then
        _pm_check "apt-get" "apt-get" apt-get
        _pm_check "dnf only" "dnf" dnf
        _pm_check "pacman only" "pacman" pacman
        _pm_check "apt-get preferred over dnf" "apt-get" apt-get dnf
        _pm_check "nothing installed" ""
    else
        _pass "skip: package-manager PATH probing is Linux-only on this host"
    fi
    rm -rf "$_pm_dir"
    if [[ "$pm_fn" == *brew* && "$pm_fn" == *Darwin* ]]; then
        _pass "_pkg_manager keeps the Darwin/brew branch"
    else
        _fail "_pkg_manager lost the Darwin/brew branch"
    fi
fi

# 35) The command we'd run per tool. Java goes through the distro package
#     manager first with SDKMAN as the no-sudo fallback; node uses nvm; python
#     uses pyenv and must pin 3.12, the version AGENTS.md and the CI matrix
#     both specify. Unknown or deliberately-unsupported tools must refuse
#     instead of printing something a caller would run.
cmd_fn=$(_extract_fn _install_cmd_for)
if [[ -z "$cmd_fn" || -z "$pm_fn" ]]; then
    _fail "_install_cmd_for helper missing"
else
    _ic_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldic)
    _ic() {  # $1=tool  $2..=fake pkg managers on PATH → prints the command
        local tool="$1"; shift
        rm -f "$_ic_dir"/*
        local t=""
        for t in "$@"; do printf '#!/bin/sh\nexit 0\n' > "$_ic_dir/$t"; chmod +x "$_ic_dir/$t"; done
        ln -sf "$(command -v uname)" "$_ic_dir/uname"
        ( PATH="$_ic_dir"
          TEXERA_NODE_VERSION=24 TEXERA_PYTHON_VERSION=3.12
          eval "$pm_fn"; eval "$cmd_fn"; _install_cmd_for "$tool" ) 2>/dev/null
    }
    _ic_expect() {  # $1=label $2=needle $3=tool $4..=fake managers
        local label="$1" needle="$2" tool="$3"; shift 3
        local got=""
        got=$(_ic "$tool" "$@")
        if [[ "$got" == *"$needle"* ]]; then
            _pass "install cmd: $label"
        else
            _fail "install cmd: $label" "expected to contain '$needle', got '$got'"
        fi
    }
    # Which manager answers is platform-dependent: _pkg_manager's Darwin branch
    # only ever looks for brew, so a faked apt-get on PATH is correctly ignored
    # there. Assert each platform's own branch.
    if [[ "$(uname -s)" == "Linux" ]]; then
        _ic_expect "java via apt names openjdk-17-jdk" "openjdk-17-jdk" java apt-get
        _ic_expect "java via apt asks for sudo explicitly" "sudo" java apt-get
        _ic_expect "java via dnf names java-17-openjdk-devel" "java-17-openjdk-devel" java dnf
    else
        _ic_expect "java via brew" "brew install openjdk@17" java brew
    fi
    # Platform-independent: with no manager at all we fall back to SDKMAN.
    _ic_expect "java with no package manager falls back to SDKMAN" "sdk" java
    _ic_expect "node uses nvm" "nvm install" node apt-get
    _ic_expect "python uses pyenv" "pyenv install" python apt-get
    _ic_expect "python pins 3.12" "3.12" python apt-get
    for tool in docker sbt definitely-not-a-tool; do
        got=""; rc=0
        got=$( PATH="$_ic_dir"
               TEXERA_NODE_VERSION=24 TEXERA_PYTHON_VERSION=3.12
               eval "$pm_fn"; eval "$cmd_fn"; _install_cmd_for "$tool" 2>/dev/null ) || rc=$?
        if (( rc != 0 )) && [[ -z "$got" ]]; then
            _pass "install cmd: refuses '$tool'"
        else
            _fail "install cmd: should refuse '$tool'" "rc=$rc got='$got'"
        fi
    done
    rm -rf "$_ic_dir"
fi

# 36) Consent. The prompt is for humans; a non-interactive run (CI, cron, an
#     agent following AGENTS.md's non-interactive subcommands) must keep the old
#     behaviour and never block on a read. This is the most important test in
#     this PR: a regression here hangs every automated `up`.
consent_fn=$(_extract_fn _consent_to_install)
if [[ -z "$consent_fn" ]]; then
    _fail "_consent_to_install helper missing"
else
    rc=0
    ( eval "$consent_fn"; _consent_to_install java "sudo apt-get install -y openjdk-17-jdk" ) \
        </dev/null >/dev/null 2>&1 || rc=$?
    if (( rc != 0 )); then
        _pass "consent: refuses without a TTY (never prompts)"
    else
        _fail "consent: granted install without a TTY"
    fi
    rc=0
    ( TEXERA_INSTALL_MISSING=1; eval "$consent_fn"; _consent_to_install java "cmd" ) \
        </dev/null >/dev/null 2>&1 || rc=$?
    if (( rc == 0 )); then
        _pass "consent: TEXERA_INSTALL_MISSING=1 assumes yes"
    else
        _fail "consent: TEXERA_INSTALL_MISSING=1 didn't assume yes" "rc=$rc"
    fi
    rc=0
    ( TEXERA_INSTALL_MISSING=0; eval "$consent_fn"; _consent_to_install java "cmd" ) \
        </dev/null >/dev/null 2>&1 || rc=$?
    if (( rc != 0 )); then
        _pass "consent: TEXERA_INSTALL_MISSING=0 refuses"
    else
        _fail "consent: TEXERA_INSTALL_MISSING=0 didn't refuse"
    fi
fi

# 37) Picking the interpreter that runs Python UDFs. The old default was
#     `command -v python3` — the system interpreter, which has none of
#     amber/requirements.txt — so UDFs died at worker launch on import errors
#     that pointed nowhere near the interpreter choice.
ver_fn=$(_extract_fn _python_version_of)
vok_fn=$(_extract_fn _python_version_ok)
dok_fn=$(_extract_fn _python_deps_ok)
if [[ -z "$ver_fn" || -z "$vok_fn" || -z "$dok_fn" ]]; then
    _fail "python probe helpers missing (_python_version_of/_python_version_ok/_python_deps_ok)"
else
    _py_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldpy)
    # `python -c ...` is only ever asked for the version string or an import
    # check, so scripts that echo a version / set an exit code drive both probes.
    printf '#!/bin/sh\necho 3.12\n' > "$_py_dir/py312"
    printf '#!/bin/sh\necho 3.11\n' > "$_py_dir/py311"
    printf '#!/bin/sh\nexit 0\n'    > "$_py_dir/deps-ok"
    printf '#!/bin/sh\nexit 1\n'    > "$_py_dir/deps-missing"
    chmod +x "$_py_dir"/*
    _py_probe() {  # $1=fn $2=path → rc
        local rc=0
        ( TEXERA_PYTHON_VERSION=3.12
          eval "$ver_fn"; eval "$vok_fn"; eval "$dok_fn"
          "$1" "$2" ) >/dev/null 2>&1 || rc=$?
        return $rc
    }
    if _py_probe _python_version_ok "$_py_dir/py312"; then
        _pass "python probe: 3.12 accepted"
    else
        _fail "python probe: 3.12 rejected"
    fi
    if ! _py_probe _python_version_ok "$_py_dir/py311"; then
        _pass "python probe: 3.11 rejected"
    else
        _fail "python probe: 3.11 accepted (must pin 3.12)"
    fi
    if ! _py_probe _python_version_ok "$_py_dir/nonexistent"; then
        _pass "python probe: missing interpreter rejected"
    else
        _fail "python probe: missing interpreter accepted"
    fi
    if _py_probe _python_deps_ok "$_py_dir/deps-ok"; then
        _pass "python probe: importable deps accepted"
    else
        _fail "python probe: importable deps rejected"
    fi
    if ! _py_probe _python_deps_ok "$_py_dir/deps-missing"; then
        _pass "python probe: missing deps rejected"
    else
        _fail "python probe: missing deps accepted"
    fi
    if ! _py_probe _python_deps_ok ""; then
        _pass "python probe: empty path rejected"
    else
        _fail "python probe: empty path accepted"
    fi
    rm -rf "$_py_dir"
fi

# 38) Candidate order. An activated venv must beat the sibling venv312 from
#     AGENTS.md's layout, and both must beat whatever `python3` happens to be on
#     PATH — that last one being the old, broken default.
cand_fn=$(_extract_fn _udf_python_candidates)
if [[ -z "$cand_fn" ]]; then
    _fail "_udf_python_candidates helper missing"
else
    _cd_dir=$(mktemp -d 2>/dev/null || mktemp -d -t ldcd)
    mkdir -p "$_cd_dir/active/bin" "$_cd_dir/ws/venv312/bin" "$_cd_dir/ws/texera"
    : > "$_cd_dir/active/bin/python";        chmod +x "$_cd_dir/active/bin/python"
    : > "$_cd_dir/ws/venv312/bin/python";    chmod +x "$_cd_dir/ws/venv312/bin/python"
    order=$( VIRTUAL_ENV="$_cd_dir/active" REPO_ROOT="$_cd_dir/ws/texera" \
             SELF_ROOT="$_cd_dir/ws/texera" TEXERA_PYTHON_VERSION=3.12
             eval "$cand_fn"; _udf_python_candidates 2>/dev/null )
    pos_active=$(printf '%s\n' "$order" | grep -n "active/bin/python" | head -1 | cut -d: -f1)
    pos_venv=$(printf '%s\n' "$order" | grep -n "venv312/bin/python" | head -1 | cut -d: -f1)
    pos_sys=$(printf '%s\n' "$order" | grep -nE "/python3$" | tail -1 | cut -d: -f1)
    if [[ -n "$pos_active" && -n "$pos_venv" ]] && (( pos_active < pos_venv )); then
        _pass "python candidates: \$VIRTUAL_ENV before sibling venv312"
    else
        _fail "python candidates: wrong order" \
            "active=$pos_active venv312=$pos_venv order=$(printf '%s' "$order" | tr '\n' '|')"
    fi
    if [[ -z "$pos_sys" ]] || { [[ -n "$pos_venv" ]] && (( pos_venv < pos_sys )); }; then
        _pass "python candidates: venv312 before bare python3 on PATH"
    else
        _fail "python candidates: bare python3 outranks venv312" \
            "venv312=$pos_venv sys=$pos_sys order=$(printf '%s' "$order" | tr '\n' '|')"
    fi
    rm -rf "$_cd_dir"
fi

# 39) The resolver must be wired into the paths that launch services, and must
#     NOT run at source time — `status` / `--help` shouldn't pay for spawning
#     interpreters.
if [[ -n "$(_extract_fn _require_udf_python)" ]]; then
    _pass "_require_udf_python helper is defined"
else
    _fail "_require_udf_python helper missing"
fi
for fn in cmd_up cmd_auto cmd_up_one; do
    body=$(_extract_fn "$fn")
    if [[ "$body" == *"_require_udf_python"* ]]; then
        _pass "$fn resolves the UDF interpreter before launching"
    else
        _fail "$fn doesn't call _require_udf_python"
    fi
done
# The UDF interpreter must not be advertised with the `-i` dashboard's hint:
# that one is about textual and amber/dev-requirements.txt, a different
# interpreter and a different requirements file.
# Comments are stripped: the code explains *why* it doesn't use that hint, and
# the explanation naturally names it.
udf_body=$(_extract_fn _require_udf_python | sed 's/^[[:space:]]*#.*$//')
if [[ "$udf_body" != *"_install_hint python"* ]] \
   && [[ "$udf_body" == *"UDF_PYTHON_PATH"* ]] \
   && [[ "$udf_body" == *"amber/requirements.txt"* ]]; then
    _pass "_require_udf_python points at amber's requirements, not the TUI hint"
else
    _fail "_require_udf_python reuses the TUI python hint or omits amber's requirements"
fi
if ! grep -qE '^export UDF_PYTHON_PATH="\$\{UDF_PYTHON_PATH:-\$\(command -v python3' "$MAIN_SH"; then
    _pass "UDF_PYTHON_PATH no longer defaults to bare python3 at source time"
else
    _fail "UDF_PYTHON_PATH still defaults to \`command -v python3\` at source time"
fi

# 40) The new knobs are discoverable, and contradictory ones are refused rather
#     than silently resolved one way.
help_out=$("$SCRIPT" --help 2>&1)
if [[ "$help_out" == *"--install-missing"* && "$help_out" == *"--no-install"* ]]; then
    _pass "--help documents --install-missing / --no-install"
else
    _fail "--help doesn't document the install flags"
fi
_im_state=$(mktemp -d 2>/dev/null || mktemp -d -t ldim)
out=$(TEXERA_LOCAL_DEV_DIR="$_im_state" "$SCRIPT" up --install-missing --no-install 2>&1); rc=$?
if (( rc == 2 )); then
    _pass "up rejects --install-missing together with --no-install (rc=2)"
else
    _fail "up accepted contradictory install flags" "rc=$rc out=$(echo "$out" | head -1)"
fi
rm -rf "$_im_state"

printf "\n%d passed, %d failed\n" "$PASS" "$FAIL"
(( FAIL == 0 ))
