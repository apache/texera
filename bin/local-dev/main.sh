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

# bin/local-dev.sh -- Manage the Texera local dev stack from a single script.
#
# Subcommands:
#   bin/local-dev.sh                          DEFAULT — one-shot text
#                                             dashboard (same as `status`).
#                                             Non-interactive, prints once and
#                                             exits — safe in scripts/CI.
#   bin/local-dev.sh -i  | --interactive      Launch the Textual TUI dashboard
#                                             (live states, SRC dirty
#                                             indicator, command prompt,
#                                             double-click for logs, ↑/↓
#                                             history, Ctrl-C twice to quit).
#                                             Requires Python + textual.
#   bin/local-dev.sh status [--json]          same as no-arg invocation. With
#                                             --json, print one machine-readable
#                                             JSON object (no table) and exit 0
#                                             iff every service is running — the
#                                             contract for agents/scripts.
#   bin/local-dev.sh up   [--fresh|--build|--no-build] [--skip=svc1,svc2] [--json]
#                         [--worktree=PATH | --branch=NAME]
#                                             Default: skip build if no source/lock
#                                             changes since last build. --build forces
#                                             incremental sbt dist + yarn/bun install.
#                                             --fresh runs `sbt clean dist`. --no-build
#                                             skips the build step entirely. --json
#                                             sends progress to stderr and the final
#                                             status JSON to stdout.
#                                             DEPLOY SOURCE: with no selector the stack
#                                             is built/run from THIS checkout. Point it
#                                             at a sibling git worktree with
#                                             --worktree=PATH or --branch=NAME (the
#                                             worktree that has NAME checked out) to
#                                             deploy a PR branch without disturbing the
#                                             main checkout. The choice is persisted, so
#                                             later status / down / logs / <svc> / auto
#                                             all act on it (run a plain `up` to return
#                                             to this checkout). local-dev.sh itself
#                                             always runs from this checkout — so if the
#                                             target branch modifies bin/local-dev/**,
#                                             those tooling changes are NOT in effect;
#                                             checkout that branch and run its own
#                                             local-dev.sh instead (a warning is printed
#                                             when such drift is detected).
#   bin/local-dev.sh down [--skip=svc1,svc2] [--json]
#                                             stop every non-skipped service
#                                             (--json: summary JSON on stdout).
#   bin/local-dev.sh start <service>          start one service (no rebuild).
#   bin/local-dev.sh stop  <service>          stop one service.
#   bin/local-dev.sh <service>                rebuild only that service incrementally
#                                             (sbt <Project>/dist), then bounce it.
#                                             frontend / agent-service are refused
#                                             (they have their own watch mode).
#   bin/local-dev.sh logs <service>           tail this service's log.
#   bin/local-dev.sh w | watch [interval]     Hands-off monitor: redraw the
#                                             dashboard every <interval>s
#                                             (default 2). No prompt; Ctrl-C
#                                             to exit.
#
# Managed services (start order):
#   config-service                 :9094  JVM (sbt ConfigService)
#   access-control-service         :9096  JVM (sbt AccessControlService)
#   file-service                   :9092  JVM (sbt FileService)
#   workflow-compiling-service     :9090  JVM (sbt WorkflowCompilingService)
#   computing-unit-managing-service :8082 JVM (sbt ComputingUnitManagingService)
#   texera-web                     :8080  JVM (sbt WorkflowExecutionService, amber)
#   computing-unit-master          :8085  JVM (rides amber dist; no own sbt project)
#   agent-service                  :3001  Bun --watch (cd agent-service && bun run dev)
#   frontend                       :4200  ng serve via cd frontend && yarn start
#
# Docker infra (postgres / minio / lakefs / lakekeeper / litellm) IS managed
# here: `up` brings it up via `docker compose` (project texera-local-dev) and
# `down` tears down any docker targets. The script warns if expected ports are
# unreachable.
#
# Logs and pid book-keeping live under: ${TEXERA_LOCAL_DEV_DIR:-/tmp/texera-local-dev}

set -euo pipefail
# Unmatched globs in bash default to the literal pattern (we handle that
# in-place at every glob site). `failglob` / `nullglob` are opt-in per glob
# via `( shopt -s nullglob; ... )` subshells where we need empty-on-no-match.

# --------- self tree vs deploy source ---------
# The orchestration tooling — this script, tui.py, and the docker overlay —
# always runs from the checkout it physically lives in: the "self" tree
# (normally the canonical `texera` clone). The *application* we build and run,
# though, can be redirected to a sibling git worktree so you can deploy a PR
# branch without disturbing the main checkout. That target is the "source"
# tree.
#
#   bin/local-dev.sh up --worktree=PATH    deploy from an explicit worktree dir
#   bin/local-dev.sh up --branch=NAME      deploy from the worktree that has
#                                          NAME checked out
#   bin/local-dev.sh up                    deploy from this (self) checkout again
#
# The selection is persisted to $STATE_DIR/deploy-source, so every later
# command (status, logs, down, single-service rebuild, auto) reads it back and
# acts on the SAME deployment. REPO_ROOT below is pointed at the source tree, which
# is what the rest of the script keys every build/run/git operation off of —
# only the handful of tooling-file paths are pinned to SELF_ROOT.
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"      # bin/local-dev in the self tree
SELF_ROOT="$(cd "$SELF_DIR/../.." && pwd)"     # self checkout root (tooling source)

STATE_DIR="${TEXERA_LOCAL_DEV_DIR:-/tmp/texera-local-dev}"
LOG_DIR="$STATE_DIR/logs"
# Per-service phase markers: shell writes `<phase>\t<epoch>` here as it
# walks each service through stop → build → start; the TUI reads them
# every tick and renders an animated transitional state in the STATE
# column. Removed once the service is up / on stale-after-90s.
PHASE_DIR="$STATE_DIR/svc-phase"
DEPLOY_SOURCE_FILE="$STATE_DIR/deploy-source"
mkdir -p "$LOG_DIR" "$PHASE_DIR"

# Absolute path of a checkout's git object store (the common dir). For a
# worktree this resolves to the shared `.git` of the main clone, so two trees
# of the same repo report the same value — that's how we tell a real sibling
# worktree apart from an unrelated repo.
_git_common_abs() {
    local dir="$1" c=""
    c="$(git -C "$dir" rev-parse --git-common-dir 2>/dev/null)" || return 1
    [[ -n "$c" ]] || return 1
    case "$c" in /*) ;; *) c="$dir/$c" ;; esac
    ( cd "$c" 2>/dev/null && pwd ) || return 1
}

# Validate a candidate deploy-source dir: a directory holding a build.sbt that
# shares this repo's git object store. Echoes the canonical abs path on success.
_validate_source_root() {
    local cand="$1" abs="" sc="" cc=""
    [[ -n "$cand" && -d "$cand" ]] || return 1
    abs="$(cd "$cand" 2>/dev/null && pwd)" || return 1
    [[ -f "$abs/build.sbt" ]] || return 1
    sc="$(_git_common_abs "$SELF_ROOT")" || return 1
    cc="$(_git_common_abs "$abs")" || return 1
    [[ "$sc" == "$cc" ]] || return 1
    printf '%s\n' "$abs"
}

# Resolve a branch name to the worktree path that has it checked out (the self
# tree counts — it's a worktree of the shared clone too).
_worktree_for_branch() {
    local want="$1" line="" path=""
    while IFS= read -r line; do
        case "$line" in
            "worktree "*) path="${line#worktree }" ;;
            "branch refs/heads/$want") printf '%s\n' "$path"; return 0 ;;
        esac
    done < <(git -C "$SELF_ROOT" worktree list --porcelain 2>/dev/null)
    return 1
}

# Deploy source resolution:
#   • Read-only commands (status / down / logs / <svc>) follow whatever the last
#     up/auto deployed — read it back from the persisted pointer. A stale
#     pointer (worktree removed/moved) is dropped silently.
#   • up / auto re-decide the deployment: --worktree=PATH / --branch=NAME selects
#     a sibling worktree. With no selector, `up` means THIS (self) checkout,
#     while `auto` keeps following the active deployment so the edit→bounce loop
#     stays on it. Either way we (re)persist so read-only commands follow it.
SOURCE_ROOT="$SELF_ROOT"
if [[ -f "$DEPLOY_SOURCE_FILE" ]]; then
    _persisted="$(cat "$DEPLOY_SOURCE_FILE" 2>/dev/null || true)"
    if _valid="$(_validate_source_root "$_persisted")"; then
        SOURCE_ROOT="$_valid"
    else
        rm -f "$DEPLOY_SOURCE_FILE"
    fi
fi

# up/auto must resolve their target BEFORE build.sbt is parsed (version + sbt
# dep graph key off the source tree), so peek the args here — cmd_up / cmd_auto
# re-see and no-op the selectors in their own parse loops.
if [[ "${1:-}" == "up" || "${1:-}" == "auto" ]]; then
    # `up` with no selector resets to this checkout; `auto` keeps the pointer
    # value already resolved above.
    [[ "${1:-}" == "up" ]] && SOURCE_ROOT="$SELF_ROOT"
    for _arg in "${@:2}"; do
        case "$_arg" in
            --worktree=*)
                _t="${_arg#--worktree=}"
                if _v="$(_validate_source_root "$_t")"; then
                    SOURCE_ROOT="$_v"
                else
                    printf "FATAL: --worktree=%s is not a valid texera worktree\n" "$_t" >&2
                    printf "       (need a directory with build.sbt that shares this repo's .git).\n" >&2
                    exit 1
                fi ;;
            --branch=*)
                _b="${_arg#--branch=}"
                if _wt="$(_worktree_for_branch "$_b")" && _v="$(_validate_source_root "$_wt")"; then
                    SOURCE_ROOT="$_v"
                else
                    printf "FATAL: no git worktree has branch '%s' checked out.\n" "$_b" >&2
                    printf "       Create one first, e.g.:\n" >&2
                    printf "         git worktree add ../texera-worktrees/%s %s\n" "${_b//\//-}" "$_b" >&2
                    exit 1
                fi ;;
        esac
    done
    # (Re)persist so read-only commands follow this deployment. Self is the
    # "no worktree" state, represented by the absence of the pointer file.
    if [[ "$SOURCE_ROOT" == "$SELF_ROOT" ]]; then
        rm -f "$DEPLOY_SOURCE_FILE"
    else
        printf '%s\n' "$SOURCE_ROOT" > "$DEPLOY_SOURCE_FILE"
    fi
fi

REPO_ROOT="$SOURCE_ROOT"
export TEXERA_DEPLOY_SOURCE="$SOURCE_ROOT"   # tui.py reads this for its banner
cd "$REPO_ROOT"

# Build stamps are content-hashes of the source tree, so they MUST be scoped
# per deploy source — otherwise a stamp from tree A could suppress the
# (required) first build of tree B, whose target/ is still empty, and the JVM
# launchers would be missing at start time. Namespace them by a stable id
# derived from the absolute source path.
_SRC_ID="$(printf '%s' "$SOURCE_ROOT" | { shasum 2>/dev/null || sha1sum 2>/dev/null || cksum; } | tr -dc 'a-f0-9' | cut -c1-12)"
[[ -z "$_SRC_ID" ]] && _SRC_ID="default"
BUILD_STAMP_DIR="$STATE_DIR/build-stamps/$_SRC_ID"
mkdir -p "$BUILD_STAMP_DIR"

# --------- associative-array shim for bash 3.2 ---------
# Apple ships bash 3.2 at /bin/bash and we ship licensing as bash 3.2 too,
# so we can't use `declare -A`. Every old `MAP[$key]=val` / `${MAP[$key]}`
# is reshaped as a function call against a flat namespace of synthetic
# variables `_amap__<MAP>__<key>`. Keys containing `-` get mangled to `_`
# so they're valid identifier chars.
amap_set() {
    local _map="$1" _key="${2//-/_}" _val="$3"
    eval "_amap__${_map}__${_key}=\$_val"
}
amap_get() {
    local _map="$1" _key="${2//-/_}"
    local _var="_amap__${_map}__${_key}"
    eval "printf '%s' \"\${$_var:-}\""
}
amap_has() {
    local _map="$1" _key="${2//-/_}"
    local _var="_amap__${_map}__${_key}"
    eval "[[ \${$_var+x} ]]"
}
amap_append() {
    # amap_append MAP key suffix — append to existing value (or seed it).
    local _map="$1" _key="${2//-/_}" _suffix="$3"
    local _var="_amap__${_map}__${_key}"
    eval "$_var=\"\${$_var:-}\$_suffix\""
}

# --------- toolchain (JDK 17 + node) ---------
# Detect a JDK 17 installation rather than pinning one path. We try, in
# order: (1) caller-set $JAVA_HOME if it really is 17, (2) macOS's official
# locator `/usr/libexec/java_home -v 17`, (3) Homebrew on Apple Silicon +
# Intel, (4) common Linux distro paths (openjdk / temurin / corretto / zulu),
# (5) SDKMAN, (6) asdf, (7) the `java` on PATH if its `-version` says 17.
# Fall through to a clear install hint if none match.
_java_is_17() {
    local home="$1"
    [[ -x "$home/bin/java" ]] || return 1
    "$home/bin/java" -version 2>&1 | head -1 | grep -q '"17[.]' || return 1
    return 0
}

_find_jdk17() {
    local cand=""
    # 1. Respect $JAVA_HOME if the caller already set it AND it's 17.
    if [[ -n "${JAVA_HOME:-}" ]] && _java_is_17 "$JAVA_HOME"; then
        printf '%s\n' "$JAVA_HOME"; return 0
    fi
    # 2. macOS native locator (works for any vendor installed via /Library).
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        cand=$(/usr/libexec/java_home -v 17 2>/dev/null) || cand=""
        if [[ -n "$cand" ]] && _java_is_17 "$cand"; then
            printf '%s\n' "$cand"; return 0
        fi
    fi
    # 3. Homebrew — try `brew --prefix openjdk@17` first, then both well-
    #    known prefixes as a fallback (script may run without brew on PATH
    #    if /etc/zprofile didn't fire).
    if command -v brew >/dev/null 2>&1; then
        cand=$(brew --prefix openjdk@17 2>/dev/null) || cand=""
        [[ -n "$cand" ]] && _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
    fi
    for cand in /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17; do
        _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
    done
    # 4. Linux distro layouts. Glob first match.
    # bash equivalents of zsh's `*(N)` "null on no match" qualifier: enable
    # `shopt -s nullglob` locally so an unmatched pattern expands to zero
    # words. We localize it in a subshell so it doesn't leak out and break
    # the unquoted globs elsewhere (e.g. unzip's zip glob).
    local matched=""
    matched=$(shopt -s nullglob; \
        for cand in \
            /usr/lib/jvm/java-17-openjdk* \
            /usr/lib/jvm/temurin-17-jdk* \
            /usr/lib/jvm/java-17-amazon-corretto* \
            /usr/lib/jvm/zulu-17* \
            /usr/lib/jvm/jdk-17*; do
            printf '%s\n' "$cand"
        done)
    while IFS= read -r cand; do
        [[ -z "$cand" ]] && continue
        _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
    done <<< "$matched"
    # 5. SDKMAN (`sdk install java 17.x-...`) — pick the lex-largest 17.* dir.
    if [[ -d "$HOME/.sdkman/candidates/java" ]]; then
        matched=$(shopt -s nullglob; \
            for cand in "$HOME"/.sdkman/candidates/java/17.*; do
                printf '%s\n' "$cand"
            done)
        while IFS= read -r cand; do
            [[ -z "$cand" ]] && continue
            _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
        done <<< "$matched"
    fi
    # 6. asdf.
    if [[ -d "$HOME/.asdf/installs/java" ]]; then
        matched=$(shopt -s nullglob; \
            for cand in "$HOME"/.asdf/installs/java/*17*; do
                printf '%s\n' "$cand"
            done)
        while IFS= read -r cand; do
            [[ -z "$cand" ]] && continue
            _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
        done <<< "$matched"
    fi
    # 7. Whatever `java` is on PATH, IF it's 17 — covers cases like Docker
    #    images or distro-managed defaults.
    cand=$(command -v java 2>/dev/null) || cand=""
    if [[ -n "$cand" ]]; then
        cand="$(dirname "$(dirname "$cand")")"
        _java_is_17 "$cand" && { printf '%s\n' "$cand"; return 0; }
    fi
    return 1
}

# Print the current java environment alongside detection results so the
# user can see *why* JDK 17 lookup failed (wrong version pinned, JAVA_HOME
# pointing at JDK 21, etc) — not just "couldn't find one".
_diagnose_jdk17() {
    echo "" >&2
    echo "  current java environment:" >&2
    if [[ -n "${JAVA_HOME:-}" ]]; then
        local jhver=""
        jhver=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 || echo "(unreadable)")
        echo "    \$JAVA_HOME = $JAVA_HOME" >&2
        echo "                ↳ $jhver" >&2
    else
        echo "    \$JAVA_HOME (unset)" >&2
    fi
    local path_java=""
    path_java=$(command -v java 2>/dev/null) || true
    if [[ -n "$path_java" ]]; then
        local pver=""
        pver=$("$path_java" -version 2>&1 | head -1 || echo "(unreadable)")
        echo "    \`java\` on PATH → $path_java" >&2
        echo "                ↳ $pver" >&2
    else
        echo "    \`java\` on PATH (not found)" >&2
    fi
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        local jh17=""
        jh17=$(/usr/libexec/java_home -v 17 2>/dev/null) || jh17="(none registered)"
        echo "    /usr/libexec/java_home -v 17 → $jh17" >&2
    fi
    if command -v brew >/dev/null 2>&1; then
        local brewp=""
        brewp=$(brew --prefix openjdk@17 2>/dev/null) || brewp="(not installed)"
        echo "    brew --prefix openjdk@17 → $brewp" >&2
    fi
    echo "" >&2
    echo "  likely cause:" >&2
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -n "$path_java" ]]; then
        echo "    JAVA_HOME points at the wrong JDK (probably not 17)." >&2
        echo "    Either fix JAVA_HOME or unset it to let auto-detect try other paths." >&2
    elif [[ -n "$path_java" ]]; then
        echo "    \`java\` on PATH is not JDK 17. Install a 17 sibling or set JAVA_HOME=/path/to/jdk-17." >&2
    else
        echo "    No JDK is installed anywhere we know to look." >&2
    fi
    echo "" >&2
}

JAVA_HOME_DETECTED="$(_find_jdk17)" || {
    echo "FATAL: could not find a JDK 17 install." >&2
    _diagnose_jdk17
    echo "  fix:" >&2
    echo "    macOS:   brew install openjdk@17" >&2
    echo "    Linux:   apt install openjdk-17-jdk    # or yum/dnf equivalent" >&2
    echo "    SDKMAN:  sdk install java 17.0.13-tem" >&2
    echo "    or set JAVA_HOME=/path/to/jdk-17 explicitly" >&2
    echo "" >&2
    exit 1
}
export JAVA_HOME="$JAVA_HOME_DETECTED"
export PATH="$JAVA_HOME/bin:$PATH"

# Node: source the user's version manager (if any) so the right `node` is on
# PATH for yarn/bun/ng. Try nvm, fnm, volta in that order; `command -v node`
# remains the ultimate fallback.
if [[ -z "${NVM_DIR:-}" && -d "$HOME/.nvm" ]]; then
    export NVM_DIR="$HOME/.nvm"
fi
if [[ -n "${NVM_DIR:-}" && -s "$NVM_DIR/nvm.sh" ]]; then
    # shellcheck disable=SC1091
    \. "$NVM_DIR/nvm.sh" >/dev/null 2>&1 || true
elif command -v fnm >/dev/null 2>&1; then
    eval "$(fnm env --use-on-cd 2>/dev/null)" || true
elif [[ -s "$HOME/.volta/load.sh" ]]; then
    # shellcheck disable=SC1091
    \. "$HOME/.volta/load.sh" >/dev/null 2>&1 || true
fi

# --------- runtime env for backend ---------
# Detect the host's primary LAN IP so we can use it as the MinIO endpoint.
# It has to be the same string from both directions:
#   • host-native JVMs need it to reach localhost-published port 9000
#   • the lakekeeper container needs it to do server-side S3 ops (validation,
#     compaction) AND to return URLs to clients that *they* can reach
# `localhost` only works for the host. `texera-minio` only works inside the
# docker network. The host's LAN IP works from BOTH (host loopback for the
# host, docker NAT'd out-and-back for the container).
_detect_host_lan_ip() {
    local iface="" ip=""
    # 1. The interface backing the default route — most reliable on a
    #    laptop that may have wifi + thunderbolt + tailscale all active.
    iface=$(route get default 2>/dev/null | awk '/interface:/{print $2; exit}')
    if [[ -n "$iface" ]]; then
        ip=$(ipconfig getifaddr "$iface" 2>/dev/null)
        [[ -n "$ip" && "$ip" != 127.* ]] && { printf '%s\n' "$ip"; return 0; }
    fi
    # 2. Fallback: linux `hostname -I`-equivalent walk over en*.
    for iface in en0 en1 en2 en3 en4 en5 en6 en7 en8 en9 en10; do
        ip=$(ipconfig getifaddr "$iface" 2>/dev/null)
        [[ -n "$ip" && "$ip" != 127.* ]] && { printf '%s\n' "$ip"; return 0; }
    done
    return 1
}
# Lazy resolver — called from subcommands that actually need to publish a
# host-reachable S3 endpoint (cmd_up, cmd_auto). Subcommands like
# `version`, `status`, `--help`, or `-i` don't talk to MinIO and shouldn't
# refuse to run just because the laptop is offline.
_require_host_lan_ip() {
    [[ -n "${HOST_LAN_IP:-}" ]] && return 0
    HOST_LAN_IP="$(_detect_host_lan_ip)" || HOST_LAN_IP=""
    if [[ -z "$HOST_LAN_IP" ]]; then
        echo "FATAL: could not detect a host LAN IP." >&2
        echo "       MinIO needs an address reachable from both docker (lakekeeper" >&2
        echo "       does S3 ops) and the host (JVMs read signed URLs back); none" >&2
        echo "       of \`route get default\` / en0-en10 had a non-loopback IPv4." >&2
        echo "       Connect to a network or export HOST_LAN_IP=<your-IP> explicitly." >&2
        exit 1
    fi
    export HOST_LAN_IP
    # STORAGE_S3_ENDPOINT below uses the parameter-default form so it
    # picks up HOST_LAN_IP once we set it here.
    export STORAGE_S3_ENDPOINT="${STORAGE_S3_ENDPOINT:-http://$HOST_LAN_IP:9000}"
}

export STORAGE_JDBC_URL="${STORAGE_JDBC_URL:-jdbc:postgresql://localhost:5432/texera_db?currentSchema=texera_db,public}"
export STORAGE_JDBC_USERNAME="${STORAGE_JDBC_USERNAME:-texera}"
export STORAGE_JDBC_PASSWORD="${STORAGE_JDBC_PASSWORD:-password}"
# STORAGE_S3_ENDPOINT is set lazily by _require_host_lan_ip — only the
# subcommands that actually touch MinIO (infra_up + cmd_up + cmd_auto)
# trigger that detection, so `version` / `status` / `-i` work offline.
export STORAGE_S3_AUTH_USERNAME="${STORAGE_S3_AUTH_USERNAME:-texera_minio}"
export STORAGE_S3_AUTH_PASSWORD="${STORAGE_S3_AUTH_PASSWORD:-password}"
export STORAGE_S3_REGION="${STORAGE_S3_REGION:-us-west-2}"
export STORAGE_ICEBERG_CATALOG_TYPE="${STORAGE_ICEBERG_CATALOG_TYPE:-rest}"
export STORAGE_ICEBERG_CATALOG_REST_URI="${STORAGE_ICEBERG_CATALOG_REST_URI:-http://localhost:8181/catalog}"
export STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME="${STORAGE_ICEBERG_CATALOG_REST_WAREHOUSE_NAME:-texera}"
export STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET="${STORAGE_ICEBERG_CATALOG_REST_S3_BUCKET:-texera-iceberg}"
export STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME="${STORAGE_ICEBERG_CATALOG_POSTGRES_USERNAME:-texera}"
export STORAGE_ICEBERG_CATALOG_POSTGRES_PASSWORD="${STORAGE_ICEBERG_CATALOG_POSTGRES_PASSWORD:-password}"
export STORAGE_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME="${STORAGE_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME:-localhost:5432/texera_iceberg_catalog}"
export STORAGE_LAKEFS_ENDPOINT="${STORAGE_LAKEFS_ENDPOINT:-http://localhost:8000/api/v1}"
export STORAGE_LAKEFS_AUTH_USERNAME="${STORAGE_LAKEFS_AUTH_USERNAME:-AKIAIOSFOLKFSSAMPLES}"
export STORAGE_LAKEFS_AUTH_PASSWORD="${STORAGE_LAKEFS_AUTH_PASSWORD:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY}"
export STORAGE_LAKEFS_AUTH_API_SECRET="${STORAGE_LAKEFS_AUTH_API_SECRET:-random_string_for_lakefs}"
export UDF_PYTHON_PATH="${UDF_PYTHON_PATH:-$(command -v python3 2>/dev/null || command -v python 2>/dev/null)}"
export TEXERA_DASHBOARD_SERVICE_ENDPOINT="${TEXERA_DASHBOARD_SERVICE_ENDPOINT:-http://localhost:8080}"
export WORKFLOW_COMPILING_SERVICE_ENDPOINT="${WORKFLOW_COMPILING_SERVICE_ENDPOINT:-http://localhost:9090}"
export WORKFLOW_EXECUTION_SERVICE_ENDPOINT="${WORKFLOW_EXECUTION_SERVICE_ENDPOINT:-http://localhost:8085}"
export FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT="${FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT:-http://localhost:9092/api/dataset/presign-download}"
export FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT="${FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT:-http://localhost:9092/api/dataset/did/upload}"
export LITELLM_BASE_URL="${LITELLM_BASE_URL:-http://localhost:4000}"
export LITELLM_MASTER_KEY="${LITELLM_MASTER_KEY:-sk-texera-internal-do-not-share}"
export LLM_ENDPOINT="${LLM_ENDPOINT:-http://localhost:8080}"
export LLM_API_KEY="${LLM_API_KEY:-dummy}"

# --------- texera version (dynamic) ---------
# The sbt-native-packager dist directory and jar names embed the project
# version (e.g. target/config-service-<VERSION>/...). That version moves
# across branches (1.3.0-incubating-SNAPSHOT on main, 1.2.0-incubating on
# release/v1.2, …) so resolve it from build.sbt at startup rather than
# hardcoding. Override via the TEXERA_VERSION env var to target a sibling
# tree or if the build.sbt parse fails.
_texera_version() {
    grep -E '^[[:space:]]*ThisBuild[[:space:]]*/[[:space:]]*version[[:space:]]*:=[[:space:]]*"' \
        "$REPO_ROOT/build.sbt" 2>/dev/null \
        | head -1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
}
TEXERA_VERSION="${TEXERA_VERSION:-$(_texera_version)}"
if [[ -z "$TEXERA_VERSION" ]]; then
    # tui_warn isn't defined yet at this point in the script; print raw.
    printf "FATAL: could not detect texera version from %s/build.sbt\n" "$REPO_ROOT" >&2
    printf "       Set the TEXERA_VERSION env var to bypass.\n" >&2
    exit 1
fi

# --------- sbt dependency graph (parsed from build.sbt) ---------
# We parse the build.sbt's project + .dependsOn() declarations so that the
# per-service "did anything that affects me change?" check can prune to
# the actual transitive closure. Without this, every JVM service watches
# every common/* dir and `cmd_auto` rebuilds half the stack whenever
# common/workflow-operator changes, even though config-service has zero
# dependency on it.
#
# Populates two amap maps keyed by sbt project name plus a parallel
# indexed-array of keys so we can iterate without `${!MAP[@]}`:
#   amap_get SBT_PATH ConfigService → "config-service"
#   amap_get SBT_DEPS ConfigService → "Auth Config"   (space-separated)
#   SBT_DEPS_KEYS=(ConfigService AccessControlService …)
SBT_DEPS_KEYS=()

_parse_sbt() {
    local file="$REPO_ROOT/build.sbt"
    [[ -f "$file" ]] || return 1
    local current="" line=""
    local decl_re='^lazy[[:space:]]+val[[:space:]]+([A-Z][A-Za-z0-9]*)[[:space:]]*=[[:space:]]*\(project[[:space:]]+in[[:space:]]+file\("([^"]+)"\)\)'
    local deps_re='\.dependsOn\(([^)]*)\)'

    # Helper: split a comma-list of dependsOn args, dropping test-scope
    # and non-project tokens, then append unique main-scope refs.
    _absorb() {
        local args="$1"
        local IFS_OLD="$IFS"
        IFS=','
        # shellcheck disable=SC2206
        local parts=($args)
        IFS="$IFS_OLD"
        local arg="" existing=""
        for arg in "${parts[@]}"; do
            arg="${arg// /}"
            arg="${arg//$'\t'/}"
            # Drop ONLY test-scope deps. The previous catch-all `*"%"*`
            # match silently swallowed any future `%`-scoped main dep
            # (e.g. `X % "compile->compile"`, `X % Provided`) and would
            # have broken dirty-detection on it.
            if [[ "$arg" =~ %\"?test(-\>[^\"]*)?\"? ]] || [[ "$arg" =~ %Test$ ]]; then
                continue
            fi
            [[ "$arg" =~ ^[A-Z] ]] || continue
            existing=$(amap_get SBT_DEPS "$current")
            case " $existing " in
                *" $arg "*) ;;
                *) amap_append SBT_DEPS "$current" " $arg" ;;
            esac
        done
    }

    while IFS= read -r line; do
        local rest="$line"
        if [[ "$line" =~ $decl_re ]]; then
            current="${BASH_REMATCH[1]}"
            amap_set SBT_PATH "$current" "${BASH_REMATCH[2]}"
            amap_set SBT_DEPS "$current" ""
            SBT_DEPS_KEYS+=("$current")
            # Don't `continue` — `.dependsOn(...)` can chain on the SAME
            # line as the declaration (WorkflowOperator's one-liner does
            # this). Fall through so the loop below catches it.
        elif [[ "$line" =~ ^lazy[[:space:]]+val ]]; then
            # Other `lazy val` (settings, etc) breaks attribution.
            current=""
            continue
        fi
        [[ -z "$current" ]] && continue
        # Scan ALL `.dependsOn(...)` matches on this line — some lines
        # chain `.dependsOn(A).dependsOn(B % "test->test")` and we must
        # see both to filter correctly. Snapshot BASH_REMATCH BEFORE
        # calling `_absorb` — that helper does its own `[[ … =~ … ]]`
        # for the test-scope filter, which clobbers `BASH_REMATCH[0]`
        # under us. Without the snapshot the `#*…` trim referenced the
        # wrong value, the strip silently no-op'd, and the outer while
        # looped forever on the first `.dependsOn(…)`.
        local _match="" _captured=""
        while [[ "$rest" =~ $deps_re ]]; do
            _match="${BASH_REMATCH[0]}"
            _captured="${BASH_REMATCH[1]}"
            _absorb "$_captured"
            rest="${rest#*"$_match"}"
        done
    done < "$file"
    # Trim leading space on every entry.
    local p="" cur=""
    for p in "${SBT_DEPS_KEYS[@]}"; do
        cur=$(amap_get SBT_DEPS "$p")
        amap_set SBT_DEPS "$p" "${cur# }"
    done
    return 0
}
_parse_sbt || true

# BFS from $1 over SBT_DEPS, emit one `<path>/src` per visited project.
# Caller can also pass an explicit fallback list for the unparseable case.
_sbt_transitive_src_dirs() {
    local root="$1"
    [[ -z "$root" ]] && return 1
    amap_has SBT_PATH "$root" || return 1
    # `_visited_` namespace is a per-call amap. We can't easily clear all
    # entries (bash 3.2 has no `${!_visited_*}` glob over set vars without
    # `compgen`, but `compgen -v` IS available). We unset matching vars
    # at function exit so successive calls start clean.
    local queue=("$root")
    local p="" path="" d="" key="" var=""
    while ((${#queue[@]} > 0)); do
        p="${queue[0]}"
        queue=("${queue[@]:1}")
        key="${p//-/_}"
        var="_amap__visited__${key}"
        eval "[[ \${$var+x} ]]" && continue
        eval "$var=1"
        path=$(amap_get SBT_PATH "$p")
        [[ -n "$path" ]] && printf '%s/src\n' "$path"
        local deps=""
        deps=$(amap_get SBT_DEPS "$p")
        for d in $deps; do
            queue+=("$d")
        done
    done
    # Clean up our visited markers so the next caller starts fresh.
    local v=""
    for v in $(compgen -v _amap__visited__ 2>/dev/null); do
        unset "$v"
    done
    return 0
}

# --------- service catalog ---------
SERVICES=(
    postgres
    minio
    lakefs
    lakekeeper
    litellm
    config-service
    access-control-service
    file-service
    workflow-compiling-service
    computing-unit-master
    computing-unit-managing-service
    texera-web
    agent-service
    frontend
)

# Service catalog. Under bash 3.2 this would normally be `typeset -A` maps;
# we use the `amap_*` helpers defined above instead. Each amap_set call
# stores into a synthetic var name (e.g. `_amap__SVC_TYPE__postgres`).

# Each docker service is now its own row in the dashboard. start/stop still
# batch through infra_up/infra_down because `docker compose up -d` and
# `docker compose down` operate at the project level.
amap_set SVC_TYPE postgres   docker; amap_set SVC_PORT postgres   5432; amap_set SVC_CWD postgres   "."
amap_set SVC_TYPE minio      docker; amap_set SVC_PORT minio      9000; amap_set SVC_CWD minio      "."
amap_set SVC_TYPE lakefs     docker; amap_set SVC_PORT lakefs     8000; amap_set SVC_CWD lakefs     "."
amap_set SVC_TYPE lakekeeper docker; amap_set SVC_PORT lakekeeper 8181; amap_set SVC_CWD lakekeeper "."
amap_set SVC_TYPE litellm    docker; amap_set SVC_PORT litellm    4000; amap_set SVC_CWD litellm    "."

amap_set SVC_TYPE       config-service jvm
amap_set SVC_PORT       config-service 9094
amap_set SVC_SBT        config-service ConfigService
amap_set SVC_LAUNCHER   config-service "target/config-service-${TEXERA_VERSION}/bin/config-service"
amap_set SVC_CWD        config-service "."
amap_set SVC_ZIP_GLOB   config-service "config-service/target/universal/config-service-*.zip"
amap_set SVC_UNZIP_DEST config-service "target/"
amap_set SVC_HEALTH     config-service "/api/healthcheck"

amap_set SVC_TYPE       access-control-service jvm
amap_set SVC_PORT       access-control-service 9096
amap_set SVC_SBT        access-control-service AccessControlService
amap_set SVC_LAUNCHER   access-control-service "target/access-control-service-${TEXERA_VERSION}/bin/access-control-service"
amap_set SVC_CWD        access-control-service "."
amap_set SVC_ZIP_GLOB   access-control-service "access-control-service/target/universal/access-control-service-*.zip"
amap_set SVC_UNZIP_DEST access-control-service "target/"
amap_set SVC_HEALTH     access-control-service "/api/healthcheck"

amap_set SVC_TYPE       file-service jvm
amap_set SVC_PORT       file-service 9092
amap_set SVC_SBT        file-service FileService
amap_set SVC_LAUNCHER   file-service "target/file-service-${TEXERA_VERSION}/bin/file-service"
amap_set SVC_CWD        file-service "."
amap_set SVC_ZIP_GLOB   file-service "file-service/target/universal/file-service-*.zip"
amap_set SVC_UNZIP_DEST file-service "target/"
amap_set SVC_HEALTH     file-service "/api/healthcheck"

amap_set SVC_TYPE       workflow-compiling-service jvm
amap_set SVC_PORT       workflow-compiling-service 9090
amap_set SVC_SBT        workflow-compiling-service WorkflowCompilingService
amap_set SVC_LAUNCHER   workflow-compiling-service "target/workflow-compiling-service-${TEXERA_VERSION}/bin/workflow-compiling-service"
amap_set SVC_CWD        workflow-compiling-service "."
amap_set SVC_ZIP_GLOB   workflow-compiling-service "workflow-compiling-service/target/universal/workflow-compiling-service-*.zip"
amap_set SVC_UNZIP_DEST workflow-compiling-service "target/"
amap_set SVC_HEALTH     workflow-compiling-service "/api/healthcheck"

amap_set SVC_TYPE       computing-unit-managing-service jvm
amap_set SVC_PORT       computing-unit-managing-service 8082
amap_set SVC_SBT        computing-unit-managing-service ComputingUnitManagingService
amap_set SVC_LAUNCHER   computing-unit-managing-service "target/computing-unit-managing-service-${TEXERA_VERSION}/bin/computing-unit-managing-service"
amap_set SVC_CWD        computing-unit-managing-service "."
amap_set SVC_ZIP_GLOB   computing-unit-managing-service "computing-unit-managing-service/target/universal/computing-unit-managing-service-*.zip"
amap_set SVC_UNZIP_DEST computing-unit-managing-service "target/"
amap_set SVC_HEALTH     computing-unit-managing-service ""

amap_set SVC_TYPE       texera-web jvm
amap_set SVC_PORT       texera-web 8080
amap_set SVC_SBT        texera-web WorkflowExecutionService
amap_set SVC_LAUNCHER   texera-web "target/amber-${TEXERA_VERSION}/bin/texera-web-application"
amap_set SVC_CWD        texera-web "amber"
amap_set SVC_ZIP_GLOB   texera-web "amber/target/universal/amber-*.zip"
amap_set SVC_UNZIP_DEST texera-web "amber/target/"
amap_set SVC_HEALTH     texera-web "/api/healthcheck"

# computing-unit-master shares the amber dist with texera-web: sbt-native-
# packager emits both `bin/texera-web-application` and `bin/computing-unit-master`
# launchers under `amber/target/amber-<VERSION>/`. We register it as a separate
# service for status/start/stop but leave SVC_SBT / SVC_ZIP_GLOB empty so the
# build pipeline knows to skip it (the texera-web build path already produces
# its launcher). Source-dir and canary-jar lookups treat it identically to
# texera-web — see _svc_src_dirs / svc_src_changed / svc_artifact_mtime.
amap_set SVC_TYPE       computing-unit-master jvm
amap_set SVC_PORT       computing-unit-master 8085
amap_set SVC_SBT        computing-unit-master ""
amap_set SVC_LAUNCHER   computing-unit-master "target/amber-${TEXERA_VERSION}/bin/computing-unit-master"
amap_set SVC_CWD        computing-unit-master "amber"
amap_set SVC_ZIP_GLOB   computing-unit-master ""
amap_set SVC_UNZIP_DEST computing-unit-master ""
amap_set SVC_HEALTH     computing-unit-master ""

amap_set SVC_TYPE   agent-service bun
amap_set SVC_PORT   agent-service 3001
amap_set SVC_CWD    agent-service "agent-service"
amap_set SVC_HEALTH agent-service "/api/healthcheck"

amap_set SVC_TYPE   frontend yarn
amap_set SVC_PORT   frontend 4200
amap_set SVC_CWD    frontend "frontend"
amap_set SVC_HEALTH frontend ""

# --------- docker infra config ---------
DOCKER_PROJECT="texera-local-dev"
# Infra orchestration is part of the tooling, not the deployed app — pin it to
# the self tree so a deployed worktree always comes up against main's known-good
# docker compose (the app schema/DDL it applies still comes from the source tree
# via $REPO_ROOT).
DOCKER_COMPOSE_FILE="$SELF_ROOT/bin/single-node/docker-compose.yml"
DOCKER_OVERLAY_FILE="$SELF_ROOT/bin/local-dev/docker-compose.override.yml"
DOCKER_ENV_FILE="$SELF_ROOT/bin/single-node/.env"
DOCKER_INFRA_SERVICES=(postgres minio minio-init lakefs lakekeeper-migrate lakekeeper lakekeeper-init litellm)
DOCKER_INFRA_LONGLIVED=(postgres minio lakefs lakekeeper litellm)  # exclude one-shot init jobs

# Build the array of -f flags: base single-node compose + local-dev overlay
# (the overlay publishes infra ports to the host, which the upstream compose
# intentionally does not do).
docker_compose_files() {
    local args=(-f "$DOCKER_COMPOSE_FILE")
    [[ -f "$DOCKER_OVERLAY_FILE" ]] && args+=(-f "$DOCKER_OVERLAY_FILE")
    printf '%s\n' "${args[@]}"
}

# --------- TUI helpers ---------
if [[ -t 1 ]] && [[ "${TERM:-}" != "dumb" ]]; then
    BOLD=$'\e[1m'; DIM=$'\e[2m'
    RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'
    BLUE=$'\e[34m'; MAGENTA=$'\e[35m'; CYAN=$'\e[36m'
    GRAY=$'\e[90m'; BRIGHT=$'\e[97m'
    RESET=$'\e[0m'
else
    BOLD="" DIM="" RED="" GREEN="" YELLOW="" BLUE="" MAGENTA="" CYAN="" GRAY="" BRIGHT="" RESET=""
fi

TUI_WIDTH=$(tput cols 2>/dev/null || echo 80)
[[ -z "$TUI_WIDTH" || "$TUI_WIDTH" -lt 60 ]] && TUI_WIDTH=80
(( TUI_WIDTH > 100 )) && TUI_WIDTH=100   # cap for readability

# Symbols
SYM_RUN="●"; SYM_STOP="○"; SYM_WARN="⚠"; SYM_OK="✓"; SYM_ERR="✗"
SYM_SECT="▸"; SYM_LIST="•"; SYM_PROG="→"

tui_hline() {
    local ch="${1:-─}" w="${2:-$TUI_WIDTH}"
    printf "${ch}%.0s" $(seq 1 "$w")
}

tui_trunc() {
    local s="$1" n="$2"
    if (( ${#s} > n )); then
        printf "%s…" "${s:0:$((n-1))}"
    else
        printf "%s" "$s"
    fi
}

tui_banner() {
    local title="$1" subtitle="${2:-}"
    local w=$TUI_WIDTH
    # Row layout: │ + 2 spaces + content + 2 spaces + │ = w  →  inner = w - 6
    local inner=$((w - 6))
    title="$(tui_trunc "$title" "$inner")"
    subtitle="$(tui_trunc "$subtitle" "$inner")"
    printf "${BLUE}╭"; tui_hline "─" $((w-2)); printf "╮${RESET}\n"
    printf "${BLUE}│${RESET}  ${BOLD}${BRIGHT}%-*s${RESET}  ${BLUE}│${RESET}\n" "$inner" "$title"
    if [[ -n "$subtitle" ]]; then
        printf "${BLUE}│${RESET}  ${DIM}%-*s${RESET}  ${BLUE}│${RESET}\n" "$inner" "$subtitle"
    fi
    printf "${BLUE}╰"; tui_hline "─" $((w-2)); printf "╯${RESET}\n"
}

tui_section() {
    printf "\n${BOLD}${MAGENTA}${SYM_SECT}${RESET} ${BOLD}%s${RESET}\n" "$1"
}

tui_ok()    { printf "  ${GREEN}${SYM_OK}${RESET}  %s\n" "$*"; }
tui_err()   { printf "  ${RED}${SYM_ERR}${RESET}  %s\n" "$*"; }
tui_warn()  { printf "  ${YELLOW}${SYM_WARN}${RESET}  %s\n" "$*"; }
tui_info()  { printf "  ${CYAN}${SYM_LIST}${RESET}  %s\n" "$*"; }
tui_step()  { printf "  ${DIM}${SYM_PROG}${RESET}  ${DIM}%s${RESET}\n" "$*"; }
tui_skip()  { printf "  ${GRAY}${SYM_STOP}${RESET}  ${GRAY}%s${RESET}\n" "$*"; }

tui_state_symbol() {
    case "$1" in
        running)              printf "${GREEN}${SYM_RUN}${RESET}" ;;
        starting)             printf "${YELLOW}${SYM_WARN}${RESET}" ;;
        unhealthy|failed)     printf "${RED}${SYM_ERR}${RESET}" ;;
        partial:*|external:*) printf "${YELLOW}${SYM_WARN}${RESET}" ;;
        exited)               printf "${GRAY}${SYM_OK}${RESET}" ;;
        *)                    printf "${GRAY}${SYM_STOP}${RESET}" ;;
    esac
}

tui_state_color() {
    case "$1" in
        running)              echo "$GREEN" ;;
        starting)             echo "$YELLOW" ;;
        unhealthy|failed)     echo "$RED" ;;
        partial:*|external:*) echo "$YELLOW" ;;
        exited)               echo "$GRAY" ;;
        *)                    echo "$GRAY" ;;
    esac
}

# Show a spinner next to $msg while $pid runs. Caller is responsible for
# `wait $pid` afterwards to capture exit code.
tui_spinner() {
    local pid="$1" msg="$2"
    if [[ ! -t 1 ]]; then
        # No cursor control on a pipe, so we can't spin in place. Print one
        # line up front, then a heartbeat every TUI_HEARTBEAT_SECS while the
        # job runs — otherwise a long silent step (e.g. `sbt dist`, whose
        # output is redirected to a log) looks hung to a non-interactive
        # caller polling the stream.
        printf "  ${BLUE}${SYM_PROG}${RESET}  ${DIM}%s (no-TTY)${RESET}\n" "$msg"
        # Poll every 1s (so we return within ~1s of the job finishing — no
        # trailing dead time) but only print a heartbeat every
        # TUI_HEARTBEAT_SECS so the log stays readable.
        local hb_start=$SECONDS hb_every="${TUI_HEARTBEAT_SECS:-15}" hb_last=0 hb_now=0
        while kill -0 "$pid" 2>/dev/null; do
            sleep 1
            hb_now=$((SECONDS - hb_start))
            if (( hb_now - hb_last >= hb_every )); then
                printf "  ${BLUE}${SYM_PROG}${RESET}  ${DIM}… still running (%ds)${RESET}\n" "$hb_now"
                hb_last=$hb_now
            fi
        done
        return
    fi
    # Use an array (vs a single multibyte string + byte indexing) because
    # bash's `${str:i:1}` is byte-wise — each braille glyph is 3 UTF-8
    # bytes, so a byte index would print broken half-chars.
    local frames=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
    local n=${#frames[@]}
    local i=0
    local start_ts=$SECONDS
    printf "\e[?25l"   # hide cursor
    while kill -0 "$pid" 2>/dev/null; do
        local elapsed=$((SECONDS - start_ts))
        local frame="${frames[i % n]}"
        printf "\r  ${BLUE}%s${RESET}  ${DIM}%s${RESET} ${GRAY}(%ds)${RESET}     " \
            "$frame" "$msg" "$elapsed"
        sleep 0.1
        i=$((i+1))
    done
    printf "\r%-${TUI_WIDTH}s\r" " "
    printf "\e[?25h"   # show cursor
}

# Run a command in the background with output captured to $log, show spinner,
# return command's exit code.
tui_run_with_spinner() {
    local log="$1" msg="$2"
    shift 2
    "$@" >"$log" 2>&1 &
    local pid=$!
    tui_spinner "$pid" "$msg"
    wait "$pid"
}

# In-place panel that polls each non-skipped service's port and redraws the
# whole panel until all are healthy or timed out. Sets a trap so Ctrl-C
# restores the cursor.
tui_wait_panel() {
    local svcs=()
    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && continue
        svcs+=("$svc")
    done
    local n=${#svcs[@]}
    (( n == 0 )) && return 0

    local frames=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
    local n_frames=${#frames[@]}
    local start_ts=$SECONDS
    # JVM + docker services bind their port within ~10-30s; 90s leaves slack
    # for cold-cache machines. The frontend's `ng serve` does a full Angular
    # compile before listening — on a fresh checkout that's 90-180s easily,
    # so give yarn-typed services 5 minutes. bun is in between.
    local timeout_default=90
    local timeout_yarn=300
    local timeout_bun=120
    local frame_idx=0
    local first_render=true
    local n_done=0
    local n_failed=0
    local svc="" i=0 state="" state_color="" state_sym="" port_str="" elapsed=0 spinner_frame=""

    if [[ ! -t 1 ]]; then
        # Non-TTY: redrawing would just spam lines. Poll each service to
        # completion (or per-service timeout) and print one line per
        # transition. Crucially we WAIT for docker containers to leave
        # `starting`/`unhealthy` rather than treating an in-flight state as
        # a failure — the previous one-shot check exited rc=1 within ~20s
        # of `up` if e.g. lakefs hadn't reached `running` yet, which is its
        # normal cold-start state.
        local n_done=0 n_failed=0
        local svc_timeout=0 waited=0 final_state=""
        for svc in "${svcs[@]}"; do
            svc_timeout=$timeout_default
            case "$(amap_get SVC_TYPE "$svc")" in
                yarn) svc_timeout=$timeout_yarn ;;
                bun)  svc_timeout=$timeout_bun  ;;
            esac
            waited=0
            final_state=""
            while (( waited < svc_timeout )); do
                if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
                    case "$(docker_svc_state "$svc")" in
                        running|exited)        final_state="ok";    break ;;
                        unhealthy|failed)      final_state="bad";   break ;;
                        # starting / created / restarting / paused / "" → keep waiting
                    esac
                else
                    [[ -n "$(listen_pid_for_port "$(amap_get SVC_PORT "$svc")")" ]] && { final_state="ok"; break; }
                fi
                sleep 1
                waited=$((waited+1))
            done
            case "$final_state" in
                ok)
                    printf "  %s  %-32s :%-6s  %s\n" "$SYM_OK" "$svc" "$(amap_get SVC_PORT "$svc")" "healthy"
                    n_done=$((n_done+1)) ;;
                bad)
                    printf "  %s  %-32s :%-6s  %s\n" "$SYM_ERR" "$svc" "$(amap_get SVC_PORT "$svc")" "unhealthy"
                    n_failed=$((n_failed+1)) ;;
                *)  # timed out without ever reaching a terminal state
                    printf "  %s  %-32s :%-6s  %s\n" "$SYM_ERR" "$svc" "$(amap_get SVC_PORT "$svc")" "timeout (${waited}s)"
                    n_failed=$((n_failed+1)) ;;
            esac
        done
        return $((n_failed > 0))
    fi

    printf "\e[?25l"   # hide cursor
    trap 'printf "\e[?25h"' EXIT INT TERM

    while true; do
        elapsed=$((SECONDS - start_ts))
        n_done=0
        n_failed=0

        if ! $first_render && [[ -t 1 ]]; then
            printf "\e[${n}A"   # move cursor to top of panel
        fi
        first_render=false

        spinner_frame="${frames[frame_idx % n_frames]}"

        for svc in "${svcs[@]}"; do
            state_color="" state_sym=""
            # Per-service wait budget — yarn (ng serve cold compile) gets the
            # most slack, bun a moderate amount, everything else the default.
            local svc_timeout=$timeout_default
            case "$(amap_get SVC_TYPE "$svc")" in
                yarn) svc_timeout=$timeout_yarn ;;
                bun)  svc_timeout=$timeout_bun  ;;
            esac
            if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
                local dstate=""
                dstate=$(docker_svc_state "$svc")
                case "$dstate" in
                    running|exited)
                        state="$dstate"
                        state_color="$GREEN"; state_sym="${GREEN}${SYM_OK}${RESET}"
                        n_done=$((n_done+1)) ;;
                    starting)
                        state="starting (${elapsed}s)"
                        state_color="$YELLOW"; state_sym="${YELLOW}${spinner_frame}${RESET}" ;;
                    unhealthy|failed)
                        state="$dstate"
                        state_color="$RED"; state_sym="${RED}${SYM_ERR}${RESET}"
                        n_failed=$((n_failed+1)) ;;
                    *)
                        if (( elapsed >= svc_timeout )); then
                            state="timeout"
                            state_color="$RED"; state_sym="${RED}${SYM_ERR}${RESET}"
                            n_failed=$((n_failed+1))
                        else
                            state="${dstate} (${elapsed}s)"
                            state_color="$YELLOW"; state_sym="${YELLOW}${spinner_frame}${RESET}"
                        fi ;;
                esac
                port_str=":$(amap_get SVC_PORT "$svc")"
            else
                if [[ -n "$(listen_pid_for_port "$(amap_get SVC_PORT "$svc")")" ]]; then
                    state="healthy"
                    state_color="$GREEN"; state_sym="${GREEN}${SYM_OK}${RESET}"
                    n_done=$((n_done+1))
                elif (( elapsed >= svc_timeout )); then
                    state="timeout — see bin/local-dev.sh logs $svc"
                    state_color="$RED"; state_sym="${RED}${SYM_ERR}${RESET}"
                    n_failed=$((n_failed+1))
                else
                    state="starting (${elapsed}s)"
                    state_color="$YELLOW"; state_sym="${YELLOW}${spinner_frame}${RESET}"
                fi
                port_str=":$(amap_get SVC_PORT "$svc")"
            fi

            [[ -t 1 ]] && printf "\e[2K"
            printf "  %s  %-32s ${DIM}%-7s${RESET}  ${state_color}%s${RESET}\n" \
                "$state_sym" "$svc" "$port_str" "$state"
        done

        if (( n_done + n_failed == n )); then
            break
        fi

        frame_idx=$((frame_idx + 1))
        sleep 0.2
    done

    if [[ -t 1 ]]; then
        printf "\e[?25h"
        trap - EXIT INT TERM
    fi

    return $((n_failed > 0))
}

# --------- toolchain install hints ---------
# Print install instructions for a missing toolchain. Used by both startup
# detection failures and runtime "command not found" surfaces. Keeps the
# guidance in one place so every failure mode looks the same.
_install_hint() {
    local tool="$1"
    case "$tool" in
        java)
            printf "  ${BOLD}install JDK 17:${RESET}\n"
            printf "    macOS:   brew install openjdk@17\n"
            printf "    Linux:   apt install openjdk-17-jdk    ${DIM}# or yum/dnf equivalent${RESET}\n"
            printf "    SDKMAN:  sdk install java 17.0.13-tem\n"
            printf "  or set JAVA_HOME=/path/to/jdk-17 explicitly\n"
            ;;
        python)
            printf "  ${BOLD}install Python 3.10+ and the TUI deps:${RESET}\n"
            printf "    macOS:   brew install python@3.12\n"
            printf "    Linux:   apt install python3 python3-pip\n"
            printf "    then:    python3 -m pip install -r %s/amber/dev-requirements.txt\n" "$REPO_ROOT"
            printf "  or set TEXERA_PYTHON=/path/to/python explicitly (must have textual installed)\n"
            ;;
        node)
            printf "  ${BOLD}install Node 20+ (needed for frontend & agent-service):${RESET}\n"
            printf "    macOS:   brew install node\n"
            printf "    nvm:     nvm install --lts && nvm use --lts\n"
            printf "    fnm:     fnm install --lts\n"
            printf "    volta:   volta install node\n"
            ;;
        yarn)
            printf "  ${BOLD}install yarn (needed for the frontend):${RESET}\n"
            printf "    macOS:   brew install yarn\n"
            printf "    npm:     npm install -g yarn\n"
            printf "    corepack: corepack enable\n"
            ;;
        bun)
            printf "  ${BOLD}install bun (needed for agent-service):${RESET}\n"
            printf "    macOS:   brew install oven-sh/bun/bun\n"
            printf "    curl:    curl -fsSL https://bun.sh/install | bash\n"
            ;;
        sbt)
            printf "  ${BOLD}install sbt (needed to build the JVM services):${RESET}\n"
            printf "    macOS:   brew install sbt\n"
            printf "    Linux:   see https://www.scala-sbt.org/download.html\n"
            ;;
        docker)
            printf "  ${BOLD}install Docker (needed for postgres/minio/lakefs/lakekeeper/litellm):${RESET}\n"
            printf "    macOS:   download Docker Desktop from https://docker.com/products/docker-desktop\n"
            printf "    Linux:   apt install docker.io docker-compose-plugin\n"
            ;;
        *)
            printf "  ${DIM}no install hint for: %s${RESET}\n" "$tool"
            ;;
    esac
}

# Inspect the user's Node/yarn/bun setup and print what we actually find,
# so "yarn not found" reads as "you have node 18 but no yarn" instead of
# the generic install hint. Called from start_one's frontend/agent paths.
_diagnose_node() {
    printf "\n  ${BOLD}node environment:${RESET}\n"
    for tool in node npm yarn bun corepack; do
        local p=""
        p=$(command -v "$tool" 2>/dev/null) || true
        if [[ -n "$p" ]]; then
            local v=""
            v=$("$tool" --version 2>&1 | head -1) || v="(no --version)"
            printf "    ${GREEN}✓${RESET} %-8s → %s  ${DIM}(%s)${RESET}\n" "$tool" "$p" "$v"
        else
            printf "    ${RED}✗${RESET} %-8s ${DIM}not on PATH${RESET}\n" "$tool"
        fi
    done
    printf "\n  ${BOLD}version manager:${RESET}\n"
    if [[ -n "${NVM_DIR:-}" && -s "${NVM_DIR}/nvm.sh" ]]; then
        printf "    nvm  → ${DIM}%s${RESET}\n" "$NVM_DIR"
    elif command -v fnm >/dev/null 2>&1; then
        printf "    fnm  → ${DIM}%s${RESET}\n" "$(command -v fnm)"
    elif [[ -s "$HOME/.volta/load.sh" ]]; then
        printf "    volta → ${DIM}%s${RESET}\n" "$HOME/.volta"
    else
        printf "    ${DIM}none detected (using whatever \`node\` is on PATH)${RESET}\n"
    fi
    printf "\n"
}

# --------- helpers ---------
listen_pid_for_port() {
    # || true so pipefail doesn't kill us when nothing is listening
    lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -1 || true
}

# Branch + short-sha of the deploy source ($REPO_ROOT), tab-separated, each
# falling back to "?" when git can't answer. Single source of truth for the
# banners and the status JSON. Read with:
#   IFS=$'\t' read -r branch sha < <(_git_head)
_git_head() {
    local branch="" sha=""
    branch=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    sha=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "?")
    printf '%s\t%s\n' "$branch" "$sha"
}

# When deploying a sibling worktree, the tooling (this script, tui.py, the
# docker overlay) still runs from the self tree — deliberately. The one case
# where that surprises people is a target branch that itself modifies
# bin/local-dev/**: those changes are NOT in effect. Print a one-line warning
# (informational, non-fatal) so the boundary is visible before someone burns
# time debugging it.
_warn_tooling_drift() {
    [[ "$REPO_ROOT" == "$SELF_ROOT" ]] && return 0
    if ! diff -rq "$SELF_ROOT/bin/local-dev" "$REPO_ROOT/bin/local-dev" >/dev/null 2>&1; then
        tui_warn "target's bin/local-dev/ differs from this checkout's — the tooling runs from HERE, so those changes are NOT in effect"
        printf "     ${DIM}(to exercise the target's tooling changes, run that worktree's own bin/local-dev.sh)${RESET}\n"
    fi
}

# Returns the count of long-lived infra services currently running under our project.
infra_running_count() {
    docker compose -p "$DOCKER_PROJECT" ps --services --filter status=running 2>/dev/null | grep -cxE "$(IFS=\|; echo "${DOCKER_INFRA_LONGLIVED[*]}")" || true
}

# Returns one of: stopped | running | partial:N | external — aggregate view
# across all infra containers, used by infra_up/infra_down and the wait panel.
infra_state() {
    local running=""
    running=$(infra_running_count)
    if [[ "$running" -eq ${#DOCKER_INFRA_LONGLIVED[@]} ]]; then
        echo "running"
    elif [[ "$running" -gt 0 ]]; then
        echo "partial:$running"
    else
        local taken=0
        for port in 5432 9000 8000 8181 4000; do
            [[ -n "$(listen_pid_for_port "$port")" ]] && taken=$((taken+1))
        done
        if [[ "$taken" -gt 0 ]]; then
            echo "external:$taken/5"
        else
            echo "stopped"
        fi
    fi
}

# Cached snapshot of `docker compose ps -a`. Refreshed at most once per second
# (5 docker services × per-row ~200 ms `docker ps` call would otherwise tank
# the render).
_docker_states_cache=""
_docker_states_cache_ts=-1

_refresh_docker_states_cache() {
    if (( _docker_states_cache_ts < 0 || SECONDS - _docker_states_cache_ts >= 1 )); then
        _docker_states_cache=$(docker compose -p "$DOCKER_PROJECT" ps -a \
            --format '{{.Service}}|{{.State}}|{{.Status}}' 2>/dev/null)
        _docker_states_cache_ts=$SECONDS
    fi
}

# Per-service state for any of postgres/minio/lakefs/lakekeeper/litellm.
# Returns one of: running | starting | unhealthy | exited | failed | stopped
docker_svc_state() {
    local svc="$1"
    _refresh_docker_states_cache
    local line=""
    line=$(printf '%s\n' "$_docker_states_cache" | grep "^${svc}|" | head -1 || true)
    if [[ -z "$line" ]]; then
        echo "stopped"
        return
    fi
    local rest="${line#*|}"
    local dstate="${rest%%|*}"     # NB: not `status` — zsh reserves $status
    local dstatus="${rest#*|}"
    case "$dstate" in
        running)
            if [[ "$dstatus" == *'(healthy)'* ]]; then echo "running"
            elif [[ "$dstatus" == *'(health: starting)'* ]]; then echo "starting"
            elif [[ "$dstatus" == *'(unhealthy)'* ]]; then echo "unhealthy"
            else echo "running"
            fi ;;
        exited)
            if [[ "$dstatus" == 'Exited (0)'* ]]; then echo "exited"
            else echo "failed"
            fi ;;
        created|restarting|paused|removing) echo "starting" ;;
        *) echo "stopped" ;;
    esac
}

infra_up() {
    # Resolve the host LAN IP now (lazy) — both the docker compose stack
    # (lakekeeper-init reads STORAGE_S3_ENDPOINT) and the host JVMs about
    # to start need it pointing at a host-reachable MinIO.
    _require_host_lan_ip
    if [[ "$(infra_state)" == external:* ]]; then
        tui_err "infra: ports already taken by non-script containers"
        printf "  ${DIM}Likely an old project (e.g. \`texera-dev\`) is running. Stop it first:${RESET}\n"
        printf "  ${DIM}  docker compose -p texera-dev down${RESET}\n"
        return 1
    fi
    local files=($(docker_compose_files))
    tui_step "infra: docker compose up -d  ${DIM}(in-place TTY progress)${RESET}"
    # No stdout redirect → docker compose detects TTY and renders an in-place
    # progress panel that overwrites itself instead of appending event lines.
    # --progress=tty forces it even if stdout looks like a pipe.
    docker compose --progress auto -p "$DOCKER_PROJECT" --env-file "$DOCKER_ENV_FILE" "${files[@]}" \
        up -d "${DOCKER_INFRA_SERVICES[@]}"
    tui_ok "infra: 5 containers up"
}

infra_down() {
    tui_step "infra: docker compose -p $DOCKER_PROJECT down  ${DIM}(in-place TTY progress)${RESET}"
    docker compose --progress auto -p "$DOCKER_PROJECT" down || true
    tui_ok "infra: stopped"
}

# Ensure the texera_db schema exists in the postgres container. The compose
# file mounts sql/*.sql to /docker-entrypoint-initdb.d, but Postgres only
# runs those on first init (empty data dir). If the volume was carried over
# from an older texera version (e.g. before the `feedback` table was added)
# the schema will be missing relations that current code references, the
# jOOQ codegen produces an incomplete Tables.java, and sbt compile fails on
# `not found: value FEEDBACK`. Probe for a canonical table and re-run
# texera_ddl.sql if it's absent.
infra_ensure_db_schema() {
    local pg="texera-postgres"
    # Wait briefly for postgres to be ready — `up -d` returned but the
    # container may still be running its own init sequence.
    local i=0
    while (( i < 30 )); do
        if docker exec "$pg" pg_isready -U texera -d texera_db -q 2>/dev/null; then
            break
        fi
        sleep 1
        i=$((i+1))
    done
    if (( i >= 30 )); then
        tui_warn "postgres: not ready after 30s -- skipping schema check"
        return 0
    fi
    # `feedback` is one of the newer tables; if it exists we assume the
    # whole schema is current. (texera_ddl.sql is idempotent with
    # CREATE TABLE IF NOT EXISTS, so re-applying it is safe even if some
    # tables already exist, but skipping the copy + exec is faster.)
    local has_feedback=""
    has_feedback=$(docker exec "$pg" psql -U texera -d texera_db -tAc \
        "SELECT 1 FROM pg_tables WHERE schemaname='texera_db' AND tablename='feedback'" \
        2>/dev/null || true)
    if [[ "$has_feedback" == "1" ]]; then
        tui_skip "postgres: schema already current"
        return 0
    fi
    tui_step "postgres: applying sql/texera_ddl.sql (one-time bootstrap)"
    local ddl="$REPO_ROOT/sql/texera_ddl.sql"
    if [[ ! -f "$ddl" ]]; then
        tui_warn "postgres: $ddl not found -- skipping (jOOQ codegen may fail)"
        return 0
    fi
    docker cp "$ddl" "$pg":/tmp/texera_ddl.sql >/dev/null
    if docker exec -u postgres "$pg" psql -U texera -f /tmp/texera_ddl.sql >/dev/null 2>&1; then
        tui_ok "postgres: schema bootstrapped"
    else
        tui_warn "postgres: ddl exec returned non-zero (check container logs)"
    fi
}

# Build precondition: ensure ONLY the postgres container is up + schema
# bootstrapped. common/dao's jooqGenerate sourceGenerator connects to
# postgres via JDBC at sbt-compile time; if postgres isn't reachable the
# catch block in common/dao/build.sbt logs "Continuing compilation with
# existing generated files..." but the generated dir is not git-tracked,
# so fresh checkouts have nothing to fall back on and the downstream
# Scala compile fails on missing Tables/Keys/etc. (#6007)
#
# Used by cmd_auto, which only wants to touch what its scan said is
# dirty. cmd_up uses the heavier infra_up + infra_ensure_db_schema pair
# instead so minio/lakefs/litellm warm up in parallel with the build.
ensure_postgres_for_build() {
    if [[ "$(docker_svc_state postgres)" != "running" ]]; then
        tui_step "postgres: starting (required for jOOQ codegen at build time)"
        local files=($(docker_compose_files))
        docker compose --progress auto -p "$DOCKER_PROJECT" --env-file "$DOCKER_ENV_FILE" "${files[@]}" \
            up -d postgres >/dev/null 2>&1 || true
    fi
    infra_ensure_db_schema
}

svc_running_pid() {
    listen_pid_for_port "$(amap_get SVC_PORT "$1")"
}

# Compact-format a duration in seconds:
#   "—" for <0, "12s", "5m 23s", "2h 14m", "3d 4h"
_format_uptime() {
    local s="${1:-0}"
    if (( s < 0 ));     then printf "—"; return; fi
    if (( s < 60 ));    then printf "%ds" "$s"; return; fi
    if (( s < 3600 ));  then printf "%dm %ds" $((s/60)) $((s%60)); return; fi
    if (( s < 86400 )); then printf "%dh %dm" $((s/3600)) $(((s%3600)/60)); return; fi
    printf "%dd %dh" $((s/86400)) $(((s%86400)/3600))
}

# Translate ps -o etime output (`[[DD-]hh:]mm:ss`) to whole seconds.
_etime_to_seconds() {
    local et="$1"
    # bash quirk: in `local a=$x b=$a`, the right-hand `$a` resolves
    # against the *outer* scope (before this `local` declares either).
    # Putting `rest` on its own line ensures it sees the fresh `et`.
    local rest="$et" days=0 h=0 m=0 s=0
    if [[ "$rest" == *-* ]]; then
        days="${rest%%-*}"
        rest="${rest#*-}"
    fi
    # `IFS=: read -ra` reliably splits on `:` without the local-IFS-vs-
    # array-literal trap.
    local parts=()
    IFS=: read -ra parts <<< "$rest"
    case "${#parts[@]}" in
        3) h=${parts[0]}; m=${parts[1]}; s=${parts[2]} ;;
        2) m=${parts[0]}; s=${parts[1]} ;;
        *) s=${parts[0]} ;;
    esac
    # Force base-10 so "08" / "09" don't blow up under bash's octal default.
    printf '%d' "$((10#${days:-0}*86400 + 10#${h:-0}*3600 + 10#${m:-0}*60 + 10#${s:-0}))"
}

# Translate a number of bytes to a 4-char-ish human string ("85M", "1.2G").
_format_bytes() {
    local b="${1:-0}"
    if (( b < 1024 ));         then printf "%dB"   "$b"; return; fi
    if (( b < 1048576 ));      then printf "%dK"   $((b/1024)); return; fi
    if (( b < 1073741824 ));   then printf "%dM"   $((b/1048576)); return; fi
    # GB → 1 decimal place
    local g_int=$((b / 1073741824)) g_frac=$(( (b % 1073741824) * 10 / 1073741824 ))
    printf "%d.%dG" "$g_int" "$g_frac"
}

# Compact "8.2%" formatter from a string like "0.50" or "8.2".
_format_pct() {
    local p="${1:-0}"
    # Strip optional trailing %, normalise weird inputs
    p="${p%%%}"
    [[ -z "$p" || "$p" == "-" ]] && { printf "—"; return; }
    # Round to 1 decimal place using awk to avoid bash's lack of float math
    printf '%s%%' "$(awk -v v="$p" 'BEGIN { printf "%.1f", v }' 2>/dev/null)"
}

# Container name for a docker-typed service. Most use `texera-<svc>`, but
# `litellm` ships unprefixed in upstream's compose.
_docker_container_for() {
    case "$1" in
        litellm) printf "litellm" ;;
        *)       printf "texera-%s" "$1" ;;
    esac
}

# Uptime for a service. Native: `ps -o etime` (POSIX, works on mac+linux).
# Docker: parse the container's ISO-8601 .State.StartedAt against `date -u`.
# Echoes "—" when not running.
svc_uptime() {
    local svc="$1"
    if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
        local container="" since=""
        container=$(_docker_container_for "$svc")
        since=$(docker inspect -f '{{.State.StartedAt}}' "$container" 2>/dev/null) || { printf "—"; return; }
        [[ -z "$since" || "$since" == "0001-01-01T00:00:00Z" ]] && { printf "—"; return; }
        local now=0 started=0
        now=$(date -u +%s 2>/dev/null) || { printf "—"; return; }
        # macOS `date -j` parses ISO without fractional seconds — trim them.
        started=$(date -u -j -f "%Y-%m-%dT%H:%M:%S" "${since%%.*}" +%s 2>/dev/null) || {
            # GNU date fallback
            started=$(date -u -d "${since%%.*}" +%s 2>/dev/null) || { printf "—"; return; }
        }
        local elapsed=$((now - started))
        (( elapsed < 0 )) && elapsed=0
        _format_uptime "$elapsed"
        return
    fi
    local pid=""
    pid=$(svc_running_pid "$svc") || true
    [[ -z "$pid" ]] && { printf "—"; return; }
    local et=""
    et=$(ps -p "$pid" -o etime= 2>/dev/null | tr -d ' ')
    [[ -z "$et" ]] && { printf "—"; return; }
    _format_uptime "$(_etime_to_seconds "$et")"
}

# CPU% + RSS for a running native process. Returns "cpu%|rss_bytes".
# Cheap enough (<5ms) that we can call it per service per tick.
svc_proc_cpu_mem() {
    local pid=""
    pid=$(svc_running_pid "$1") || true
    [[ -z "$pid" ]] && { printf "—|—"; return; }
    local cpu="" rss=""
    # macOS ps emits RSS in KB. Linux's `ps -o rss=` is also KB.
    read -r cpu rss <<< "$(ps -p "$pid" -o pcpu=,rss= 2>/dev/null | tr -s ' ')"
    [[ -z "$cpu" || -z "$rss" ]] && { printf "—|—"; return; }
    printf "%s|%s" "$(_format_pct "$cpu")" "$(_format_bytes $((rss * 1024)))"
}

# Bulk docker stats — one slow `docker stats --no-stream` call returns CPU
# + memory for every running container. Callers can grep their service out
# of the result instead of paying the 1-2 second cost N times. Format:
#   <container>|<cpu%>|<used-bytes>
_docker_stats_snapshot() {
    docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}' 2>/dev/null \
        | while IFS='|' read -r name cpu memu; do
            # MemUsage is like "85.2MiB / 1.95GiB" — keep only the first
            # half and translate IEC units to bytes.
            local used="${memu%% *}"   # "85.2MiB"
            local val="${used%[A-Za-z]*}"
            local unit="${used##*[0-9.]}"
            local bytes=0
            case "$unit" in
                KB|KiB) bytes=$(awk -v v="$val" 'BEGIN { printf "%d", v * 1024 }') ;;
                MB|MiB) bytes=$(awk -v v="$val" 'BEGIN { printf "%d", v * 1048576 }') ;;
                GB|GiB) bytes=$(awk -v v="$val" 'BEGIN { printf "%d", v * 1073741824 }') ;;
                *)      bytes=$(awk -v v="$val" 'BEGIN { printf "%d", v }') ;;
            esac
            printf "%s|%s|%s\n" "$name" "$(_format_pct "${cpu%%%}")" "$(_format_bytes "$bytes")"
        done
}

svc_artifact_mtime() {
    local svc="$1" type=""
    type=$(amap_get SVC_TYPE "$svc")
    case "$type" in
        jvm)
            local launcher="$(amap_get SVC_CWD "$svc")/$(amap_get SVC_LAUNCHER "$svc")"
            launcher="${launcher#./}"
            local jar_dir=""
            jar_dir="$(dirname "$(dirname "$launcher")")/lib"
            if [[ -d "$jar_dir" ]]; then
                # `shopt -s nullglob` localised in a subshell so an
                # unmatched glob yields no words instead of the literal
                # pattern. Capture into a string then split into an array.
                local globbed=""
                globbed=$(shopt -s nullglob; \
                    printf '%s\n' "$jar_dir"/org.apache.texera."${svc}"-*.jar)
                local main_jars=()
                while IFS= read -r _f; do
                    [[ -n "$_f" ]] && main_jars+=("$_f")
                done <<< "$globbed"
                if [[ ${#main_jars[@]} -eq 0 && ( "$svc" == "texera-web" || "$svc" == "computing-unit-master" ) ]]; then
                    globbed=$(shopt -s nullglob; \
                        printf '%s\n' "$jar_dir"/org.apache.texera.amber-*.jar)
                    while IFS= read -r _f; do
                        [[ -n "$_f" ]] && main_jars+=("$_f")
                    done <<< "$globbed"
                fi
                if [[ ${#main_jars[@]} -gt 0 ]]; then
                    stat -f "%Sm" -t "%Y-%m-%d %H:%M" "${main_jars[0]}"
                    return
                fi
            fi
            echo "—"
            ;;
        bun)    stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$(amap_get SVC_CWD "$svc")/bun.lock" 2>/dev/null || echo "—" ;;
        yarn)   stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$(amap_get SVC_CWD "$svc")/yarn.lock" 2>/dev/null || echo "—" ;;
        docker) echo "—" ;;
    esac
}

is_skipped() {
    [[ ",${SKIP_LIST:-}," == *",$1,"* ]]
}

wait_for_port() {
    local port="$1" timeout="${2:-90}" i=0
    while (( i < timeout )); do
        [[ -n "$(listen_pid_for_port "$port")" ]] && return 0
        sleep 1
        i=$((i+1))
    done
    return 1
}

# Write a transitional phase for $svc so the TUI dashboard can render it
# in the STATE column with animated dots. Cleared by phase_clear or after
# 90s (TUI side stale check).
phase_set() {
    local svc="$1" phase="$2"
    printf '%s\t%s\n' "$phase" "$(date +%s)" > "$PHASE_DIR/$svc" 2>/dev/null || true
}
phase_clear() {
    local svc="$1"
    rm -f "$PHASE_DIR/$svc" 2>/dev/null || true
}

stop_one() {
    local svc="$1"
    if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
        phase_set "$svc" stopping
        tui_step "$svc: docker compose stop $svc"
        docker compose -p "$DOCKER_PROJECT" stop "$svc" >/dev/null 2>&1 || true
        phase_clear "$svc"
        tui_ok "$svc: stopped"
        return
    fi
    local pid=""
    pid=$(svc_running_pid "$svc")
    if [[ -z "$pid" ]]; then
        tui_skip "$svc: already stopped"
        return 0
    fi
    phase_set "$svc" stopping
    tui_step "$svc: stopping PID $pid"
    kill "$pid" 2>/dev/null || true
    local i=0
    while (( i < 30 )) && kill -0 "$pid" 2>/dev/null; do
        sleep 0.5
        i=$((i+1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        tui_warn "$svc: SIGKILL $pid"
        kill -9 "$pid" 2>/dev/null || true
    fi
    phase_clear "$svc"
    tui_ok "$svc: stopped"
}

start_one() {
    local svc="$1"
    local type=""
    type=$(amap_get SVC_TYPE "$svc")
    # JVMs and docker services need STORAGE_S3_ENDPOINT exported before
    # launch (JVM clients dial MinIO; lakekeeper bakes the URL into the
    # warehouse storage profile). yarn/bun watch services don't.
    if [[ "$type" == "jvm" || "$type" == "docker" ]]; then
        _require_host_lan_ip
    fi
    if [[ "$type" == "docker" ]]; then
        local dstate=""
        dstate=$(docker_svc_state "$svc")
        if [[ "$dstate" == "running" ]]; then
            tui_ok "$svc: already running"
            return 0
        fi
        phase_set "$svc" starting
        tui_step "$svc: docker compose up -d $svc"
        local files=()
        while read -r f; do files+=("$f"); done < <(docker_compose_files | tr ' ' '\n')
        docker compose --progress auto -p "$DOCKER_PROJECT" --env-file "$DOCKER_ENV_FILE" \
            "${files[@]}" up -d "$svc" >/dev/null 2>&1
        # Don't clear the phase yet — docker `up -d` returns before the
        # container is healthy. The TUI's docker_state poller will flip
        # the row to "running" once the container reports healthy; the
        # stale-after-90s rule covers cleanup if that never happens.
        tui_ok "$svc: started"
        return
    fi
    if [[ -n "$(svc_running_pid "$svc")" ]]; then
        tui_ok "$svc: already running ${DIM}(PID $(svc_running_pid "$svc"))${RESET}"
        return 0
    fi
    local cwd="" log="$LOG_DIR/$svc.log"
    cwd=$(amap_get SVC_CWD "$svc")
    phase_set "$svc" starting
    tui_step "$svc: starting ${DIM}(log: $log)${RESET}"
    case "$type" in
        jvm)
            local launcher=""
            launcher=$(amap_get SVC_LAUNCHER "$svc")
            if [[ ! -x "$cwd/$launcher" ]]; then
                phase_clear "$svc"
                tui_err "$svc: launcher missing at $cwd/$launcher -- run \`bin/local-dev.sh up\` to build first"
                return 1
            fi
            ( cd "$cwd" && nohup "./$launcher" >"$log" 2>&1 </dev/null & )
            ;;
        bun)
            if ! command -v bun >/dev/null 2>&1; then
                phase_clear "$svc"
                tui_err "$svc: \`bun\` not found on PATH"
                _diagnose_node
                _install_hint bun
                return 1
            fi
            ( cd "$cwd" && nohup bun run dev >"$log" 2>&1 </dev/null & )
            ;;
        yarn)
            if ! command -v yarn >/dev/null 2>&1; then
                phase_clear "$svc"
                tui_err "$svc: \`yarn\` not found on PATH"
                _diagnose_node
                if ! command -v node >/dev/null 2>&1; then
                    _install_hint node
                else
                    _install_hint yarn
                fi
                return 1
            fi
            ( cd "$cwd" && nohup yarn start >"$log" 2>&1 </dev/null & )
            ;;
    esac
}

build_one_jvm() {
    local svc="$1" proj=""
    proj=$(amap_get SVC_SBT "$svc")
    local log="$LOG_DIR/sbt-${svc}.log"
    # Empty SVC_SBT means this service rides another service's dist (e.g.
    # computing-unit-master shares amber's). Nothing to build directly — the
    # launcher is produced when its sibling builds. Stamp `svc` so the dirty
    # indicator can clear if amber/src actually matches.
    if [[ -z "$proj" ]]; then
        tui_skip "$svc: no own sbt project (built with its sibling)"
        svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc" 2>/dev/null || true
        return 0
    fi
    phase_set "$svc" building
    if tui_run_with_spinner "$log" "sbt $proj/dist  ${DIM}(log: $log)${RESET}" \
        sbt -no-colors "$proj/dist"; then
        local zip_glob="" unzip_dest=""
        zip_glob=$(amap_get SVC_ZIP_GLOB "$svc")
        unzip_dest=$(amap_get SVC_UNZIP_DEST "$svc")
        tui_step "unzip ${zip_glob} → ${unzip_dest}"
        # shellcheck disable=SC2086
        unzip -oq ${zip_glob} -d "${unzip_dest}"
        # Stamp = SHA-1 of the source we just built from. Clears the `*` and
        # lets us tell content-vs-mtime apart on the next dirty check.
        svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
        tui_ok "$svc: build done"
        # Don't clear the phase yet — the caller (cmd_update_one) will
        # transition us through stop_one/start_one which overwrite it.
        # If something else is the caller, the TUI's "phase cleared once
        # poller sees running" rule covers us.
    else
        phase_clear "$svc"
        tui_err "$svc: sbt $proj/dist FAILED  ${DIM}(tail -f $log)${RESET}"
        return 1
    fi
}

# True if ANY JVM service's source changed (content-hash) since its last build.
# This is what `up`/`build` use to decide whether to skip the sbt step. It
# is the same check the dashboard's SRC `*` indicator uses, just OR'd across
# all JVM services.
any_jvm_src_changed() {
    local svc=""
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
        if svc_src_changed "$svc" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

needs_yarn_install() {
    [[ ! -f frontend/node_modules/.yarn-state.yml ]] && return 0
    [[ frontend/yarn.lock -nt frontend/node_modules/.yarn-state.yml ]] && return 0
    [[ frontend/package.json -nt frontend/node_modules/.yarn-state.yml ]] && return 0
    return 1
}

needs_bun_install() {
    [[ ! -d agent-service/node_modules ]] && return 0
    [[ agent-service/bun.lock -nt agent-service/node_modules ]] && return 0
    [[ agent-service/package.json -nt agent-service/node_modules ]] && return 0
    return 1
}

# Cache of per-service transitive src dirs, populated once at startup by
# _precompute_src_dirs(). Each entry (in the SVC_SRC_DIRS amap) is a
# newline-separated list — callers consume via `while read` (same shape
# as the old _svc_src_dirs output).

_precompute_src_dirs() {
    local svc="" entry="" dirs=""
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
        entry=$(amap_get SVC_SBT "$svc")
        # computing-unit-master rides amber's dist (no SVC_SBT of its
        # own); enter the graph at the producing project explicitly.
        if [[ -z "$entry" && "$svc" == "computing-unit-master" ]]; then
            entry="WorkflowExecutionService"
        fi
        if [[ -z "$entry" ]]; then
            echo "FATAL: service '$svc' has no SVC_SBT entry and no" >&2
            echo "       SHARED_SBT_PROJECT mapping — can't compute its" >&2
            echo "       transitive source-dir closure." >&2
            exit 1
        fi
        if ! amap_has SBT_PATH "$entry"; then
            echo "FATAL: sbt project '$entry' (for service '$svc') is not" >&2
            echo "       in the parsed build.sbt graph. Either build.sbt" >&2
            echo "       parsing failed or the project was renamed/removed." >&2
            exit 1
        fi
        dirs="$(_sbt_transitive_src_dirs "$entry")"
        if [[ -z "$dirs" ]]; then
            echo "FATAL: transitive source-dir closure for '$svc' is empty." >&2
            exit 1
        fi
        amap_set SVC_SRC_DIRS "$svc" "$dirs"
    done
}

# Look up the pre-populated cache.
_svc_src_dirs() {
    local svc="$1"
    local cached=""
    cached=$(amap_get SVC_SRC_DIRS "$svc")
    if [[ -z "$cached" ]]; then
        echo "FATAL: _svc_src_dirs: '$svc' missing from SVC_SRC_DIRS" >&2
        echo "       (did _precompute_src_dirs run?)" >&2
        exit 1
    fi
    printf '%s\n' "$cached"
}

# Compute a SHA-1 over the content of every .scala/.java/.proto file that
# matters for this service. ~100 ms; called only on the slow path of
# svc_src_changed and from the post-build stamp write.
svc_source_hash() {
    local svc="$1"
    local dirs=()
    local d=""
    while IFS= read -r d; do
        [[ -n "$d" ]] && dirs+=("$d")
    done < <(_svc_src_dirs "$svc")
    find "${dirs[@]}" \
        \( -name "*.scala" -o -name "*.java" -o -name "*.proto" \) \
        -type f -print0 2>/dev/null \
        | sort -z \
        | xargs -0 cat 2>/dev/null \
        | shasum -a 1 \
        | awk '{print $1}'
}

# Per-service dirty check (the SRC * indicator). Two-stage:
#   Fast path  (~22 ms): is any tracked source newer than the stamp file's
#                        mtime? If not, definitely clean.
#   Slow path (~100 ms): compute current source hash and compare to the hash
#                        we stored at last build time. If they match, the
#                        mtime moved without content moving (typical for git
#                        checkout) — refresh the stamp mtime so we skip the
#                        slow path next tick. If they differ, dirty.
svc_src_changed() {
    local svc="$1"
    case "$(amap_get SVC_TYPE "$svc")" in
        jvm)
            local stamp="$BUILD_STAMP_DIR/$svc"
            # Lazy seed: if we have a jar but no stamp, assume the jar matches
            # current sources and seed with the hash. First REPL after a fresh
            # checkout pays this once (~100 ms) and is clean afterwards.
            if [[ ! -s "$stamp" ]]; then
                local jar=""
                if [[ "$svc" == "texera-web" || "$svc" == "computing-unit-master" ]]; then
                    jar="amber/target/amber-${TEXERA_VERSION}/lib/org.apache.texera.amber-${TEXERA_VERSION}.jar"
                else
                    jar="target/${svc}-${TEXERA_VERSION}/lib/org.apache.texera.${svc}-${TEXERA_VERSION}.jar"
                fi
                [[ ! -f "$jar" ]] && return 0   # no jar, definitely dirty
                svc_source_hash "$svc" > "$stamp"
                return 1
            fi

            # Fast path: any tracked source mtime newer than stamp's mtime?
            local dirs=() d=""
            while IFS= read -r d; do
                [[ -n "$d" ]] && dirs+=("$d")
            done < <(_svc_src_dirs "$svc")
            local newer=""
            newer=$(find "${dirs[@]}" \
                \( -name "*.scala" -o -name "*.java" -o -name "*.proto" \) \
                -newer "$stamp" -type f -print 2>/dev/null | head -1)
            if [[ -z "$newer" ]]; then
                return 1   # nothing changed since last stamp → clean
            fi

            # Slow path: did the content actually change, or just mtimes?
            local current_hash="" stored_hash=""
            current_hash=$(svc_source_hash "$svc")
            stored_hash=$(cat "$stamp" 2>/dev/null)
            if [[ "$current_hash" == "$stored_hash" ]]; then
                # Same content, just newer mtimes (git checkout, touch, etc.).
                # Refresh stamp mtime to skip the slow path next tick.
                touch "$stamp"
                return 1
            fi
            return 0   # content really changed → dirty
            ;;
        yarn)   needs_yarn_install ;;
        bun)    needs_bun_install ;;
        docker) return 1 ;;
    esac
}

build_all() {
    BUILD_DID_RUN=false
    local log="$LOG_DIR/sbt-dist.log"

    # Build a whitelist of sbt project/dist targets from the non-skipped JVM
    # services. Empty ⇒ nothing to build. computing-unit-master has no own
    # sbt project (rides amber's dist); if it's kept but texera-web is
    # skipped we force WorkflowExecutionService/dist back in.
    local -a sbt_task=()
    local svc="" proj=""
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
        is_skipped "$svc" && continue
        proj=$(amap_get SVC_SBT "$svc")
        [[ -n "$proj" ]] && sbt_task+=("${proj}/dist")
    done
    if ! is_skipped computing-unit-master && is_skipped texera-web; then
        sbt_task+=("WorkflowExecutionService/dist")
    fi
    if (( ${#sbt_task[@]} == 0 )); then
        tui_skip "sbt dist: skipped (no JVM services selected)"
        return 0
    fi

    # CLI-only build knobs applied to the local-dev entrypoint (build.sbt
    # untouched): skip scaladoc (biggest single win on dist), pipeline
    # signature-then-body compile across projects, raise heap + G1GC.
    local -a sbt_opts=(
        -no-colors
        -J-Xmx4g
        -J-XX:+UseG1GC
        -Dsbt.pipelining=true
        'set every (Compile / doc / sources) := Seq.empty'
        'set every (Compile / packageDoc / publishArtifact) := false'
    )

    if [[ "${FRESH:-false}" == "true" ]]; then
        if tui_run_with_spinner "$log" "sbt clean dist  ${DIM}(log: $log)${RESET}" \
            sbt "${sbt_opts[@]}" clean "${sbt_task[@]}"; then
            tui_ok "sbt: clean dist done"
            BUILD_DID_RUN=true
        else
            tui_err "sbt: clean dist FAILED  ${DIM}(tail -f $log)${RESET}"
            return 1
        fi
    elif [[ "${BUILD:-auto}" == "auto" ]] && ! any_jvm_src_changed; then
        tui_skip "sbt dist: skipped (no source changes since last build)"
        return 0
    else
        if tui_run_with_spinner "$log" "sbt dist  ${DIM}(log: $log)${RESET}" \
            sbt "${sbt_opts[@]}" "${sbt_task[@]}"; then
            tui_ok "sbt: dist done"
            BUILD_DID_RUN=true
        else
            tui_err "sbt: dist FAILED  ${DIM}(tail -f $log)${RESET}"
            return 1
        fi
    fi
    # Stop any running JVMs BEFORE unzip — overwriting jars under a live JVM
    # corrupts its lazy class loads and the service silently dies later.
    # --skip'd services are left alone; the user asked us not to touch them.
    if [[ "$BUILD_DID_RUN" == "true" ]]; then
        local pid=""
        for svc in "${SERVICES[@]}"; do
            [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
            is_skipped "$svc" && continue
            pid=$(svc_running_pid "$svc")
            [[ -z "$pid" ]] && continue
            tui_step "$svc: pre-bouncing PID $pid (jars about to change)"
            kill "$pid" 2>/dev/null || true
        done
        # Wait briefly for them to actually exit
        local waited=0
        while (( waited < 10 )); do
            local still_up=0
            for svc in "${SERVICES[@]}"; do
                [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
                is_skipped "$svc" && continue
                [[ -n "$(svc_running_pid "$svc")" ]] && still_up=$((still_up+1))
            done
            (( still_up == 0 )) && break
            sleep 0.5
            waited=$((waited+1))
        done
    fi
    tui_step "unzipping dist artifacts"
    local zip_glob="" unzip_dest=""
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "jvm" ]] || continue
        is_skipped "$svc" && continue
        zip_glob=$(amap_get SVC_ZIP_GLOB "$svc")
        unzip_dest=$(amap_get SVC_UNZIP_DEST "$svc")
        # Sibling services (empty ZIP_GLOB) share another service's dist —
        # just stamp them as clean since the unzip already happened for the
        # twin holding the build.
        if [[ -z "$zip_glob" ]]; then
            svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc" 2>/dev/null || true
            continue
        fi
        # shellcheck disable=SC2086
        local zip_file=""
        zip_file=$(ls -t ${zip_glob} 2>/dev/null | head -1)
        if [[ -n "$zip_file" ]] && unzip -oq "$zip_file" -d "${unzip_dest}" 2>/dev/null; then
            svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
        else
            tui_warn "${zip_glob} not produced"
        fi
    done
    tui_ok "artifacts unzipped"
}

refresh_node_deps() {
    if ! is_skipped frontend; then
        if [[ "${BUILD:-auto}" == "auto" ]] && ! needs_yarn_install; then
            tui_skip "yarn install: skipped (lock unchanged)"
        else
            local log="$LOG_DIR/yarn-install.log"
            if tui_run_with_spinner "$log" "yarn install (frontend)  ${DIM}(log: $log)${RESET}" \
                bash -c "cd frontend && yarn install"; then
                tui_ok "yarn: deps refreshed"
            else
                tui_warn "yarn install failed  ${DIM}(tail -f $log)${RESET}"
            fi
        fi
    fi
    if ! is_skipped agent-service; then
        if [[ "${BUILD:-auto}" == "auto" ]] && ! needs_bun_install; then
            tui_skip "bun install: skipped (lock unchanged)"
        else
            local log="$LOG_DIR/bun-install.log"
            if tui_run_with_spinner "$log" "bun install (agent-service)  ${DIM}(log: $log)${RESET}" \
                bash -c "cd agent-service && bun install"; then
                tui_ok "bun: deps refreshed"
            else
                tui_warn "bun install failed  ${DIM}(tail -f $log)${RESET}"
            fi
        fi
    fi
}

# --------- subcommands ---------
# Machine-readable counterpart to cmd_status: one JSON object on stdout, no
# colours, no decorative table. The stable contract for agents/scripts that
# would otherwise scrape the dashboard. Exit code mirrors health: 0 iff every
# service is running, else 1.
emit_status_json() {
    local branch="" sha="" worktree=""
    IFS=$'\t' read -r branch sha < <(_git_head)
    worktree="$(basename "$REPO_ROOT")"

    local n_running=0 n_total=0 first=true svc="" type="" port="" state="" pid="" rows=""
    for svc in "${SERVICES[@]}"; do
        n_total=$((n_total+1))
        type=$(amap_get SVC_TYPE "$svc")
        port=$(amap_get SVC_PORT "$svc")
        pid="null"
        if [[ "$type" == "docker" ]]; then
            state=$(docker_svc_state "$svc")
            case "$state" in running|exited) n_running=$((n_running+1)) ;; esac
        else
            local p=""
            p=$(svc_running_pid "$svc")
            if [[ -n "$p" ]]; then
                state="running"; pid="$p"; n_running=$((n_running+1))
            else
                state="stopped"
            fi
        fi
        $first || rows+=","
        first=false
        rows+=$(printf '{"service":"%s","port":%s,"type":"%s","pid":%s,"state":"%s"}' \
            "$svc" "$port" "$type" "$pid" "$state")
    done
    printf '{"branch":"%s","sha":"%s","worktree":"%s","source":"%s","running":%d,"total":%d,"services":[%s]}\n' \
        "$branch" "$sha" "$worktree" "$REPO_ROOT" "$n_running" "$n_total" "$rows"
    (( n_running == n_total ))
}

cmd_status() {
    case "${1:-}" in
        --json) emit_status_json; return $? ;;
        "")     ;;
        *)      tui_err "unknown flag: $1" >&2; exit 2 ;;
    esac
    local branch="" sha="" wt=""
    IFS=$'\t' read -r branch sha < <(_git_head)
    [[ "$REPO_ROOT" != "$SELF_ROOT" ]] && wt="  ·  worktree: $(basename "$REPO_ROOT")"
    tui_banner "Texera Local Dev" "branch: $branch  @  $sha$wt"

    # One docker stats call up front — paying the ~2s docker-API cost once
    # is cheaper than running it per docker service. Indexed by container
    # name → "cpu%|mem" so the per-row formatting is just a lookup.
    # `_DSTATS` is an amap (bash 3.2: no associative arrays).
    while IFS='|' read -r name cpu mem; do
        [[ -n "$name" ]] && amap_set _DSTATS "$name" "$cpu|$mem"
    done < <(_docker_stats_snapshot)

    printf "\n"
    printf "    ${BOLD}%-32s %-6s %-9s %-10s %-7s %-7s %s${RESET}\n" \
        "SERVICE" "PORT" "PID" "UPTIME" "CPU%" "MEM" "STATE"
    printf "    ${GRAY}"
    tui_hline "─" 32; printf " "
    tui_hline "─" 6;  printf " "
    tui_hline "─" 9;  printf " "
    tui_hline "─" 10; printf " "
    tui_hline "─" 7;  printf " "
    tui_hline "─" 7;  printf " "
    tui_hline "─" 12; printf "${RESET}\n"

    local n_running=0 n_total=0 frontend_up=false
    for svc in "${SERVICES[@]}"; do
        n_total=$((n_total+1))
        local pid="—" state="stopped" uptime="—" cpu="—" mem="—"
        if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
            state=$(docker_svc_state "$svc")
            if [[ "$state" == "running" ]]; then
                local container="" dstat=""
                container=$(_docker_container_for "$svc")
                dstat=$(amap_get _DSTATS "$container")
                if [[ -n "$dstat" ]]; then
                    cpu="${dstat%|*}"
                    mem="${dstat#*|}"
                fi
                uptime=$(svc_uptime "$svc")
            fi
        else
            local found_pid=""
            found_pid=$(svc_running_pid "$svc")
            if [[ -n "$found_pid" ]]; then
                state="running"; pid="$found_pid"
                uptime=$(svc_uptime "$svc")
                local cm=""
                cm=$(svc_proc_cpu_mem "$svc")
                cpu="${cm%|*}"
                mem="${cm#*|}"
            fi
        fi
        [[ "$state" == "running" ]] && n_running=$((n_running+1))
        [[ "$svc" == "frontend" && "$state" == "running" ]] && frontend_up=true

        local sym="" color=""
        sym=$(tui_state_symbol "$state")
        color=$(tui_state_color "$state")
        printf "  %s " "$sym"
        printf "${color}%-32s${RESET} %-6s ${DIM}%-9s${RESET} %-10s %-7s %-7s ${color}%s${RESET}\n" \
            "$svc" "$(amap_get SVC_PORT "$svc")" "$pid" "$uptime" "$cpu" "$mem" "$state"
    done
    printf "\n"

    local summary_color="$YELLOW"
    (( n_running == n_total )) && summary_color="$GREEN"
    (( n_running == 0 )) && summary_color="$GRAY"
    printf "  ${BOLD}Status${RESET}: ${summary_color}%d of %d services running${RESET}\n" "$n_running" "$n_total"

    printf "\n"
    printf "  ${CYAN}${SYM_LIST}${RESET}  Logs:    ${DIM}%s${RESET}\n" "$LOG_DIR/<service>.log"
    printf "  ${CYAN}${SYM_LIST}${RESET}  Docker:  ${DIM}docker compose -p %s ps${RESET}\n" "$DOCKER_PROJECT"
    if $frontend_up; then
        printf "  ${CYAN}${SYM_LIST}${RESET}  Open:    ${BOLD}${GREEN}http://localhost:4200${RESET}  ${GREEN}(frontend live)${RESET}\n"
    else
        printf "  ${CYAN}${SYM_LIST}${RESET}  Open:    ${DIM}http://localhost:4200  (frontend not running yet)${RESET}\n"
    fi

    printf "\n  ${BOLD}Common operations${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh up${RESET}            ${DIM}# bring up the whole stack (build + start)${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh down${RESET}          ${DIM}# stop every service${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh auto${RESET}          ${DIM}# rebuild + bounce only the services whose source changed${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh <svc>${RESET}         ${DIM}# rebuild that one JVM service and bounce it${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh start <svc>${RESET}   ${DIM}# start one service without rebuilding${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh stop  <svc>${RESET}   ${DIM}# stop one service${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh logs  <svc>${RESET}   ${DIM}# tail a service's log${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh -i${RESET}            ${DIM}# open the live TUI (needs Python + textual)${RESET}\n"
    printf "    ${BOLD}bin/local-dev.sh --help${RESET}        ${DIM}# full reference${RESET}\n"
    printf "\n"
}

cmd_up() {
    SKIP_LIST=""
    FRESH=false
    BUILD=auto       # auto (skip if no source change) | force | no
    JSON_OUT=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip=*)   SKIP_LIST="${1#--skip=}" ;;
            --fresh)    FRESH=true; BUILD=force ;;
            --build)    BUILD=force ;;
            --no-build) BUILD=no ;;
            --json)     JSON_OUT=true ;;
            # Deploy-target selectors are resolved at startup (they must precede
            # the build.sbt parse); accept and ignore them here.
            --worktree=*|--branch=*) ;;
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done

    # --json: the final summary on stdout must be pure JSON, so push all the
    # human progress (banner, sections, in-place health panel) to stderr and
    # keep the real stdout on fd 3 for emit_status_json. stderr is unbuffered,
    # so a non-interactive caller still sees progress live on the side stream.
    if $JSON_OUT; then exec 3>&1 1>&2; fi

    local n_skip=0
    [[ -n "$SKIP_LIST" ]] && n_skip=$(echo "$SKIP_LIST" | tr ',' '\n' | wc -l | tr -d ' ')
    local skip_label="none"
    (( n_skip > 0 )) && skip_label="$n_skip service(s)"
    tui_banner "Texera Local Dev — bringing stack up" "JDK 17 · skip=$skip_label · build=$BUILD"

    # ── Deploy target ─────────────────────────────────────────────────────
    # Mark exactly what we are about to build and run, so it is unambiguous in
    # the log which branch/worktree/commit this deployment reflects.
    local _db="" _ds=""
    IFS=$'\t' read -r _db _ds < <(_git_head)
    tui_section "Deploy target"
    if [[ "$REPO_ROOT" == "$SELF_ROOT" ]]; then
        tui_info "checkout: $(basename "$REPO_ROOT")  ${DIM}(self / canonical)${RESET}"
    else
        tui_info "worktree: $(basename "$REPO_ROOT")  ${DIM}$REPO_ROOT${RESET}"
        tui_info "tooling : $(basename "$SELF_ROOT")  ${DIM}(local-dev.sh runs from here)${RESET}"
    fi
    tui_info "branch  : $_db @ $_ds"
    _warn_tooling_drift

    # ── Pre-flight short-circuit ───────────────────────────────────────────
    # If nothing's changed AND every service is already running, just say so
    # and exit. Saves the user from scrolling through 30+ "already running"
    # lines for the common "I just want to check" case.
    if [[ "$BUILD" == "auto" && -z "$SKIP_LIST" ]]; then
        local nothing_to_build=true
        any_jvm_src_changed   && nothing_to_build=false
        needs_yarn_install    && nothing_to_build=false
        needs_bun_install     && nothing_to_build=false

        local all_running=true svc=""
        for svc in "${SERVICES[@]}"; do
            if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
                [[ "$(docker_svc_state "$svc")" == "running" ]] || { all_running=false; break; }
            else
                [[ -n "$(svc_running_pid "$svc")" ]] || { all_running=false; break; }
            fi
        done

        if $nothing_to_build && $all_running; then
            tui_section "Pre-flight"
            tui_ok "no source/lock changes since last build"
            tui_ok "all ${#SERVICES[@]} services already running"
            printf "\n  ${BOLD}${GREEN}${SYM_OK} nothing to do${RESET}  ${DIM}(use \`u --build\` to force a rebuild, or \`<svc>\` to bounce just one)${RESET}\n\n"
            $JSON_OUT && { emit_status_json >&3 || true; }
            return 0
        fi
    fi

    # ── Infra (must precede Build) ────────────────────────────────────────
    # common/dao's jooqGenerate sourceGenerator runs at sbt-compile time
    # and connects to postgres via JDBC; if postgres isn't reachable the
    # generator returns Seq.empty and the downstream Scala compile fails
    # on missing Tables/Keys/etc (the generated dir is not git-tracked).
    # Bring infra up first so postgres is ready when the build fires.
    # As a bonus minio/lakefs/litellm warm up while sbt runs. (#6007)
    local svc=""
    local has_docker_targets=false
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]] || continue
        is_skipped "$svc" && continue
        has_docker_targets=true
        break
    done
    if $has_docker_targets; then
        tui_section "Infra"
        case "$(infra_state)" in
            running)    tui_ok "infra: already running" ;;
            external:*) tui_err "infra: ports taken by non-script containers"
                        printf "  ${DIM}docker compose -p texera-dev down${RESET}\n" ;;
            *)          infra_up || true ;;
        esac
        infra_ensure_db_schema
    fi

    if [[ "$BUILD" != "no" ]]; then
        tui_section "Build"
        build_all
        refresh_node_deps
    else
        tui_section "Build"
        tui_skip "build: --no-build (using existing artifacts)"
    fi

    # ▸ Services -- native services only; docker rows were handled by the
    # Infra section above. We kick each one off silently in the background
    # and a single redrawing panel below shows progress for ALL of them.
    tui_section "Services  ${DIM}(launching)${RESET}"
    local cwd="" log="" type="" launcher=""

    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && { tui_skip "$svc: --skip"; continue; }
        type=$(amap_get SVC_TYPE "$svc")
        [[ "$type" == "docker" ]] && continue   # handled by Infra section above
        if [[ -n "$(svc_running_pid "$svc")" ]]; then
            tui_ok "$svc: already running ${DIM}(PID $(svc_running_pid "$svc"))${RESET}"
            continue
        fi
        cwd=$(amap_get SVC_CWD "$svc")
        log="$LOG_DIR/$svc.log"
        tui_step "$svc: launching → ${DIM}$log${RESET}"
        case "$type" in
            jvm)
                launcher=$(amap_get SVC_LAUNCHER "$svc")
                if [[ ! -x "$cwd/$launcher" ]]; then
                    tui_err "$svc: launcher missing at $cwd/$launcher"
                    continue
                fi
                ( cd "$cwd" && nohup "./$launcher" >"$log" 2>&1 </dev/null & ) ;;
            bun)  ( cd "$cwd" && nohup bun run dev >"$log" 2>&1 </dev/null & ) ;;
            yarn) ( cd "$cwd" && nohup yarn start  >"$log" 2>&1 </dev/null & ) ;;
        esac
    done

    tui_section "Health  ${DIM}(refreshing in place)${RESET}"
    local ec=0
    tui_wait_panel || ec=$?

    printf "\n"
    local ok=0 total=0 failed=0
    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && continue
        total=$((total+1))
        if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
            case "$(docker_svc_state "$svc")" in
                running|exited) ok=$((ok+1)) ;;
                *)              failed=$((failed+1)) ;;
            esac
        else
            [[ -n "$(svc_running_pid "$svc")" ]] && ok=$((ok+1)) || failed=$((failed+1))
        fi
    done
    if (( failed == 0 )); then
        printf "  ${BOLD}${GREEN}${SYM_OK} %d of %d services healthy${RESET}\n" "$ok" "$total"
    else
        printf "  ${BOLD}${YELLOW}${SYM_WARN} %d of %d services healthy${RESET}  ${RED}(%d failed)${RESET}\n" \
            "$ok" "$total" "$failed"
    fi
    printf "\n"

    if $JSON_OUT; then emit_status_json >&3 || true; else cmd_status; fi
    [[ $ec -eq 0 ]]
}

# `auto`: the minimal "make the running services match my current source" path.
# Walks every service, identifies what's actually dirty (content-hash for JVM,
# lock mtime for yarn/bun, never for docker), and only touches those:
#   - dirty JVM, currently running   → rebuild + bounce
#   - dirty JVM, currently stopped   → rebuild only (don't auto-start)
#   - dirty yarn (frontend lock)     → yarn install + bounce frontend if up
#   - dirty bun (agent-service lock) → bun install (bun --watch reloads itself)
# Clean services are left alone — no pre-bounce, no restart.
cmd_auto() {
    SKIP_LIST=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip=*) SKIP_LIST="${1#--skip=}" ;;
            # Deploy-target selectors are resolved at startup; accept here.
            --worktree=*|--branch=*) ;;
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done

    tui_banner "Texera Local Dev — auto bounce" \
        "rebuild + bounce only what changed since last build"
    if [[ "$REPO_ROOT" != "$SELF_ROOT" ]]; then
        tui_info "deploy source: worktree $(basename "$REPO_ROOT")  ${DIM}$REPO_ROOT${RESET}"
        _warn_tooling_drift
    fi

    # ── Scan ──────────────────────────────────────────────────────────────
    tui_section "Scan"
    local svc=""
    local dirty_jvms=()
    local need_yarn=false
    local need_bun=false
    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && continue
        case "$(amap_get SVC_TYPE "$svc")" in
            jvm)
                if svc_src_changed "$svc"; then
                    dirty_jvms+=("$svc")
                    tui_warn "$svc: source changed since last build"
                fi ;;
            yarn)
                if needs_yarn_install; then
                    need_yarn=true
                    tui_warn "frontend: yarn.lock newer than node_modules — needs install"
                fi ;;
            bun)
                if needs_bun_install; then
                    need_bun=true
                    tui_warn "agent-service: bun.lock newer than node_modules — needs install"
                fi ;;
        esac
    done

    if (( ${#dirty_jvms[@]} == 0 )) && ! $need_yarn && ! $need_bun; then
        tui_ok "everything up-to-date — nothing to bounce"
        printf "\n"
        return 0
    fi

    # ── Build ─────────────────────────────────────────────────────────────
    # One `sbt dist` covers every dirty JVM in a single sbt invocation; sbt's
    # own incremental compiler only recompiles the subprojects that need it,
    # and we only unzip + bounce the dirty ones below — clean services don't
    # get pre-bounced just because the build ran.
    if (( ${#dirty_jvms[@]} > 0 )); then
        tui_section "Build  ${DIM}(${#dirty_jvms[@]} JVM service(s) dirty)${RESET}"
        # sbt build precondition: common/dao's jooqGenerate connects to
        # postgres at compile time; a `down` + `auto` sequence on a
        # fresh-ish checkout otherwise fails before any service launches.
        # See #6007.
        ensure_postgres_for_build
        # Mark each dirty service as "building" so the TUI shows the
        # animation across the whole sbt window (~30s+). Without this the
        # dashboard stays on the prior STATE during the slow build.
        local _s=""
        for _s in "${dirty_jvms[@]}"; do
            phase_set "$_s" building
        done
        local log="$LOG_DIR/sbt-dist.log"
        if ! tui_run_with_spinner "$log" "sbt dist  ${DIM}(log: $log)${RESET}" \
                sbt -no-colors dist; then
            for _s in "${dirty_jvms[@]}"; do
                phase_clear "$_s"
            done
            tui_err "sbt dist failed  ${DIM}(tail -f $log)${RESET}"
            return 1
        fi
        tui_ok "sbt: dist done"
    fi

    # ── Bounce dirty JVMs ────────────────────────────────────────────────
    # Two passes so siblings can coexist:
    #   1) stop + (maybe-)unzip + stamp every dirty service
    #   2) start the ones that were running when we entered
    #
    # Pass 1 needs to finish before pass 2 because computing-unit-master
    # shares amber's dist with texera-web: cu-master has empty SVC_ZIP_GLOB
    # / SVC_UNZIP_DEST, so its "rebuild" is just waiting for texera-web's
    # iteration to unzip amber. If we kept the previous single-pass loop
    # we'd `continue` past start_one whenever ZIP_GLOB was empty and the
    # sibling never came back up.
    local n_bounced=0 n_rebuilt=0
    # `_was_running` is an amap (bash 3.2: no associative arrays). Keyed by
    # svc → the PID we observed at the start of pass 1, "" if not running.
    # Cleared with `unset` at the top so a re-invocation (e.g. interactive
    # REPL would have, if we still had one) starts fresh.
    local _wr_var=""
    for _wr_var in $(compgen -v _amap___was_running__ 2>/dev/null); do
        unset "$_wr_var"
    done
    if (( ${#dirty_jvms[@]} > 0 )); then
        tui_section "Bounce"
        # Pass 1: stop running pids; unzip own dist if we have one.
        for svc in "${dirty_jvms[@]}"; do
            local pid=""
            pid=$(svc_running_pid "$svc")
            amap_set _was_running "$svc" "$pid"
            if [[ -n "$pid" ]]; then
                # Flip the dashboard from `building` to `stopping` while we
                # SIGTERM/SIGKILL the JVM, then back to `building` for the
                # unzip step below. Without these the user only sees one
                # state during the whole bounce.
                phase_set "$svc" stopping
                tui_step "$svc: stopping PID $pid before unzip"
                kill "$pid" 2>/dev/null || true
                local i=0
                while (( i < 30 )) && kill -0 "$pid" 2>/dev/null; do
                    sleep 0.5
                    i=$((i+1))
                done
                kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
            fi
            local zip_glob="" unzip_dest=""
            zip_glob=$(amap_get SVC_ZIP_GLOB "$svc")
            unzip_dest=$(amap_get SVC_UNZIP_DEST "$svc")
            if [[ -z "$zip_glob" ]]; then
                # Sibling service (e.g. computing-unit-master): no own dist
                # to unzip — its launcher comes from the twin's unzip later
                # in this pass. Just stamp it clean.
                phase_set "$svc" building
                svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc" 2>/dev/null || true
                n_rebuilt=$((n_rebuilt+1))
                continue
            fi
            phase_set "$svc" building
            # shellcheck disable=SC2086
            local zip_file=""
            zip_file=$(ls -t ${zip_glob} 2>/dev/null | head -1)
            if [[ -n "$zip_file" ]] && unzip -oq "$zip_file" -d "${unzip_dest}" 2>/dev/null; then
                svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
                n_rebuilt=$((n_rebuilt+1))
            else
                tui_warn "$svc: ${zip_glob} not produced — skipping"
            fi
        done
        # amber's two siblings (texera-web + computing-unit-master) are
        # *intended* to run together — they share the dist and they're
        # both required for a working stack. If either was running before
        # the auto pass, treat the whole group as "should be running" so
        # the other doesn't end up silently dead just because it had
        # already crashed / been left stopped from an earlier session.
        local AMBER_SIBLINGS=(texera-web computing-unit-master)
        local amber_group_active=false
        local s=""
        for s in "${AMBER_SIBLINGS[@]}"; do
            [[ -n "$(amap_get _was_running "$s")" ]] && amber_group_active=true
        done

        # Pass 2: start the ones that had been running, plus any amber
        # sibling that is dirty when the group is "active". By now every
        # sibling's launcher is on disk (the twin's unzip in pass 1
        # populated `amber/target/amber-<VERSION>/bin/*`).
        for svc in "${dirty_jvms[@]}"; do
            local should_start=false
            if [[ -n "$(amap_get _was_running "$svc")" ]]; then
                should_start=true
            elif $amber_group_active && { [[ "$svc" == "texera-web" ]] || [[ "$svc" == "computing-unit-master" ]]; }; then
                should_start=true
                tui_step "$svc: was stopped but its sibling is active — starting too"
            fi
            if $should_start; then
                start_one "$svc"
                n_bounced=$((n_bounced+1))
            else
                # Pass 1 left phase=building set so the dashboard animates
                # during sbt + unzip; clear it now since we won't be
                # starting (otherwise the row spins building… until the
                # 90s stale rule kicks in).
                phase_clear "$svc"
                tui_skip "$svc: was stopped — rebuilt but not started"
            fi
        done
    fi

    # ── Node deps ────────────────────────────────────────────────────────
    if $need_yarn; then
        tui_section "Frontend deps"
        local log="$LOG_DIR/yarn-install.log"
        if tui_run_with_spinner "$log" "yarn install  ${DIM}(log: $log)${RESET}" \
                bash -c "cd frontend && yarn install"; then
            tui_ok "yarn: deps refreshed"
            # ng serve doesn't pick up dependency-tree changes from a running
            # process; bounce if it was up.
            if [[ -n "$(svc_running_pid frontend)" ]]; then
                stop_one frontend
                start_one frontend
                n_bounced=$((n_bounced+1))
            else
                tui_skip "frontend: was stopped — deps refreshed but not started"
            fi
        else
            tui_err "yarn install failed  ${DIM}(tail -f $log)${RESET}"
        fi
    fi

    if $need_bun; then
        tui_section "Agent-service deps"
        local log="$LOG_DIR/bun-install.log"
        if tui_run_with_spinner "$log" "bun install  ${DIM}(log: $log)${RESET}" \
                bash -c "cd agent-service && bun install"; then
            tui_ok "bun: deps refreshed"
            # bun --watch reloads itself when node_modules changes; no manual
            # bounce needed.
            if [[ -n "$(svc_running_pid agent-service)" ]]; then
                tui_skip "agent-service: bun --watch will reload"
            else
                tui_skip "agent-service: was stopped — deps refreshed but not started"
            fi
        else
            tui_err "bun install failed  ${DIM}(tail -f $log)${RESET}"
        fi
    fi

    # ── Summary + final dashboard ────────────────────────────────────────
    printf "\n"
    printf "  ${BOLD}${GREEN}${SYM_OK} auto bounce done${RESET}: %d rebuilt, %d bounced\n\n" \
        "$n_rebuilt" "$n_bounced"
    cmd_status
}

cmd_down() {
    SKIP_LIST=""
    JSON_OUT=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip=*) SKIP_LIST="${1#--skip=}" ;;
            --json)   JSON_OUT=true ;;
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done
    # See cmd_up: human progress to stderr, JSON summary on real stdout (fd 3).
    if $JSON_OUT; then exec 3>&1 1>&2; fi
    tui_banner "Texera Local Dev — stopping stack" "skip=${SKIP_LIST:-none}"
    tui_section "Stopping (reverse order)"
    local svc=""
    # Stop native services first (reverse declaration order)
    # bash arrays are 0-indexed: last element is at N-1.
    for (( i=${#SERVICES[@]} - 1; i>=0; i-- )); do
        svc="${SERVICES[i]}"
        [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]] && continue
        if is_skipped "$svc"; then
            tui_skip "$svc: --skip"
            continue
        fi
        stop_one "$svc"
    done
    # Then one project-level docker compose down for any docker target.
    local has_docker_targets=false
    for svc in "${SERVICES[@]}"; do
        [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]] || continue
        if is_skipped "$svc"; then
            tui_skip "$svc: --skip"
            continue
        fi
        has_docker_targets=true
    done
    $has_docker_targets && infra_down
    printf "\n"
    $JSON_OUT && { emit_status_json >&3 || true; }
    return 0
}

cmd_update_one() {
    local svc="$1"
    if ! amap_has SVC_TYPE "$svc"; then
        tui_err "unknown service: ${BOLD}$svc${RESET}"
        printf "  ${DIM}Known:${RESET} ${SERVICES[*]}\n"
        exit 1
    fi
    local type=""
    type=$(amap_get SVC_TYPE "$svc")
    case "$type" in
        docker)
            tui_banner "Restarting ${svc}" "docker compose restart $svc"
            tui_step "$svc: docker compose restart $svc"
            docker compose -p "$DOCKER_PROJECT" restart "$svc" >/dev/null 2>&1 \
                && tui_ok "$svc: restarted" \
                || { tui_err "$svc: restart failed"; exit 1; }
            exit 0
            ;;
        yarn)
            tui_warn "frontend uses ng's watch -- source changes hot-reload automatically."
            printf "  ${DIM}For dep changes: kill PID ${RESET}$(svc_running_pid frontend)${DIM}; then bin/local-dev.sh up${RESET}\n"
            exit 0
            ;;
        bun)
            tui_banner "Updating ${svc}" "bun install + bounce"
            tui_section "Deps"
            ( cd "$(amap_get SVC_CWD "$svc")" && bun install )
            tui_section "Bounce"
            stop_one "$svc"
            start_one "$svc"
            ;;
        jvm)
            local _sbt_proj=""
            _sbt_proj=$(amap_get SVC_SBT "$svc")
            if [[ -n "$_sbt_proj" ]]; then
                tui_banner "Updating ${svc}" "sbt ${_sbt_proj}/dist + bounce"
            else
                tui_banner "Updating ${svc}" "bounce only (shares dist with its sibling)"
            fi
            tui_section "Build"
            build_one_jvm "$svc"
            tui_section "Bounce"
            # amber's two siblings share a dist — rebuilding one moves the
            # jar bytes underneath the other. Always bounce them together
            # so neither ends up running stale code (or silently dead).
            local sibling=""
            case "$svc" in
                texera-web)            sibling="computing-unit-master" ;;
                computing-unit-master) sibling="texera-web" ;;
            esac
            local sibling_was_running=false
            if [[ -n "$sibling" ]] && [[ -n "$(svc_running_pid "$sibling")" ]]; then
                sibling_was_running=true
                tui_step "$sibling: stopping (shares amber dist with $svc)"
                stop_one "$sibling"
            fi
            stop_one "$svc"
            start_one "$svc"
            if $sibling_was_running; then
                start_one "$sibling"
            fi
            ;;
    esac
    tui_section "Health"
    local _port=""
    _port=$(amap_get SVC_PORT "$svc")
    if wait_for_port "$_port" 60; then
        printf "  ${GREEN}${SYM_OK}${RESET}  %-32s ${DIM}:%s${RESET}\n" "$svc" "$_port"
    else
        printf "  ${RED}${SYM_ERR}${RESET}  %-32s ${DIM}:%s${RESET}  ${RED}timeout${RESET}  ${DIM}(bin/local-dev.sh logs %s)${RESET}\n" \
            "$svc" "$_port" "$svc"
        exit 1
    fi
    printf "\n"
}

cmd_logs() {
    local svc="${1:?usage: bin/local-dev.sh logs <service>}"
    if ! amap_has SVC_TYPE "$svc"; then
        echo "Unknown service: $svc" >&2
        exit 1
    fi
    if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
        exec docker compose -p "$DOCKER_PROJECT" logs -f "$svc"
    fi
    exec tail -f "$LOG_DIR/$svc.log"
}

# Render the interactive dashboard panel (banner + service table + hint + summary).
tui_render_dashboard() {
    printf "\e[2J\e[H"   # clear screen + home cursor (scrollback preserved)
    local branch="" sha="" wt=""
    IFS=$'\t' read -r branch sha < <(_git_head)
    [[ "$REPO_ROOT" != "$SELF_ROOT" ]] && wt="worktree: $(basename "$REPO_ROOT") · "
    tui_banner "Texera Local Dev — interactive" "${wt}branch: $branch @ $sha · $(date '+%H:%M:%S') · type ? for help"
    printf "\n"

    printf "    ${BOLD}%-32s %-7s %-9s %-18s %-3s %s${RESET}\n" \
        "SERVICE" "PORT" "PID" "ARTIFACT MTIME" "SRC" "STATE"
    printf "    ${GRAY}"; tui_hline "─" 32; printf " "
    tui_hline "─" 7; printf " "; tui_hline "─" 9; printf " "
    tui_hline "─" 18; printf " "; tui_hline "─" 3; printf " "
    tui_hline "─" 12; printf "${RESET}\n"

    local n_run=0 n_total=0 n_dirty=0
    local svc=""
    for svc in "${SERVICES[@]}"; do
        n_total=$((n_total+1))
        local pid="—" state="stopped" mtime="—" port_str="—" src_disp="   "
        if [[ "$(amap_get SVC_TYPE "$svc")" == "docker" ]]; then
            state=$(docker_svc_state "$svc")
            mtime="docker"
            port_str=":$(amap_get SVC_PORT "$svc")"
        else
            local found_pid=""
            found_pid=$(svc_running_pid "$svc")
            if [[ -n "$found_pid" ]]; then state="running"; pid="$found_pid"; fi
            mtime=$(svc_artifact_mtime "$svc")
            port_str=":$(amap_get SVC_PORT "$svc")"
        fi
        [[ "$state" == "running" ]] && n_run=$((n_run+1))

        if svc_src_changed "$svc" 2>/dev/null; then
            src_disp="${YELLOW}${BOLD}*${RESET}  "
            n_dirty=$((n_dirty+1))
        fi

        local sym="" color=""
        sym=$(tui_state_symbol "$state")
        color=$(tui_state_color "$state")
        printf "  %s " "$sym"
        printf "${color}%-32s${RESET} %-7s ${DIM}%-9s${RESET} %-18s %s ${color}%s${RESET}\n" \
            "$svc" "$port_str" "$pid" "$mtime" "$src_disp" "$state"
    done

    printf "\n"
    local sum_color="$YELLOW"
    (( n_run == n_total )) && sum_color="$GREEN"
    (( n_run == 0 ))       && sum_color="$GRAY"
    printf "  ${BOLD}${sum_color}%d of %d running${RESET}" "$n_run" "$n_total"
    if (( n_dirty > 0 )); then
        printf "    ${YELLOW}${BOLD}*${RESET} ${DIM}%d with source changes${RESET}" "$n_dirty"
    fi
    printf "\n\n"
    printf "  ${DIM}Commands:${RESET}  "
    printf "${BOLD}r${RESET}efresh${DIM} (or just ↩)${RESET} · "
    printf "${BOLD}u${RESET}p · ${BOLD}d${RESET}own · "
    printf "${BOLD}b${RESET}uild · "
    printf "${BOLD}<svc>${RESET}${DIM}=rebuild+bounce${RESET} · "
    printf "${BOLD}l${RESET}ogs ${DIM}<svc>${RESET} · "
    printf "${BOLD}s${RESET}top ${DIM}<svc>${RESET} · "
    printf "${BOLD}q${RESET}uit\n\n"
}

# Pure monitoring mode: redraw the dashboard every $1 seconds, no prompt.
# Ctrl-C to exit. Useful when watching a build/restart from another terminal.
cmd_watch() {
    if [[ ! -t 1 ]]; then
        tui_err "watch mode requires a TTY"
        exit 1
    fi
    local interval="${1:-2}"
    trap 'printf "\e[?25h\n${DIM}bye${RESET}\n"; exit 0' EXIT INT TERM
    printf "\e[?25l"   # hide cursor
    while true; do
        tui_render_dashboard
        printf "  ${DIM}watch: refreshing every %ss · Ctrl-C to exit${RESET}\n" "$interval"
        sleep "$interval"
    done
}

# Pause and let the user read command output before re-rendering the dashboard.

# Print the ordered list of Python interpreters we consider for launching the
# Textual TUI: an explicit override, then any active venv, then the canonical
# texera dev venv, then whatever `python3`/`python` happen to resolve to. We
# de-duplicate as we go so the diagnostic doesn't show the same path twice.
_probed_pythons() {
    local seen=""
    local cand=""
    local raw=(
        "${TEXERA_PYTHON:-}"
        "${VIRTUAL_ENV:+$VIRTUAL_ENV/bin/python}"
        "$(command -v python3 2>/dev/null)"
        "$(command -v python  2>/dev/null)"
    )
    for cand in "${raw[@]}"; do
        [[ -z "$cand" ]] && continue
        case ":$seen:" in *":$cand:"*) continue ;; esac
        seen="$seen:$cand"
        printf '%s\n' "$cand"
    done
}

# Walk `_probed_pythons` and return the first interpreter where `import
# textual` succeeds, or empty string if none.
_find_python_with_textual() {
    local cand=""
    while IFS= read -r cand; do
        [[ -x "$cand" ]] || continue
        if "$cand" -c "import textual" >/dev/null 2>&1; then
            printf '%s\n' "$cand"
            return 0
        fi
    done < <(_probed_pythons)
    return 1
}

# Show every candidate Python the picker considered, its version, and
# whether it can `import textual`. Pinpoints "I have the right python on
# PATH but textual isn't installed THERE" vs "no python found at all".
_diagnose_python() {
    printf "\n  ${BOLD}candidate interpreters:${RESET}\n"
    local any=0
    while IFS= read -r cand; do
        any=1
        if [[ ! -x "$cand" ]]; then
            printf "    ${RED}✗${RESET} %s  ${DIM}(not executable)${RESET}\n" "$cand"
            continue
        fi
        local ver=""
        ver=$("$cand" --version 2>&1 | head -1) || ver="(unreadable)"
        if "$cand" -c "import textual; print(textual.__version__)" >/dev/null 2>&1; then
            local tv=""
            tv=$("$cand" -c "import textual; print(textual.__version__)" 2>/dev/null) || tv="?"
            printf "    ${GREEN}✓${RESET} %s  ${DIM}(%s, textual %s)${RESET}\n" "$cand" "$ver" "$tv"
        else
            printf "    ${YELLOW}!${RESET} %s  ${DIM}(%s, textual MISSING)${RESET}\n" "$cand" "$ver"
        fi
    done < <(_probed_pythons)
    if (( any == 0 )); then
        printf "    ${RED}✗${RESET} ${DIM}no candidate python on PATH or in TEXERA_PYTHON / VIRTUAL_ENV${RESET}\n"
    fi
    printf "\n  ${BOLD}env:${RESET}\n"
    printf "    \$TEXERA_PYTHON = ${DIM}%s${RESET}\n" "${TEXERA_PYTHON:-(unset)}"
    printf "    \$VIRTUAL_ENV   = ${DIM}%s${RESET}\n" "${VIRTUAL_ENV:-(unset)}"
    printf "\n"
}

# Hand off to the Python + Textual TUI. Hard requirement now (no more zsh
# REPL fallback) — if we can't find a Python with `textual` installed,
# print install instructions and exit non-zero. Use the non-interactive
# `status` (or any other subcommand) when you don't have Python set up.
cmd_interactive() {
    if [[ ! -t 0 || ! -t 1 ]]; then
        tui_err "interactive mode requires a TTY"
        exit 1
    fi
    # zsh + `set -e` kills the script when a command substitution's command
    # exits non-zero (`var=$(false)` → silent abort). Suppress with || true
    # so the error path below actually gets to print the install hint.
    local py=""
    py="$(_find_python_with_textual)" || true
    if [[ -z "$py" ]]; then
        tui_err "interactive mode requires Python with the ${BOLD}textual${RESET} package"
        _diagnose_python
        _install_hint python
        exit 1
    fi
    exec "$py" "$SELF_ROOT/bin/local-dev/tui.py"
}

# --------- main ---------
# Warm the per-service source-dir cache so dirty checks below skip the
# repeated BFS over the sbt graph. ~5ms of one-time cost; saves N×BFS
# work per `cmd_up` / `cmd_auto` / status loop.
_precompute_src_dirs

case "${1:-}" in
    "")               cmd_status ;;             # default: one-shot dashboard (safe in scripts/CI)
    status)           shift; cmd_status "$@" ;; # `status [--json]`
    -i|--interactive) cmd_interactive ;;        # opt in to the live TUI
    up)               shift; cmd_up "$@" ;;
    auto)             shift; cmd_auto "$@" ;;
    down)             shift; cmd_down "$@" ;;
    start)            shift; start_one "${1:?need service name}" ;;
    stop)             shift; stop_one "${1:?need service name}" ;;
    logs)             shift; cmd_logs "${1:-}" ;;
    w|watch)          shift; cmd_watch "${1:-2}" ;;
    version)          printf "%s\n" "$TEXERA_VERSION" ;;
    -h|--help)        sed -n '18,92p' "$0" ;;
    *)                cmd_update_one "$1" ;;
esac
