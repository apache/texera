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
#   bin/local-dev.sh status                   same as no-arg invocation.
#   bin/local-dev.sh up   [--fresh|--build|--no-build] [--skip=svc1,svc2]
#                                             Default: skip build if no source/lock
#                                             changes since last build. --build forces
#                                             incremental sbt dist + yarn/bun install.
#                                             --fresh runs `sbt clean dist`. --no-build
#                                             skips the build step entirely.
#   bin/local-dev.sh down [--skip=svc1,svc2]  stop every non-skipped service.
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
# Docker infra (postgres / minio / lakefs / lakekeeper / litellm) is NOT managed
# here -- bring those up yourself before `up`. The script will warn if expected
# ports are unreachable.
#
# Logs and pid book-keeping live under: ${TEXERA_LOCAL_DEV_DIR:-/tmp/texera-local-dev}

set -euo pipefail
setopt no_nomatch   # don't error on unmatched globs (match bash behaviour)

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

STATE_DIR="${TEXERA_LOCAL_DEV_DIR:-/tmp/texera-local-dev}"
LOG_DIR="$STATE_DIR/logs"
BUILD_STAMP_DIR="$STATE_DIR/build-stamps"
mkdir -p "$LOG_DIR" "$BUILD_STAMP_DIR"

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
        print -r -- "$JAVA_HOME"; return 0
    fi
    # 2. macOS native locator (works for any vendor installed via /Library).
    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        cand=$(/usr/libexec/java_home -v 17 2>/dev/null) || cand=""
        if [[ -n "$cand" ]] && _java_is_17 "$cand"; then
            print -r -- "$cand"; return 0
        fi
    fi
    # 3. Homebrew — try `brew --prefix openjdk@17` first, then both well-
    #    known prefixes as a fallback (script may run without brew on PATH
    #    if /etc/zprofile didn't fire).
    if command -v brew >/dev/null 2>&1; then
        cand=$(brew --prefix openjdk@17 2>/dev/null) || cand=""
        [[ -n "$cand" ]] && _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
    fi
    for cand in /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17; do
        _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
    done
    # 4. Linux distro layouts. Glob first match.
    local glob=""
    for glob in \
        /usr/lib/jvm/java-17-openjdk* \
        /usr/lib/jvm/temurin-17-jdk* \
        /usr/lib/jvm/java-17-amazon-corretto* \
        /usr/lib/jvm/zulu-17* \
        /usr/lib/jvm/jdk-17* ; do
        for cand in $~glob(N); do
            _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
        done
    done
    # 5. SDKMAN (`sdk install java 17.x-...`) — pick the lex-largest 17.* dir.
    if [[ -d "$HOME/.sdkman/candidates/java" ]]; then
        for cand in "$HOME"/.sdkman/candidates/java/17.*(N); do
            _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
        done
    fi
    # 6. asdf.
    if [[ -d "$HOME/.asdf/installs/java" ]]; then
        for cand in "$HOME"/.asdf/installs/java/*17*(N); do
            _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
        done
    fi
    # 7. Whatever `java` is on PATH, IF it's 17 — covers cases like Docker
    #    images or distro-managed defaults.
    cand=$(command -v java 2>/dev/null) || cand=""
    if [[ -n "$cand" ]]; then
        cand="$(dirname "$(dirname "$cand")")"
        _java_is_17 "$cand" && { print -r -- "$cand"; return 0; }
    fi
    return 1
}

JAVA_HOME_DETECTED="$(_find_jdk17)" || {
    echo "FATAL: could not find a JDK 17 install." >&2
    echo "  tried: \$JAVA_HOME, /usr/libexec/java_home -v 17, Homebrew, Linux /usr/lib/jvm/*, SDKMAN, asdf, \$PATH" >&2
    echo "  install one of: brew install openjdk@17 · apt install openjdk-17-jdk · sdk install java 17.0.x-tem" >&2
    echo "  or set JAVA_HOME=/path/to/jdk-17 explicitly" >&2
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
        [[ -n "$ip" && "$ip" != 127.* ]] && { print -r -- "$ip"; return 0; }
    fi
    # 2. Fallback: linux `hostname -I`-equivalent walk over en*.
    for iface in en0 en1 en2 en3 en4 en5 en6 en7 en8 en9 en10; do
        ip=$(ipconfig getifaddr "$iface" 2>/dev/null)
        [[ -n "$ip" && "$ip" != 127.* ]] && { print -r -- "$ip"; return 0; }
    done
    return 1
}
HOST_LAN_IP="$(_detect_host_lan_ip)" || HOST_LAN_IP=""
if [[ -z "$HOST_LAN_IP" ]]; then
    echo "WARN: no LAN IP detected -- S3 endpoint falling back to localhost." >&2
    echo "WARN: Iceberg ops may fail because the lakekeeper container can't reach localhost:9000." >&2
    HOST_LAN_IP="localhost"
fi

export STORAGE_JDBC_URL="${STORAGE_JDBC_URL:-jdbc:postgresql://localhost:5432/texera_db?currentSchema=texera_db,public}"
export STORAGE_JDBC_USERNAME="${STORAGE_JDBC_USERNAME:-texera}"
export STORAGE_JDBC_PASSWORD="${STORAGE_JDBC_PASSWORD:-password}"
export STORAGE_S3_ENDPOINT="${STORAGE_S3_ENDPOINT:-http://$HOST_LAN_IP:9000}"
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

typeset -A SVC_TYPE SVC_PORT SVC_SBT SVC_LAUNCHER SVC_CWD SVC_HEALTH SVC_ZIP_GLOB SVC_UNZIP_DEST

# Each docker service is now its own row in the dashboard. start/stop still
# batch through infra_up/infra_down because `docker compose up -d` and
# `docker compose down` operate at the project level.
SVC_TYPE[postgres]=docker;   SVC_PORT[postgres]=5432;   SVC_CWD[postgres]="."
SVC_TYPE[minio]=docker;      SVC_PORT[minio]=9000;      SVC_CWD[minio]="."
SVC_TYPE[lakefs]=docker;     SVC_PORT[lakefs]=8000;     SVC_CWD[lakefs]="."
SVC_TYPE[lakekeeper]=docker; SVC_PORT[lakekeeper]=8181; SVC_CWD[lakekeeper]="."
SVC_TYPE[litellm]=docker;    SVC_PORT[litellm]=4000;    SVC_CWD[litellm]="."

SVC_TYPE[config-service]=jvm
SVC_PORT[config-service]=9094
SVC_SBT[config-service]=ConfigService
SVC_LAUNCHER[config-service]="target/config-service-${TEXERA_VERSION}/bin/config-service"
SVC_CWD[config-service]="."
SVC_ZIP_GLOB[config-service]="config-service/target/universal/config-service-*.zip"
SVC_UNZIP_DEST[config-service]="target/"
SVC_HEALTH[config-service]="/api/healthcheck"

SVC_TYPE[access-control-service]=jvm
SVC_PORT[access-control-service]=9096
SVC_SBT[access-control-service]=AccessControlService
SVC_LAUNCHER[access-control-service]="target/access-control-service-${TEXERA_VERSION}/bin/access-control-service"
SVC_CWD[access-control-service]="."
SVC_ZIP_GLOB[access-control-service]="access-control-service/target/universal/access-control-service-*.zip"
SVC_UNZIP_DEST[access-control-service]="target/"
SVC_HEALTH[access-control-service]="/api/healthcheck"

SVC_TYPE[file-service]=jvm
SVC_PORT[file-service]=9092
SVC_SBT[file-service]=FileService
SVC_LAUNCHER[file-service]="target/file-service-${TEXERA_VERSION}/bin/file-service"
SVC_CWD[file-service]="."
SVC_ZIP_GLOB[file-service]="file-service/target/universal/file-service-*.zip"
SVC_UNZIP_DEST[file-service]="target/"
SVC_HEALTH[file-service]="/api/healthcheck"

SVC_TYPE[workflow-compiling-service]=jvm
SVC_PORT[workflow-compiling-service]=9090
SVC_SBT[workflow-compiling-service]=WorkflowCompilingService
SVC_LAUNCHER[workflow-compiling-service]="target/workflow-compiling-service-${TEXERA_VERSION}/bin/workflow-compiling-service"
SVC_CWD[workflow-compiling-service]="."
SVC_ZIP_GLOB[workflow-compiling-service]="workflow-compiling-service/target/universal/workflow-compiling-service-*.zip"
SVC_UNZIP_DEST[workflow-compiling-service]="target/"
SVC_HEALTH[workflow-compiling-service]="/api/healthcheck"

SVC_TYPE[computing-unit-managing-service]=jvm
SVC_PORT[computing-unit-managing-service]=8082
SVC_SBT[computing-unit-managing-service]=ComputingUnitManagingService
SVC_LAUNCHER[computing-unit-managing-service]="target/computing-unit-managing-service-${TEXERA_VERSION}/bin/computing-unit-managing-service"
SVC_CWD[computing-unit-managing-service]="."
SVC_ZIP_GLOB[computing-unit-managing-service]="computing-unit-managing-service/target/universal/computing-unit-managing-service-*.zip"
SVC_UNZIP_DEST[computing-unit-managing-service]="target/"
SVC_HEALTH[computing-unit-managing-service]=""

SVC_TYPE[texera-web]=jvm
SVC_PORT[texera-web]=8080
SVC_SBT[texera-web]=WorkflowExecutionService
SVC_LAUNCHER[texera-web]="target/amber-${TEXERA_VERSION}/bin/texera-web-application"
SVC_CWD[texera-web]="amber"
SVC_ZIP_GLOB[texera-web]="amber/target/universal/amber-*.zip"
SVC_UNZIP_DEST[texera-web]="amber/target/"
SVC_HEALTH[texera-web]="/api/healthcheck"

# computing-unit-master shares the amber dist with texera-web: sbt-native-
# packager emits both `bin/texera-web-application` and `bin/computing-unit-master`
# launchers under `amber/target/amber-<VERSION>/`. We register it as a separate
# service for status/start/stop but leave SVC_SBT / SVC_ZIP_GLOB empty so the
# build pipeline knows to skip it (the texera-web build path already produces
# its launcher). Source-dir and canary-jar lookups treat it identically to
# texera-web — see _svc_src_dirs / svc_src_changed / svc_artifact_mtime.
SVC_TYPE[computing-unit-master]=jvm
SVC_PORT[computing-unit-master]=8085
SVC_SBT[computing-unit-master]=""
SVC_LAUNCHER[computing-unit-master]="target/amber-${TEXERA_VERSION}/bin/computing-unit-master"
SVC_CWD[computing-unit-master]="amber"
SVC_ZIP_GLOB[computing-unit-master]=""
SVC_UNZIP_DEST[computing-unit-master]=""
SVC_HEALTH[computing-unit-master]=""

SVC_TYPE[agent-service]=bun
SVC_PORT[agent-service]=3001
SVC_CWD[agent-service]="agent-service"
SVC_HEALTH[agent-service]="/api/healthcheck"

SVC_TYPE[frontend]=yarn
SVC_PORT[frontend]=4200
SVC_CWD[frontend]="frontend"
SVC_HEALTH[frontend]=""

# --------- docker infra config ---------
DOCKER_PROJECT="texera-local-dev"
DOCKER_COMPOSE_FILE="$REPO_ROOT/bin/single-node/docker-compose.yml"
DOCKER_OVERLAY_FILE="$REPO_ROOT/bin/local-dev/docker-compose.override.yml"
DOCKER_ENV_FILE="$REPO_ROOT/bin/single-node/.env"
DOCKER_INFRA_SERVICES=(postgres minio minio-init lakefs lakekeeper-migrate lakekeeper lakekeeper-init litellm)
DOCKER_INFRA_LONGLIVED=(postgres minio lakefs lakekeeper litellm)  # exclude one-shot init jobs

# Build the array of -f flags: base single-node compose + local-dev overlay
# (the overlay publishes infra ports to the host, which the upstream compose
# intentionally does not do).
docker_compose_files() {
    local args=(-f "$DOCKER_COMPOSE_FILE")
    [[ -f "$DOCKER_OVERLAY_FILE" ]] && args+=(-f "$DOCKER_OVERLAY_FILE")
    print -r -- "${args[@]}"
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
        printf "  ${BLUE}${SYM_PROG}${RESET}  ${DIM}%s (no-TTY, no spinner)${RESET}\n" "$msg"
        return
    fi
    local frames="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local n=${#frames}
    local i=0
    local start_ts=$SECONDS
    printf "\e[?25l"   # hide cursor
    while kill -0 "$pid" 2>/dev/null; do
        local elapsed=$((SECONDS - start_ts))
        local frame="${frames[((i % n) + 1)]}"
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

    local frames="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local n_frames=${#frames}
    local start_ts=$SECONDS
    local timeout=90
    local frame_idx=0
    local first_render=true
    local n_done=0
    local n_failed=0
    local svc="" i=0 state="" state_color="" state_sym="" port_str="" elapsed=0 spinner_frame=""

    if [[ ! -t 1 ]]; then
        # Non-TTY: redrawing would just spam lines. Fall back to sequential.
        local n_done=0 n_failed=0
        for svc in "${svcs[@]}"; do
            if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
                local dstate=""
                dstate=$(docker_svc_state "$svc")
                case "$dstate" in
                    running|exited)
                        printf "  %s  %-32s :%-6s  %s\n" "$SYM_OK" "$svc" "${SVC_PORT[$svc]}" "$dstate"
                        n_done=$((n_done+1)) ;;
                    *)
                        printf "  %s  %-32s :%-6s  %s\n" "$SYM_ERR" "$svc" "${SVC_PORT[$svc]}" "$dstate"
                        n_failed=$((n_failed+1)) ;;
                esac
                continue
            fi
            if wait_for_port "${SVC_PORT[$svc]}" "$timeout"; then
                printf "  %s  %-32s :%-6s  %s\n" "$SYM_OK" "$svc" "${SVC_PORT[$svc]}" "healthy"
                n_done=$((n_done+1))
            else
                printf "  %s  %-32s :%-6s  %s\n" "$SYM_ERR" "$svc" "${SVC_PORT[$svc]}" "timeout"
                n_failed=$((n_failed+1))
            fi
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

        spinner_frame="${frames[((frame_idx % n_frames) + 1)]}"

        for svc in "${svcs[@]}"; do
            state_color="" state_sym=""
            if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
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
                        if (( elapsed >= timeout )); then
                            state="timeout"
                            state_color="$RED"; state_sym="${RED}${SYM_ERR}${RESET}"
                            n_failed=$((n_failed+1))
                        else
                            state="${dstate} (${elapsed}s)"
                            state_color="$YELLOW"; state_sym="${YELLOW}${spinner_frame}${RESET}"
                        fi ;;
                esac
                port_str=":${SVC_PORT[$svc]}"
            else
                if [[ -n "$(listen_pid_for_port "${SVC_PORT[$svc]}")" ]]; then
                    state="healthy"
                    state_color="$GREEN"; state_sym="${GREEN}${SYM_OK}${RESET}"
                    n_done=$((n_done+1))
                elif (( elapsed >= timeout )); then
                    state="timeout — see bin/local-dev.sh logs $svc"
                    state_color="$RED"; state_sym="${RED}${SYM_ERR}${RESET}"
                    n_failed=$((n_failed+1))
                else
                    state="starting (${elapsed}s)"
                    state_color="$YELLOW"; state_sym="${YELLOW}${spinner_frame}${RESET}"
                fi
                port_str=":${SVC_PORT[$svc]}"
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

# --------- helpers ---------
listen_pid_for_port() {
    # || true so pipefail doesn't kill us when nothing is listening
    lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -1 || true
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

svc_running_pid() {
    listen_pid_for_port "${SVC_PORT[$1]}"
}

svc_artifact_mtime() {
    local svc="$1" type="${SVC_TYPE[$1]}"
    case "$type" in
        jvm)
            local launcher="${SVC_CWD[$svc]}/${SVC_LAUNCHER[$svc]}"
            launcher="${launcher#./}"
            local jar_dir=""
            jar_dir="$(dirname "$(dirname "$launcher")")/lib"
            if [[ -d "$jar_dir" ]]; then
                local main_jars=("$jar_dir"/org.apache.texera.${svc}-*.jar(NoL[1]))
                if [[ ${#main_jars[@]} -eq 0 && ( "$svc" == "texera-web" || "$svc" == "computing-unit-master" ) ]]; then
                    main_jars=("$jar_dir"/org.apache.texera.amber-*.jar(NoL[1]))
                fi
                if [[ ${#main_jars[@]} -gt 0 ]]; then
                    stat -f "%Sm" -t "%Y-%m-%d %H:%M" "${main_jars[1]}"
                    return
                fi
            fi
            echo "—"
            ;;
        bun)    stat -f "%Sm" -t "%Y-%m-%d %H:%M" "${SVC_CWD[$svc]}/bun.lock" 2>/dev/null || echo "—" ;;
        yarn)   stat -f "%Sm" -t "%Y-%m-%d %H:%M" "${SVC_CWD[$svc]}/yarn.lock" 2>/dev/null || echo "—" ;;
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

stop_one() {
    local svc="$1"
    if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
        tui_step "$svc: docker compose stop $svc"
        docker compose -p "$DOCKER_PROJECT" stop "$svc" >/dev/null 2>&1 || true
        tui_ok "$svc: stopped"
        return
    fi
    local pid=""
    pid=$(svc_running_pid "$svc")
    if [[ -z "$pid" ]]; then
        tui_skip "$svc: already stopped"
        return 0
    fi
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
    tui_ok "$svc: stopped"
}

start_one() {
    local svc="$1"
    local type="${SVC_TYPE[$svc]}"
    if [[ "$type" == "docker" ]]; then
        local dstate=""
        dstate=$(docker_svc_state "$svc")
        if [[ "$dstate" == "running" ]]; then
            tui_ok "$svc: already running"
            return 0
        fi
        tui_step "$svc: docker compose up -d $svc"
        local files=()
        while read -r f; do files+=("$f"); done < <(docker_compose_files | tr ' ' '\n')
        docker compose --progress auto -p "$DOCKER_PROJECT" --env-file "$DOCKER_ENV_FILE" \
            "${files[@]}" up -d "$svc" >/dev/null 2>&1
        tui_ok "$svc: started"
        return
    fi
    if [[ -n "$(svc_running_pid "$svc")" ]]; then
        tui_ok "$svc: already running ${DIM}(PID $(svc_running_pid "$svc"))${RESET}"
        return 0
    fi
    local cwd="${SVC_CWD[$svc]}" log="$LOG_DIR/$svc.log"
    tui_step "$svc: starting ${DIM}(log: $log)${RESET}"
    case "$type" in
        jvm)
            local launcher="${SVC_LAUNCHER[$svc]}"
            if [[ ! -x "$cwd/$launcher" ]]; then
                tui_err "$svc: launcher missing at $cwd/$launcher -- run \`bin/local-dev.sh up\` to build first"
                return 1
            fi
            ( cd "$cwd" && nohup "./$launcher" >"$log" 2>&1 </dev/null & )
            ;;
        bun)
            if ! command -v bun >/dev/null 2>&1; then
                tui_err "$svc: \`bun\` not found on PATH"
                _install_hint bun
                return 1
            fi
            ( cd "$cwd" && nohup bun run dev >"$log" 2>&1 </dev/null & )
            ;;
        yarn)
            if ! command -v yarn >/dev/null 2>&1; then
                tui_err "$svc: \`yarn\` not found on PATH"
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
    local svc="$1" proj="${SVC_SBT[$svc]}"
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
    if tui_run_with_spinner "$log" "sbt $proj/dist  ${DIM}(log: $log)${RESET}" \
        sbt -no-colors "$proj/dist"; then
        tui_step "unzip ${SVC_ZIP_GLOB[$svc]} → ${SVC_UNZIP_DEST[$svc]}"
        # shellcheck disable=SC2086
        unzip -oq ${SVC_ZIP_GLOB[$svc]} -d "${SVC_UNZIP_DEST[$svc]}"
        # Stamp = SHA-1 of the source we just built from. Clears the `*` and
        # lets us tell content-vs-mtime apart on the next dirty check.
        svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
        tui_ok "$svc: build done"
    else
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
        [[ "${SVC_TYPE[$svc]}" == "jvm" ]] || continue
        if svc_src_changed "$svc" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

# DEPRECATED — kept only as a documentation breadcrumb. Used to be the mtime-
# based check `up --auto` consulted; replaced by any_jvm_src_changed because
# git checkouts move mtimes without changing content.
needs_jvm_build() {
    local canary="amber/target/amber-${TEXERA_VERSION}/lib/org.apache.texera.amber-${TEXERA_VERSION}.jar"
    [[ ! -f "$canary" ]] && return 0
    local newer=""
    newer=$(find amber/src common/dao/src common/config/src common/auth/src \
        common/workflow-core/src common/workflow-operator/src common/pybuilder/src \
        config-service/src access-control-service/src file-service/src \
        workflow-compiling-service/src computing-unit-managing-service/src \
        build.sbt amber/build.sbt config-service/build.sbt access-control-service/build.sbt \
        file-service/build.sbt workflow-compiling-service/build.sbt \
        computing-unit-managing-service/build.sbt project/JdkOptions.scala project/plugins.sbt \
        \( -newer "$canary" \) -type f -print 2>/dev/null | head -1)
    [[ -n "$newer" ]]
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

_svc_src_dirs() {
    local svc="$1"
    # texera-web and computing-unit-master both live under amber/.
    if [[ "$svc" == "texera-web" || "$svc" == "computing-unit-master" ]]; then
        echo "amber/src"
    else
        echo "$svc/src"
    fi
    # One dir per line — zsh doesn't word-split unquoted `$d` by default
    # (unlike bash), so multi-dir strings would land in the array as a single
    # literal path and `find` would bail on the non-existent compound path.
    echo "common/dao/src"
    echo "common/config/src"
    echo "common/auth/src"
    echo "common/workflow-core/src"
    echo "common/workflow-operator/src"
    echo "common/pybuilder/src"
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
    case "${SVC_TYPE[$svc]}" in
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
    if [[ "${FRESH:-false}" == "true" ]]; then
        if tui_run_with_spinner "$log" "sbt clean dist  ${DIM}(log: $log)${RESET}" \
            sbt -no-colors clean dist; then
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
            sbt -no-colors dist; then
            tui_ok "sbt: dist done"
            BUILD_DID_RUN=true
        else
            tui_err "sbt: dist FAILED  ${DIM}(tail -f $log)${RESET}"
            return 1
        fi
    fi
    # Stop any running JVMs BEFORE unzip — overwriting jars under a live JVM
    # corrupts its lazy class loads and the service silently dies later.
    if [[ "$BUILD_DID_RUN" == "true" ]]; then
        local svc="" pid=""
        for svc in "${SERVICES[@]}"; do
            [[ "${SVC_TYPE[$svc]}" == "jvm" ]] || continue
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
                [[ "${SVC_TYPE[$svc]}" == "jvm" ]] || continue
                [[ -n "$(svc_running_pid "$svc")" ]] && still_up=$((still_up+1))
            done
            (( still_up == 0 )) && break
            sleep 0.5
            waited=$((waited+1))
        done
    fi
    tui_step "unzipping dist artifacts"
    for svc in "${SERVICES[@]}"; do
        [[ "${SVC_TYPE[$svc]}" == "jvm" ]] || continue
        # Sibling services (empty ZIP_GLOB) share another service's dist —
        # just stamp them as clean since the unzip already happened for the
        # twin holding the build.
        if [[ -z "${SVC_ZIP_GLOB[$svc]}" ]]; then
            svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc" 2>/dev/null || true
            continue
        fi
        # shellcheck disable=SC2086
        if unzip -oq ${SVC_ZIP_GLOB[$svc]} -d "${SVC_UNZIP_DEST[$svc]}" 2>/dev/null; then
            svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
        else
            tui_warn "${SVC_ZIP_GLOB[$svc]} not produced"
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
cmd_status() {
    local branch="" sha=""
    branch=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    sha=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "?")
    tui_banner "Texera Local Dev" "branch: $branch  @  $sha"

    printf "\n"
    printf "    ${BOLD}%-32s %-6s %-9s %-18s %s${RESET}\n" \
        "SERVICE" "PORT" "PID" "ARTIFACT MTIME" "STATE"
    printf "    ${GRAY}"; tui_hline "─" 32; printf " "
    tui_hline "─" 6; printf " "; tui_hline "─" 9; printf " "
    tui_hline "─" 18; printf " "; tui_hline "─" 12; printf "${RESET}\n"

    local n_running=0 n_total=0
    for svc in "${SERVICES[@]}"; do
        n_total=$((n_total+1))
        local pid="—" state="stopped" mtime="—"
        if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
            state=$(docker_svc_state "$svc")
            mtime="${DIM}docker${RESET}"
        else
            local found_pid=""
            found_pid=$(svc_running_pid "$svc")
            if [[ -n "$found_pid" ]]; then state="running"; pid="$found_pid"; fi
            mtime=$(svc_artifact_mtime "$svc")
        fi
        [[ "$state" == "running" ]] && n_running=$((n_running+1))

        local sym="" color=""
        sym=$(tui_state_symbol "$state")
        color=$(tui_state_color "$state")
        printf "  %s " "$sym"
        printf "${color}%-32s${RESET} %-6s ${DIM}%-9s${RESET} %-18s ${color}%s${RESET}\n" \
            "$svc" "${SVC_PORT[$svc]}" "$pid" "$mtime" "$state"
    done
    printf "\n"

    local summary_color="$YELLOW"
    (( n_running == n_total )) && summary_color="$GREEN"
    (( n_running == 0 )) && summary_color="$GRAY"
    printf "  ${BOLD}Status${RESET}: ${summary_color}%d of %d services running${RESET}\n" "$n_running" "$n_total"

    printf "\n"
    printf "  ${CYAN}${SYM_LIST}${RESET}  Logs:    ${DIM}%s${RESET}\n" "$LOG_DIR/<service>.log"
    printf "  ${CYAN}${SYM_LIST}${RESET}  Docker:  ${DIM}docker compose -p %s ps${RESET}\n" "$DOCKER_PROJECT"
    printf "  ${CYAN}${SYM_LIST}${RESET}  Open:    ${DIM}http://localhost:4200${RESET}  ${DIM}(frontend)${RESET}\n"

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
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip=*)   SKIP_LIST="${1#--skip=}" ;;
            --fresh)    FRESH=true; BUILD=force ;;
            --build)    BUILD=force ;;
            --no-build) BUILD=no ;;
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done

    local n_skip=0
    [[ -n "$SKIP_LIST" ]] && n_skip=$(echo "$SKIP_LIST" | tr ',' '\n' | wc -l | tr -d ' ')
    local skip_label="none"
    (( n_skip > 0 )) && skip_label="$n_skip service(s)"
    tui_banner "Texera Local Dev — bringing stack up" "JDK 17 · skip=$skip_label · build=$BUILD"

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
            if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
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
            return 0
        fi
    fi

    if [[ "$BUILD" != "no" ]]; then
        tui_section "Build"
        build_all
        refresh_node_deps
    else
        tui_section "Build"
        tui_skip "build: --no-build (using existing artifacts)"
    fi

    # ▸ Services -- docker compose has its own TTY panel; native services we
    # kick off silently in the background, then a single redrawing panel below
    # shows progress for ALL of them.
    tui_section "Services  ${DIM}(launching)${RESET}"
    local svc="" cwd="" log="" type="" launcher=""

    # One project-level `docker compose up -d` for every non-skipped docker
    # row — much faster than five separate calls.
    local has_docker_targets=false
    for svc in "${SERVICES[@]}"; do
        [[ "${SVC_TYPE[$svc]}" == "docker" ]] || continue
        is_skipped "$svc" && continue
        has_docker_targets=true
        break
    done
    if $has_docker_targets; then
        case "$(infra_state)" in
            running)    tui_ok "infra: already running" ;;
            external:*) tui_err "infra: ports taken by non-script containers"
                        printf "  ${DIM}docker compose -p texera-dev down${RESET}\n" ;;
            *)          infra_up || true ;;
        esac
        # Whether infra is fresh or already up, make sure the texera_db
        # schema is current — JVMs about to start expect it (jOOQ + Iceberg
        # both need real tables).
        infra_ensure_db_schema
    fi

    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && { tui_skip "$svc: --skip"; continue; }
        type="${SVC_TYPE[$svc]}"
        [[ "$type" == "docker" ]] && continue   # already handled by infra_up
        if [[ -n "$(svc_running_pid "$svc")" ]]; then
            tui_ok "$svc: already running ${DIM}(PID $(svc_running_pid "$svc"))${RESET}"
            continue
        fi
        cwd="${SVC_CWD[$svc]}"
        log="$LOG_DIR/$svc.log"
        tui_step "$svc: launching → ${DIM}$log${RESET}"
        case "$type" in
            jvm)
                launcher="${SVC_LAUNCHER[$svc]}"
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
        if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
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

    cmd_status
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
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done

    tui_banner "Texera Local Dev — auto bounce" \
        "rebuild + bounce only what changed since last build"

    # ── Scan ──────────────────────────────────────────────────────────────
    tui_section "Scan"
    local svc=""
    local dirty_jvms=()
    local need_yarn=false
    local need_bun=false
    for svc in "${SERVICES[@]}"; do
        is_skipped "$svc" && continue
        case "${SVC_TYPE[$svc]}" in
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
        local log="$LOG_DIR/sbt-dist.log"
        if ! tui_run_with_spinner "$log" "sbt dist  ${DIM}(log: $log)${RESET}" \
                sbt -no-colors dist; then
            tui_err "sbt dist failed  ${DIM}(tail -f $log)${RESET}"
            return 1
        fi
        tui_ok "sbt: dist done"
    fi

    # ── Bounce dirty JVMs ────────────────────────────────────────────────
    local n_bounced=0 n_rebuilt=0
    if (( ${#dirty_jvms[@]} > 0 )); then
        tui_section "Bounce"
        for svc in "${dirty_jvms[@]}"; do
            local pid=""
            pid=$(svc_running_pid "$svc")
            if [[ -n "$pid" ]]; then
                tui_step "$svc: stopping PID $pid before unzip"
                kill "$pid" 2>/dev/null || true
                local i=0
                while (( i < 30 )) && kill -0 "$pid" 2>/dev/null; do
                    sleep 0.5
                    i=$((i+1))
                done
                kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
            fi
            # shellcheck disable=SC2086
            if unzip -oq ${SVC_ZIP_GLOB[$svc]} -d "${SVC_UNZIP_DEST[$svc]}" 2>/dev/null; then
                svc_source_hash "$svc" > "$BUILD_STAMP_DIR/$svc"
                n_rebuilt=$((n_rebuilt+1))
            else
                tui_warn "$svc: ${SVC_ZIP_GLOB[$svc]} not produced — skipping"
                continue
            fi
            if [[ -n "$pid" ]]; then
                start_one "$svc"
                n_bounced=$((n_bounced+1))
            else
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
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip=*) SKIP_LIST="${1#--skip=}" ;;
            *) tui_err "unknown flag: $1" >&2; exit 2 ;;
        esac
        shift
    done
    tui_banner "Texera Local Dev — stopping stack" "skip=${SKIP_LIST:-none}"
    tui_section "Stopping (reverse order)"
    local svc=""
    # Stop native services first (reverse declaration order)
    for (( i=${#SERVICES[@]}; i>=1; i-- )); do
        svc="${SERVICES[i]}"
        [[ "${SVC_TYPE[$svc]}" == "docker" ]] && continue
        if is_skipped "$svc"; then
            tui_skip "$svc: --skip"
            continue
        fi
        stop_one "$svc"
    done
    # Then one project-level docker compose down for any docker target.
    local has_docker_targets=false
    for svc in "${SERVICES[@]}"; do
        [[ "${SVC_TYPE[$svc]}" == "docker" ]] || continue
        if is_skipped "$svc"; then
            tui_skip "$svc: --skip"
            continue
        fi
        has_docker_targets=true
    done
    $has_docker_targets && infra_down
    printf "\n"
}

cmd_update_one() {
    local svc="$1"
    if [[ -z "${SVC_TYPE[$svc]+x}" ]]; then
        tui_err "unknown service: ${BOLD}$svc${RESET}"
        printf "  ${DIM}Known:${RESET} ${SERVICES[*]}\n"
        exit 1
    fi
    local type="${SVC_TYPE[$svc]}"
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
            ( cd "${SVC_CWD[$svc]}" && bun install )
            tui_section "Bounce"
            stop_one "$svc"
            start_one "$svc"
            ;;
        jvm)
            if [[ -n "${SVC_SBT[$svc]}" ]]; then
                tui_banner "Updating ${svc}" "sbt ${SVC_SBT[$svc]}/dist + bounce"
            else
                tui_banner "Updating ${svc}" "bounce only (shares dist with its sibling)"
            fi
            tui_section "Build"
            build_one_jvm "$svc"
            tui_section "Bounce"
            stop_one "$svc"
            start_one "$svc"
            ;;
    esac
    tui_section "Health"
    if wait_for_port "${SVC_PORT[$svc]}" 60; then
        printf "  ${GREEN}${SYM_OK}${RESET}  %-32s ${DIM}:%s${RESET}\n" "$svc" "${SVC_PORT[$svc]}"
    else
        printf "  ${RED}${SYM_ERR}${RESET}  %-32s ${DIM}:%s${RESET}  ${RED}timeout${RESET}  ${DIM}(bin/local-dev.sh logs %s)${RESET}\n" \
            "$svc" "${SVC_PORT[$svc]}" "$svc"
        exit 1
    fi
    printf "\n"
}

cmd_logs() {
    local svc="${1:?usage: bin/local-dev.sh logs <service>}"
    if [[ -z "${SVC_TYPE[$svc]+x}" ]]; then
        echo "Unknown service: $svc" >&2
        exit 1
    fi
    if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
        exec docker compose -p "$DOCKER_PROJECT" logs -f "$svc"
    fi
    exec tail -f "$LOG_DIR/$svc.log"
}

# Render the interactive dashboard panel (banner + service table + hint + summary).
tui_render_dashboard() {
    printf "\e[2J\e[H"   # clear screen + home cursor (scrollback preserved)
    local branch="" sha=""
    branch=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    sha=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "?")
    tui_banner "Texera Local Dev — interactive" "branch: $branch @ $sha · $(date '+%H:%M:%S') · type ? for help"
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
        if [[ "${SVC_TYPE[$svc]}" == "docker" ]]; then
            state=$(docker_svc_state "$svc")
            mtime="docker"
            port_str=":${SVC_PORT[$svc]}"
        else
            local found_pid=""
            found_pid=$(svc_running_pid "$svc")
            if [[ -n "$found_pid" ]]; then state="running"; pid="$found_pid"; fi
            mtime=$(svc_artifact_mtime "$svc")
            port_str=":${SVC_PORT[$svc]}"
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
        print -r -- "$cand"
    done
}

# Walk `_probed_pythons` and return the first interpreter where `import
# textual` succeeds, or empty string if none.
_find_python_with_textual() {
    local cand=""
    while IFS= read -r cand; do
        [[ -x "$cand" ]] || continue
        if "$cand" -c "import textual" >/dev/null 2>&1; then
            print -r -- "$cand"
            return 0
        fi
    done < <(_probed_pythons)
    return 1
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
    local py=""
    py="$(_find_python_with_textual)"
    if [[ -z "$py" ]]; then
        tui_err "interactive mode requires Python with the ${BOLD}textual${RESET} package"
        printf "  ${DIM}tried interpreters:${RESET} %s\n" "$(_probed_pythons | paste -sd ' ' -)"
        printf "\n"
        _install_hint python
        exit 1
    fi
    exec "$py" "$REPO_ROOT/bin/local-dev-tui.py"
}

# --------- main ---------
case "${1:-}" in
    ""|status)        cmd_status ;;             # default: one-shot dashboard (safe in scripts/CI)
    -i|--interactive) cmd_interactive ;;        # opt in to the live TUI
    up)               shift; cmd_up "$@" ;;
    auto)             shift; cmd_auto "$@" ;;
    down)             shift; cmd_down "$@" ;;
    start)            shift; start_one "${1:?need service name}" ;;
    stop)             shift; stop_one "${1:?need service name}" ;;
    logs)             shift; cmd_logs "${1:-}" ;;
    w|watch)          shift; cmd_watch "${1:-2}" ;;
    version)          printf "%s\n" "$TEXERA_VERSION" ;;
    -h|--help)        sed -n '17,49p' "$0" ;;
    *)                cmd_update_one "$1" ;;
esac
