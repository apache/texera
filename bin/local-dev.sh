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
#   bin/local-dev.sh                          DEFAULT — launches the Textual
#                                             TUI dashboard (live service
#                                             states, SRC dirty indicator,
#                                             command prompt, double-click for
#                                             logs, ↑/↓ history, Ctrl-C twice
#                                             to quit).
#   bin/local-dev.sh status                   one-shot text dashboard (no TUI).
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

# --------- toolchain (JDK 17 + nvm node) ---------
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@17}"
if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
    echo "FATAL: $JAVA_HOME/bin/java missing. Install with: brew install openjdk@17" >&2
    exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"

if [[ -z "${NVM_DIR:-}" && -d "$HOME/.nvm" ]]; then
    export NVM_DIR="$HOME/.nvm"
fi
if [[ -n "${NVM_DIR:-}" && -s "$NVM_DIR/nvm.sh" ]]; then
    # shellcheck disable=SC1091
    \. "$NVM_DIR/nvm.sh" >/dev/null 2>&1 || true
fi

# --------- runtime env for backend ---------
export STORAGE_JDBC_URL="${STORAGE_JDBC_URL:-jdbc:postgresql://localhost:5432/texera_db?currentSchema=texera_db,public}"
export STORAGE_JDBC_USERNAME="${STORAGE_JDBC_USERNAME:-texera}"
export STORAGE_JDBC_PASSWORD="${STORAGE_JDBC_PASSWORD:-password}"
export STORAGE_S3_ENDPOINT="${STORAGE_S3_ENDPOINT:-http://localhost:9000}"
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
export UDF_PYTHON_PATH="${UDF_PYTHON_PATH:-$HOME/Repos/venv312/bin/python}"
export TEXERA_DASHBOARD_SERVICE_ENDPOINT="${TEXERA_DASHBOARD_SERVICE_ENDPOINT:-http://localhost:8080}"
export WORKFLOW_COMPILING_SERVICE_ENDPOINT="${WORKFLOW_COMPILING_SERVICE_ENDPOINT:-http://localhost:9090}"
export WORKFLOW_EXECUTION_SERVICE_ENDPOINT="${WORKFLOW_EXECUTION_SERVICE_ENDPOINT:-http://localhost:8085}"
export FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT="${FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT:-http://localhost:9092/api/dataset/presign-download}"
export FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT="${FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT:-http://localhost:9092/api/dataset/did/upload}"
export LITELLM_BASE_URL="${LITELLM_BASE_URL:-http://localhost:4000}"
export LITELLM_MASTER_KEY="${LITELLM_MASTER_KEY:-sk-texera-internal-do-not-share}"
export LLM_ENDPOINT="${LLM_ENDPOINT:-http://localhost:8080}"
export LLM_API_KEY="${LLM_API_KEY:-dummy}"

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
SVC_LAUNCHER[config-service]="target/config-service-1.3.0-incubating-SNAPSHOT/bin/config-service"
SVC_CWD[config-service]="."
SVC_ZIP_GLOB[config-service]="config-service/target/universal/config-service-*.zip"
SVC_UNZIP_DEST[config-service]="target/"
SVC_HEALTH[config-service]="/api/healthcheck"

SVC_TYPE[access-control-service]=jvm
SVC_PORT[access-control-service]=9096
SVC_SBT[access-control-service]=AccessControlService
SVC_LAUNCHER[access-control-service]="target/access-control-service-1.3.0-incubating-SNAPSHOT/bin/access-control-service"
SVC_CWD[access-control-service]="."
SVC_ZIP_GLOB[access-control-service]="access-control-service/target/universal/access-control-service-*.zip"
SVC_UNZIP_DEST[access-control-service]="target/"
SVC_HEALTH[access-control-service]="/api/healthcheck"

SVC_TYPE[file-service]=jvm
SVC_PORT[file-service]=9092
SVC_SBT[file-service]=FileService
SVC_LAUNCHER[file-service]="target/file-service-1.3.0-incubating-SNAPSHOT/bin/file-service"
SVC_CWD[file-service]="."
SVC_ZIP_GLOB[file-service]="file-service/target/universal/file-service-*.zip"
SVC_UNZIP_DEST[file-service]="target/"
SVC_HEALTH[file-service]="/api/healthcheck"

SVC_TYPE[workflow-compiling-service]=jvm
SVC_PORT[workflow-compiling-service]=9090
SVC_SBT[workflow-compiling-service]=WorkflowCompilingService
SVC_LAUNCHER[workflow-compiling-service]="target/workflow-compiling-service-1.3.0-incubating-SNAPSHOT/bin/workflow-compiling-service"
SVC_CWD[workflow-compiling-service]="."
SVC_ZIP_GLOB[workflow-compiling-service]="workflow-compiling-service/target/universal/workflow-compiling-service-*.zip"
SVC_UNZIP_DEST[workflow-compiling-service]="target/"
SVC_HEALTH[workflow-compiling-service]="/api/healthcheck"

SVC_TYPE[computing-unit-managing-service]=jvm
SVC_PORT[computing-unit-managing-service]=8082
SVC_SBT[computing-unit-managing-service]=ComputingUnitManagingService
SVC_LAUNCHER[computing-unit-managing-service]="target/computing-unit-managing-service-1.3.0-incubating-SNAPSHOT/bin/computing-unit-managing-service"
SVC_CWD[computing-unit-managing-service]="."
SVC_ZIP_GLOB[computing-unit-managing-service]="computing-unit-managing-service/target/universal/computing-unit-managing-service-*.zip"
SVC_UNZIP_DEST[computing-unit-managing-service]="target/"
SVC_HEALTH[computing-unit-managing-service]=""

SVC_TYPE[texera-web]=jvm
SVC_PORT[texera-web]=8080
SVC_SBT[texera-web]=WorkflowExecutionService
SVC_LAUNCHER[texera-web]="target/amber-1.3.0-incubating-SNAPSHOT/bin/texera-web-application"
SVC_CWD[texera-web]="amber"
SVC_ZIP_GLOB[texera-web]="amber/target/universal/amber-*.zip"
SVC_UNZIP_DEST[texera-web]="amber/target/"
SVC_HEALTH[texera-web]="/api/healthcheck"

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
                if [[ ${#main_jars[@]} -eq 0 && "$svc" == "texera-web" ]]; then
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
            ( cd "$cwd" && nohup bun run dev >"$log" 2>&1 </dev/null & )
            ;;
        yarn)
            ( cd "$cwd" && nohup yarn start >"$log" 2>&1 </dev/null & )
            ;;
    esac
}

build_one_jvm() {
    local svc="$1" proj="${SVC_SBT[$svc]}"
    local log="$LOG_DIR/sbt-${svc}.log"
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
    local canary="amber/target/amber-1.3.0-incubating-SNAPSHOT/lib/org.apache.texera.amber-1.3.0-incubating-SNAPSHOT.jar"
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
    if [[ "$svc" == "texera-web" ]]; then
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
                if [[ "$svc" == "texera-web" ]]; then
                    jar="amber/target/amber-1.3.0-incubating-SNAPSHOT/lib/org.apache.texera.amber-1.3.0-incubating-SNAPSHOT.jar"
                else
                    jar="target/${svc}-1.3.0-incubating-SNAPSHOT/lib/org.apache.texera.${svc}-1.3.0-incubating-SNAPSHOT.jar"
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
            tui_banner "Updating ${svc}" "sbt ${SVC_SBT[$svc]}/dist + bounce"
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
tui_pause_before_redraw() {
    printf "\n${DIM}↩ to return to dashboard...${RESET}"
    read -r _ 2>/dev/null || true
}

# Proper-TUI primitives. Real terminal UIs (htop, k9s, vim …) don't append-and-
# refresh; they enter the alternate screen buffer + raw mode so they own every
# key and can redraw whatever they want while the user types. We do the same.

_tui_saved_stty=""
_tui_in_alt_screen=false
_tui_status_line=""
_tui_log_file=""
_tui_last_cmd=""
_tui_cmd_pid=""
_tui_cmd_start_ts=0
_tui_log_lines=8   # height of the log region inside the dashboard

_tui_enter_alt() {
    _tui_saved_stty=$(stty -g 2>/dev/null)
    # -icanon: deliver each char immediately (no line buffering)
    # -echo:   we render the input ourselves
    # -isig:   Ctrl-C/Ctrl-Z don't kill us; we receive them as bytes
    # min 0 time 0: non-blocking reads
    stty -icanon -echo -isig min 0 time 0 2>/dev/null
    printf '\e[?1049h\e[?25l\e[H'   # alt screen + hide cursor + home
    _tui_in_alt_screen=true
}

_tui_leave_alt() {
    $_tui_in_alt_screen || return 0
    printf '\e[?25h\e[?1049l'
    [[ -n "$_tui_saved_stty" ]] && stty "$_tui_saved_stty" 2>/dev/null
    _tui_in_alt_screen=false
}

# Is a backgrounded command from the REPL still running?
_tui_cmd_running() {
    [[ -n "$_tui_cmd_pid" ]] && kill -0 "$_tui_cmd_pid" 2>/dev/null
}

# Full screen redraw. Called at most ~once per second (or on Enter/state change)
# because the per-row work (lsof per service, find for SRC) costs ~50-100 ms.
# Keystroke echo uses the cheap `_tui_draw_prompt` instead.
_tui_draw_full() {
    local input="$1"
    # Synchronized update if the terminal supports it (iTerm, kitty, recent
    # alacritty) — avoids partial-frame flicker while we paint top-to-bottom.
    # \e[H\e[J = move home + clear entire screen.  Doing this every frame is
    # the only sure way to wipe residue when a new frame is shorter than the
    # previous on any individual row (e.g. state was 'external:5/5' and is now
    # just 'running' — the row's tail must be cleared).
    printf '\e[?2026h\e[H\e[J'

    local branch="" sha=""
    branch=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
    sha=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "?")
    tui_banner "Texera Local Dev — interactive" \
        "branch: $branch @ $sha · $(date '+%H:%M:%S') · live; input is preserved"
    printf '\n'

    printf "    ${BOLD}%-32s %-7s %-9s %-18s %-3s %s${RESET}\n" \
        "SERVICE" "PORT" "PID" "ARTIFACT MTIME" "SRC" "STATE"
    printf "    ${GRAY}"; tui_hline "─" 32; printf " "
    tui_hline "─" 7; printf " "; tui_hline "─" 9; printf " "
    tui_hline "─" 18; printf " "; tui_hline "─" 3; printf " "
    tui_hline "─" 12; printf "${RESET}\n"

    local n_run=0 n_total=0 n_dirty=0 svc=""
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

    printf '\n'
    local sum_color="$YELLOW"
    (( n_run == n_total )) && sum_color="$GREEN"
    (( n_run == 0 ))       && sum_color="$GRAY"
    printf "  ${BOLD}${sum_color}%d of %d running${RESET}" "$n_run" "$n_total"
    if (( n_dirty > 0 )); then
        printf "    ${YELLOW}${BOLD}*${RESET} ${DIM}%d with source changes${RESET}" "$n_dirty"
    fi
    printf '\n\n'

    printf "  ${DIM}Commands:${RESET}  "
    printf "${BOLD}u${RESET}p · ${BOLD}d${RESET}own · ${BOLD}b${RESET}uild · "
    printf "${BOLD}<svc>${RESET} ${DIM}rebuild+bounce${RESET} · "
    printf "${BOLD}l${RESET}ogs ${DIM}<svc>${RESET} · "
    printf "${BOLD}s${RESET}top ${DIM}<svc>${RESET} · "
    printf "${BOLD}q${RESET}uit\n\n"

    # ── Log region ─────────────────────────────────────────────────────────
    # Always renders ${_tui_log_lines} body rows so the prompt below stays at
    # a stable row (no flicker, no rows-from-an-old-frame leakage).
    local log_header="log"
    if [[ -n "$_tui_last_cmd" ]]; then log_header="log: ${BOLD}$_tui_last_cmd${RESET}"; fi
    local log_status=""
    if _tui_cmd_running; then
        local elapsed=$((SECONDS - _tui_cmd_start_ts))
        log_status="  ${YELLOW}● running (${elapsed}s)${RESET}"
    elif [[ -n "$_tui_last_cmd" ]]; then
        log_status="  ${DIM}(done)${RESET}"
    fi
    printf "  ${GRAY}── ${RESET}%s%s\n" "$log_header" "$log_status"

    local lines_printed=0 line=""
    # During up/down — and only while the command is actually running — we
    # replace the raw log tail with a docker-compose-style per-container
    # panel. Once the command finishes we fall back to tailing the REPL log
    # so messages like the pre-flight "nothing to do" survive.
    local show_docker_panel=false
    case "$_tui_last_cmd" in
        u|up|d|down) _tui_cmd_running && show_docker_panel=true ;;
    esac
    case "$show_docker_panel" in
        true)
            local frames="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
            local frame_idx=$((SECONDS % ${#frames}))
            local spin="${frames[frame_idx + 1]}"
            # All bare in zsh — must initialize, otherwise the first assignment
            # of each (inside the loop) is echoed to stdout by `typeset`.
            local cname="" cstate="" cstatus="" sym="" color=""
            while IFS='|' read -r cname cstate cstatus; do
                (( lines_printed >= _tui_log_lines )) && break
                case "$cstate" in
                    running)
                        if [[ "$cstatus" == *'(healthy)'* ]]; then
                            sym="${GREEN}${SYM_OK}${RESET}"; color="$GREEN"
                        elif [[ "$cstatus" == *'(health: starting)'* ]]; then
                            sym="${YELLOW}${spin}${RESET}"; color="$YELLOW"
                        elif [[ "$cstatus" == *'(unhealthy)'* ]]; then
                            sym="${RED}${SYM_ERR}${RESET}"; color="$RED"
                        else
                            sym="${GREEN}${SYM_OK}${RESET}"; color="$GREEN"
                        fi ;;
                    exited)
                        if [[ "$cstatus" == 'Exited (0)'* ]]; then
                            sym="${GRAY}${SYM_OK}${RESET}"; color="$GRAY"
                        else
                            sym="${RED}${SYM_ERR}${RESET}"; color="$RED"
                        fi ;;
                    created|restarting|paused|removing)
                        sym="${YELLOW}${spin}${RESET}"; color="$YELLOW" ;;
                    *)
                        sym="${GRAY}${SYM_STOP}${RESET}"; color="$GRAY" ;;
                esac
                printf "  ${DIM}│${RESET} %s  %-26s ${color}%s${RESET}\n" \
                    "$sym" "${cname:0:26}" "${cstatus:0:$((TUI_WIDTH - 36))}"
                lines_printed=$((lines_printed + 1))
            done < <(docker compose -p "$DOCKER_PROJECT" ps -a \
                --format '{{.Name}}|{{.State}}|{{.Status}}' 2>/dev/null)
            ;;
        *)
            if [[ -n "$_tui_log_file" && -f "$_tui_log_file" ]]; then
                # Strip ALL CSI sequences and CRs in one sed pass. Keeping colors
                # is tempting, but the truncation below is by byte length and
                # ANSI codes inflate byte length unpredictably — truncating
                # mid-escape corrupts subsequent rendering. Plain text truncates
                # cleanly.
                while IFS= read -r line; do
                    (( lines_printed >= _tui_log_lines )) && break
                    printf "  ${DIM}│${RESET} %s\n" "${line:0:$((TUI_WIDTH - 4))}"
                    lines_printed=$((lines_printed + 1))
                done < <(tail -n "$_tui_log_lines" "$_tui_log_file" 2>/dev/null \
                    | LC_ALL=C sed $'s/\033\\[[0-9;?]*[A-Za-z]//g' \
                    | tr -d '\r')
            fi ;;
    esac
    while (( lines_printed < _tui_log_lines )); do
        printf "  ${DIM}│${RESET}\n"
        lines_printed=$((lines_printed + 1))
    done
    printf "  ${GRAY}"; tui_hline "─" $((TUI_WIDTH - 4)); printf "${RESET}\n"
    printf "\n"

    # Prompt + input + fake cursor (reverse-video block). Hardware cursor is
    # kept hidden so it can't flicker between renders.
    printf "${BOLD}${CYAN}local-dev${RESET} ${DIM}›${RESET} %s\e[7m \e[27m" "$input"

    # Wipe any residue from a previous taller frame, then commit the frame.
    printf '\e[J'
    printf '\e[?2026l'
}

# Cheap redraw of just the prompt line — used on every keystroke so typing
# feels instant. Assumes the cursor is still on the prompt line, which it
# always is after `_tui_draw_full` (it leaves the cursor on the fake-cursor
# block) and after a previous `_tui_draw_prompt` (same).
_tui_draw_prompt() {
    local input="$1"
    printf '\r\e[K'
    printf "${BOLD}${CYAN}local-dev${RESET} ${DIM}›${RESET} %s\e[7m \e[27m" "$input"
}

# Dispatch one entered line. Caller has already left the alt screen and
# restored normal stty, so we can run sbt / docker-compose / etc. as usual.
_tui_exec_cmd() {
    local input="$1"
    local verb="" arg=""
    verb="${input%% *}"
    arg=""
    [[ "$input" != "$verb" ]] && arg="${input#* }"

    case "$verb" in
        ""|r|refresh) ;;
        \?|h|help)
            printf "Commands:\n"
            printf "  r           refresh dashboard (also: ↩ on empty input)\n"
            printf "  u  up       build (incremental) and start every service\n"
            printf "  d  down     stop every service\n"
            printf "  b  build    force incremental sbt dist + yarn/bun install\n"
            printf "  <svc>       rebuild that JVM service and bounce it\n"
            printf "  l <svc>     tail the service log (Ctrl-C returns)\n"
            printf "  s <svc>     stop one service\n"
            printf "  q  quit     leave\n" ;;
        u|up)
            if [[ -n "$arg" ]]; then
                # `u <svc>` = bring up that one service only. No build, no
                # touching other services. Use the bare `<svc>` form if you
                # want rebuild+bounce instead.
                if [[ -z "${SVC_TYPE[$arg]+x}" ]]; then
                    tui_err "unknown service: $arg  ${DIM}(known: ${SERVICES[*]})${RESET}"
                else
                    start_one "$arg"
                fi
            else
                BUILD=auto FRESH=false SKIP_LIST=""; cmd_up
            fi ;;
        d|down)
            if [[ -n "$arg" ]]; then
                if [[ -z "${SVC_TYPE[$arg]+x}" ]]; then
                    tui_err "unknown service: $arg  ${DIM}(known: ${SERVICES[*]})${RESET}"
                else
                    stop_one "$arg"
                fi
            else
                SKIP_LIST=""; cmd_down
            fi ;;
        b|build) BUILD=force FRESH=false SKIP_LIST=""; build_all; refresh_node_deps ;;
        s|stop)
            if [[ -z "${SVC_TYPE[$arg]+x}" ]]; then
                tui_err "usage: stop <service>"
            else
                stop_one "$arg"
            fi ;;
        l|logs|tail)
            if [[ -z "${SVC_TYPE[$arg]+x}" ]]; then
                tui_err "usage: logs <service>"
            else
                printf "${DIM}Tailing $arg (Ctrl-C returns)...${RESET}\n\n"
                if [[ "${SVC_TYPE[$arg]}" == "docker" ]]; then
                    ( trap 'exit 0' INT; docker compose -p "$DOCKER_PROJECT" logs -f "$arg" )
                else
                    ( trap 'exit 0' INT; tail -f "$LOG_DIR/$arg.log" )
                fi
            fi ;;
        *)
            if [[ -n "${SVC_TYPE[$verb]+x}" ]]; then
                cmd_update_one "$verb"
            else
                tui_err "unknown: ${BOLD}$verb${RESET}  ${DIM}(type 'h' for help)${RESET}"
            fi ;;
    esac
}

cmd_interactive() {
    if [[ ! -t 0 || ! -t 1 ]]; then
        tui_err "interactive mode requires a TTY"
        exit 1
    fi
    # Prefer the Python (Textual) TUI — much smoother than the zsh repl below
    # (diff rendering, real input editing, no scrollback growth). Fall back to
    # the legacy zsh REPL if the Python interpreter or textual aren't found.
    local py="${TEXERA_PYTHON:-$HOME/Repos/venv312/bin/python}"
    if [[ -x "$py" ]] && "$py" -c "import textual" >/dev/null 2>&1; then
        exec "$py" "$REPO_ROOT/bin/local-dev-tui.py"
    fi
    tui_warn "Falling back to zsh REPL (no Python+textual found at $py)"
    tui_warn "  install:  '$py' -m pip install textual"
    set +e

    _tui_log_file="$LOG_DIR/repl.log"
    : > "$_tui_log_file"
    _tui_last_cmd=""
    _tui_cmd_pid=""

    _tui_enter_alt
    trap '_tui_leave_alt' EXIT
    trap '_tui_leave_alt; exit 130' INT
    trap '_tui_leave_alt; exit 143' TERM

    local input=""
    local last_full_ts=0
    local ch="" rest=""
    local need_full=true   # force first frame
    local need_prompt=false

    while true; do
        # 1 Hz full redraw, or whenever an event flips need_full.
        if $need_full || (( SECONDS - last_full_ts >= 1 )); then
            _tui_draw_full "$input"
            last_full_ts=$SECONDS
            need_full=false
            need_prompt=false
        elif $need_prompt; then
            _tui_draw_prompt "$input"
            need_prompt=false
        fi

        # Detect command completion so the next full redraw shows "(done)"
        # and gets a single forced refresh out of the 1 Hz cadence.
        if [[ -n "$_tui_cmd_pid" ]] && ! _tui_cmd_running; then
            _tui_cmd_pid=""
            need_full=true
            continue
        fi

        # 100 ms non-blocking read. Long enough to keep CPU at near-zero while
        # idle, short enough that the user can't notice the polling.
        if ! read -r -k 1 -t 0.1 ch 2>/dev/null; then
            continue
        fi

        case "$ch" in
            $'\n'|$'\r')   # Enter
                if [[ -z "$input" ]]; then
                    need_full=true   # blank Enter = manual refresh
                    continue
                fi
                local cmd_input="$input"
                input=""

                case "$cmd_input" in
                    q|quit|exit) break ;;
                    \?|h|help)
                        : > "$_tui_log_file"
                        {
                            printf "Commands:\n"
                            printf "  r           refresh dashboard (or just ↩)\n"
                            printf "  u           build + start every service\n"
                            printf "  u <svc>     start one service (no rebuild)\n"
                            printf "  d           stop every service\n"
                            printf "  d <svc>     stop one service\n"
                            printf "  b           force incremental sbt + node deps\n"
                            printf "  <svc>       rebuild that service and bounce it\n"
                            printf "  l <svc>     tail the service log (Ctrl-C returns)\n"
                            printf "  s <svc>     alias: stop one service\n"
                            printf "  q          leave\n"
                        } > "$_tui_log_file"
                        _tui_last_cmd="$cmd_input"
                        _tui_cmd_pid=""
                        ;;
                    *)
                        # Run the command in the background, with output piped
                        # to the REPL log. The spinner / wait_panel / docker
                        # `--progress auto` all see a non-TTY stdout there and
                        # fall back to their plain modes, which makes a clean
                        # log we can tail.
                        _tui_last_cmd="$cmd_input"
                        _tui_cmd_start_ts=$SECONDS
                        : > "$_tui_log_file"
                        ( _tui_exec_cmd "$cmd_input" ) >"$_tui_log_file" 2>&1 &
                        _tui_cmd_pid=$!
                        ;;
                esac
                need_full=true ;;
            $'\x7f'|$'\b')  # Backspace / DEL
                input="${input%?}"
                need_prompt=true ;;
            $'\x03')  # Ctrl-C
                if _tui_cmd_running; then
                    kill "$_tui_cmd_pid" 2>/dev/null || true
                    need_full=true
                elif [[ -n "$input" ]]; then
                    input=""
                    need_prompt=true
                else
                    break
                fi ;;
            $'\x04')  # Ctrl-D
                [[ -z "$input" ]] && break
                need_prompt=true ;;
            $'\x0c')  # Ctrl-L
                need_full=true ;;
            $'\x15')  # Ctrl-U: clear input line
                input=""
                need_prompt=true ;;
            $'\x17')  # Ctrl-W: delete previous word
                input="${input% *}"
                need_prompt=true ;;
            $'\x1b')  # ESC — eat any follow-up arrow-key escape sequence
                read -r -k 2 -t 0.01 rest 2>/dev/null || true ;;
            *)
                if [[ "$ch" == [[:print:]] ]]; then
                    input="${input}${ch}"
                    need_prompt=true
                fi ;;
        esac
    done

    if _tui_cmd_running; then
        kill "$_tui_cmd_pid" 2>/dev/null || true
    fi
    _tui_leave_alt
    printf "${DIM}bye${RESET}\n"
    trap - EXIT INT TERM
}

# --------- main ---------
case "${1:-}" in
    "")               cmd_interactive ;;      # no args → launch the TUI
    status)           cmd_status ;;
    up)               shift; cmd_up "$@" ;;
    down)             shift; cmd_down "$@" ;;
    start)            shift; start_one "${1:?need service name}" ;;
    stop)             shift; stop_one "${1:?need service name}" ;;
    logs)             shift; cmd_logs "${1:-}" ;;
    w|watch)          shift; cmd_watch "${1:-2}" ;;
    -h|--help)        sed -n '17,45p' "$0" ;;
    *)                cmd_update_one "$1" ;;
esac
