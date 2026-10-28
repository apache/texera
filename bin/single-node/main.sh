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

# bin/single-node.sh -- Manage the Texera single-node Docker stack.
#
# Subcommands:
#   bin/single-node.sh                       same as `status` (no-arg default).
#   bin/single-node.sh up [--with-examples] [--with-observability]
#                                            pre-flight docker check, then
#                                            `docker compose up -d`. Detached
#                                            so you can keep using the shell.
#                                            --with-examples pre-creates two
#                                            demo workflows + datasets; and
#                                            --with-observability starts the
#                                            collector + Grafana/Loki/Jaeger/
#                                            VictoriaMetrics and turns telemetry
#                                            emission on (off by default).
#   bin/single-node.sh down [--volumes]      stop every container. `--volumes`
#                                            also drops data volumes
#                                            (full reset — destroys workflows,
#                                            datasets, accounts).
#   bin/single-node.sh status                container state (`docker compose
#                                            ps`) + dashboard URL tip block.
#   bin/single-node.sh logs <service>        tail one service's logs (Ctrl-C
#                                            to detach).
#   bin/single-node.sh --help                full reference.
#
# For development with native JVM + frontend (sbt / yarn / bun) instead of
# docker, see `bin/local-dev.sh`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/bin/single-node/docker-compose.yml"
OBS_OVERRIDE="$REPO_ROOT/bin/single-node/docker-compose.observability.yml"
# Signal profiles enabled by --with-observability. Parca/eBPF profiling is a
# separate privileged opt-in and is deliberately not included here.
OBS_PROFILES=(--profile observability-collector --profile observability-logs --profile observability-metrics --profile observability-traces --profile observability-grafana)

# --------- output helpers ---------
if [[ -t 1 ]]; then
    BOLD=$'\e[1m'; DIM=$'\e[2m'; RESET=$'\e[0m'
    GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; CYAN=$'\e[36m'
else
    BOLD=""; DIM=""; RESET=""; GREEN=""; YELLOW=""; RED=""; CYAN=""
fi
tui_step() { printf "  ${CYAN}→${RESET}  %s\n" "$*"; }
tui_ok()   { printf "  ${GREEN}✓${RESET}  %s\n" "$*"; }
tui_warn() { printf "  ${YELLOW}⚠${RESET}  %s\n" "$*"; }
tui_err()  { printf "  ${RED}✗${RESET}  %s\n" "$*" >&2; }
tui_header() {
    local title="$1"
    printf "\n${BOLD}╭──────────────────────────────────────────────────────────────────────────────╮${RESET}\n"
    printf "${BOLD}│${RESET}  ${BOLD}%-76s${RESET}${BOLD}│${RESET}\n" "$title"
    printf "${BOLD}╰──────────────────────────────────────────────────────────────────────────────╯${RESET}\n\n"
}

# --------- pre-flight ---------
need_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        tui_err "docker not on PATH — install Docker Desktop and try again"
        exit 1
    fi
    if ! docker info >/dev/null 2>&1; then
        tui_err "Docker daemon not reachable — is Docker Desktop running?"
        exit 1
    fi
    if ! docker compose version >/dev/null 2>&1; then
        tui_err "docker compose v2 not available — upgrade Docker Desktop"
        exit 1
    fi
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        tui_err "compose file missing: $COMPOSE_FILE"
        exit 1
    fi
}

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

print_url_tip() {
    printf "  ${DIM}Open:${RESET}     ${BOLD}http://localhost:8080${RESET}\n"
    printf "  ${DIM}Login:${RESET}    ${BOLD}texera${RESET} / ${BOLD}texera${RESET} (default)\n"
    printf "  ${DIM}Logs:${RESET}     bin/single-node.sh logs <service>\n"
    printf "  ${DIM}Compose:${RESET}  docker compose -f bin/single-node/docker-compose.yml ps\n\n"
}

print_observability_urls() {
    printf "  ${BOLD}Observability${RESET} (enabled, default-off otherwise):\n"
    printf "  ${DIM}Grafana:${RESET}  ${BOLD}http://localhost:3000${RESET}        admin / texera, dashboards over all signals\n"
    printf "  ${DIM}Jaeger:${RESET}   ${BOLD}http://localhost:16686${RESET}       traces\n"
    printf "  ${DIM}Metrics:${RESET}  ${BOLD}http://localhost:8428/vmui/${RESET}  VictoriaMetrics\n\n"
}

# --------- subcommands ---------
cmd_up() {
    local with_examples=false with_observability=false
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --with-examples)      with_examples=true ;;
            --with-observability) with_observability=true ;;
            *)
                tui_err "unknown flag for up: $1 (accepted: --with-examples, --with-observability)"
                exit 1
                ;;
        esac
        shift
    done
    need_docker
    tui_header "Texera single-node — up"
    local file_args=() profile_args=()
    $with_examples && profile_args+=(--profile examples)
    if $with_observability; then
        profile_args+=("${OBS_PROFILES[@]}")
        file_args+=(-f "$OBS_OVERRIDE")
    fi
    local mode="docker compose up -d"
    $with_observability && mode="$mode (observability on)"
    $with_examples && mode="$mode (with examples)"
    tui_step "$mode"
    compose "${file_args[@]}" "${profile_args[@]}" up -d
    tui_ok "stack started (first boot pulls ~5 min of images)"
    printf "\n"
    print_url_tip
    $with_observability && print_observability_urls
}

cmd_down() {
    local drop_volumes=false
    case "${1:-}" in
        "")          ;;
        --volumes|-v) drop_volumes=true ;;
        *)
            tui_err "unknown flag for down: $1 (only --volumes is accepted)"
            exit 1
            ;;
    esac
    need_docker
    tui_header "Texera single-node — down"
    # Include every optional profile (examples + observability) so down stops
    # them too, however the stack was started. compose silently ignores profile
    # services that aren't running, so this is safe in all modes.
    if $drop_volumes; then
        tui_step "docker compose down --volumes"
        compose --profile examples "${OBS_PROFILES[@]}" down --volumes
        tui_ok "stack stopped & data volumes removed"
    else
        tui_step "docker compose down"
        compose --profile examples "${OBS_PROFILES[@]}" down
        tui_ok "stack stopped (data volumes preserved)"
    fi
}

cmd_status() {
    need_docker
    tui_header "Texera single-node"
    compose ps
    printf "\n"
    print_url_tip
}

cmd_logs() {
    if [[ $# -eq 0 ]]; then
        tui_err "usage: bin/single-node.sh logs <service>"
        exit 1
    fi
    need_docker
    compose logs -f "$1"
}

cmd_help() {
    # Print the long-form usage block at the top of this file (the
    # leading `# ` lines starting at the "bin/single-node.sh -- ..."
    # heading). Keeps --help and the in-file docs in lockstep.
    awk '
        /^# bin\/single-node\.sh -- / { on=1 }
        on && /^[^#]/                  { exit }
        on                              { sub(/^# ?/,""); print }
    ' "$0"
}

# --------- dispatch ---------
case "${1:-}" in
    ""|status)     cmd_status ;;
    up)            shift; cmd_up "$@" ;;
    down)          shift; cmd_down "$@" ;;
    logs)          shift; cmd_logs "$@" ;;
    -h|--help)     cmd_help ;;
    *)
        tui_err "unknown subcommand: $1 (try \`bin/single-node.sh --help\`)"
        exit 1
        ;;
esac
