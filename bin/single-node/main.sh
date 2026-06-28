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
#   bin/single-node.sh up [--with-examples]  pre-flight docker check, then
#                                            `docker compose up -d`. Detached
#                                            so you can keep using the shell.
#                                            Add `--with-examples` to also
#                                            pre-create two demo workflows +
#                                            datasets (the `examples` profile).
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

# --------- subcommands ---------
cmd_up() {
    local with_examples=false
    case "${1:-}" in
        "")              ;;
        --with-examples) with_examples=true ;;
        *)
            tui_err "unknown flag for up: $1 (only --with-examples is accepted)"
            exit 1
            ;;
    esac
    need_docker
    tui_header "Texera single-node — up"
    if $with_examples; then
        tui_step "docker compose --profile examples up -d  (with demo workflows + datasets)"
        compose --profile examples up -d
    else
        tui_step "docker compose up -d"
        compose up -d
    fi
    tui_ok "stack started (first boot pulls ~5 min of images)"
    printf "\n"
    print_url_tip
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
    # Always include `--profile examples` so we also stop the demo
    # containers if the user started with --with-examples. compose
    # silently ignores profile services that aren't running, so this is
    # safe in both modes.
    if $drop_volumes; then
        tui_step "docker compose --profile examples down --volumes"
        compose --profile examples down --volumes
        tui_ok "stack stopped & data volumes removed"
    else
        tui_step "docker compose --profile examples down"
        compose --profile examples down
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
