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

# check-services.sh — probe each Texera HTTP service and report UP/DOWN.
# A service is UP if its application port returns any HTTP response within
# the timeout. Non-HTTP probes (e.g. WebSocket) are not covered.
# Exit 0 if every service responded, 1 otherwise.

set -u

HOST="${TEXERA_HOST:-localhost}"
TIMEOUT="${TEXERA_PROBE_TIMEOUT:-2}"

# name|port pairs, in display order
SERVICES=(
  "texera-web-application|8080"
  "computing-unit-master|8085"
  "computing-unit-managing-service|8888"
  "workflow-compiling-service|9090"
  "file-service|9092"
  "config-service|9094"
  "access-control-service|9096"
)

if [ -t 1 ]; then
  TTY=1
  TERM_LINES=$(tput lines 2>/dev/null || echo 0)
  GREEN=$'\033[0;32m'; RED=$'\033[0;31m'
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
  TTY=0
  TERM_LINES=0
  GREEN=""; RED=""; BOLD=""; DIM=""; RESET=""
fi

BAR="════════════════════════════════════════════════════════════"

down_names=()
down_ports=()
down_reasons=()
total=${#SERVICES[@]}

printf '%-34s %-6s %-6s %s\n' "SERVICE" "PORT" "STATE" "DETAIL"

for entry in "${SERVICES[@]}"; do
  name="${entry%%|*}"
  port="${entry##*|}"

  # %{http_code} prints 000 when curl never got an HTTP response.
  read -r code curl_err <<EOF
$(curl --silent --output /dev/null \
       --max-time "$TIMEOUT" \
       --write-out '%{http_code} %{errormsg}\n' \
       "http://$HOST:$port/" 2>/dev/null)
EOF

  if [ "${code:-000}" != "000" ] && [ -n "$code" ]; then
    printf '%-34s :%-5s %sUP%s    %s(HTTP %s)%s\n' \
      "$name" "$port" "$GREEN" "$RESET" "$DIM" "$code" "$RESET"
  else
    detail="${curl_err:-no response}"
    printf '%-34s :%-5s %sDOWN%s  %s(%s)%s\n' \
      "$name" "$port" "$RED" "$RESET" "$DIM" "$detail" "$RESET"
    down_names+=("$name")
    down_ports+=("$port")
    down_reasons+=("$detail")
  fi
done

# Pad with blank lines so the banner ends at the terminal's bottom row.
# Banner height = 1 leading blank + 1 bar + 1 title + 1 bar [+ N items + 1 bar].
down_count=${#down_names[@]}
if [ "$down_count" -gt 0 ]; then
  banner_height=$((4 + down_count + 1))
else
  banner_height=4
fi
table_lines=$((1 + total))  # header + per-service rows
if [ "$TTY" = "1" ] && [ "$TERM_LINES" -gt 0 ]; then
  fill=$((TERM_LINES - table_lines - banner_height))
  while [ "$fill" -gt 0 ]; do echo; fill=$((fill - 1)); done
fi

if [ "$down_count" -gt 0 ]; then
  printf '\n%s%s%s%s\n' "$BOLD" "$RED" "$BAR" "$RESET"
  printf '%s%s✗ %d of %d SERVICES DOWN%s\n' \
    "$BOLD" "$RED" "$down_count" "$total" "$RESET"
  printf '%s%s%s%s\n' "$BOLD" "$RED" "$BAR" "$RESET"
  for i in "${!down_names[@]}"; do
    printf '  %s%-32s%s :%-5s %s%s%s\n' \
      "$BOLD" "${down_names[$i]}" "$RESET" \
      "${down_ports[$i]}" \
      "$DIM" "${down_reasons[$i]}" "$RESET"
  done
  printf '%s%s%s%s\n' "$BOLD" "$RED" "$BAR" "$RESET"
  exit 1
fi

printf '\n%s%s%s%s\n' "$BOLD" "$GREEN" "$BAR" "$RESET"
printf '%s%s✓ ALL %d SERVICES STARTED SUCCESSFULLY%s\n' \
  "$BOLD" "$GREEN" "$total" "$RESET"
printf '%s%s%s%s\n' "$BOLD" "$GREEN" "$BAR" "$RESET"
exit 0
