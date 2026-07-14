#!/bin/sh
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

# Wrapper around `docker compose up` that honors the per-backend
# observability disable env vars described in .env. Run from
# bin/single-node/.
#
# Usage:
#   ./up.sh                                  # bring up everything
#   TEXERA_OBSERVABILITY_PROFILES=disabled ./up.sh   # no Parca / agent
#   ./up.sh -d --remove-orphans              # extra args forwarded to compose
#
# Why this exists: docker-compose has no first-class way to say
# "default-on, disable per env var". We translate the TEXERA_OBSERVABILITY_*
# envs into the COMPOSE_PROFILES list, then exec `docker compose`.

set -eu

cd "$(dirname "$0")"

# Start from the full set; drop entries as disable envs are set.
PROFILES="observability-collector observability-logs observability-metrics observability-traces observability-profiles"

drop_profile() {
  # $1 = profile name to remove from PROFILES
  PROFILES=$(printf '%s\n' $PROFILES | grep -vx "$1" | tr '\n' ' ')
}

case "${TEXERA_OBSERVABILITY_LOGS:-enabled}" in
  disabled|off|false|0) drop_profile observability-logs ;;
esac
case "${TEXERA_OBSERVABILITY_METRICS:-enabled}" in
  disabled|off|false|0) drop_profile observability-metrics ;;
esac
case "${TEXERA_OBSERVABILITY_TRACES:-enabled}" in
  disabled|off|false|0) drop_profile observability-traces ;;
esac
case "${TEXERA_OBSERVABILITY_PROFILES:-enabled}" in
  disabled|off|false|0) drop_profile observability-profiles ;;
esac
case "${TEXERA_OBSERVABILITY_COLLECTOR:-enabled}" in
  disabled|off|false|0) drop_profile observability-collector ;;
esac

# Comma-separated for COMPOSE_PROFILES.
COMPOSE_PROFILES=$(printf '%s\n' $PROFILES | paste -sd, -)
export COMPOSE_PROFILES

echo "Bringing up Texera with COMPOSE_PROFILES=${COMPOSE_PROFILES:-<none>}"
exec docker compose up "$@"
