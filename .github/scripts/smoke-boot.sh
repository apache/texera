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

# smoke-boot.sh -- launch a packaged Texera service from its unpacked dist and
# assert it boots (reaches a listening port) instead of crashing on startup with
# a runtime classpath/linkage failure (NoClassDefFoundError, LinkageError,
# Jackson/Scala-module version conflict, ...). That class of bug compiles and
# unit-tests clean but crashes the real `main`, so it slips through CI, which
# otherwise only builds + unit-tests each service and never starts it. See
# https://github.com/apache/texera/issues/6220.
#
# The verdict is based on the process's own behaviour, not on scanning its logs:
#   * reaches LISTEN         -> booted OK
#   * exits before LISTEN    -> crashed on boot (report its exit code)
#   * neither, within timeout -> hung / failed to come up
# Scanning stdout/stderr for exception names was fragile -- any library that
# merely prints an exception name in prose (e.g. jOOQ's random "tip of the day")
# tripped it. See https://github.com/apache/texera/issues/6332.
#
# Usage:
#   smoke-boot.sh <launcher-glob> <port> [timeout_secs]
#     <launcher-glob>  path (globbable) to the dist launcher, e.g.
#                      /tmp/dists/config-service-*/bin/config-service
#     <port>           application HTTP port to wait for (TCP LISTEN)
#     [timeout_secs]   how long to wait for LISTEN (default 60)
#
# Requirements:
#   * TEXERA_HOME must point at the checkout root -- services resolve their
#     config yaml from <TEXERA_HOME>/<service>/src/main/resources/...
#   * the service's backing infra must already be up (postgres for every service,
#     plus MinIO + LakeFS for file-service); the JVM connects via storage.conf
#     defaults (postgres/postgres @ localhost:5432, MinIO :9000, LakeFS :8000).

set -euo pipefail

launcher_glob="${1:?usage: smoke-boot.sh <launcher-glob> <port> [timeout]}"
port="${2:?port required}"
timeout="${3:-60}"

# Resolve the (possibly globbed) launcher to exactly one concrete executable.
# Erroring on multiple matches avoids silently smoke-testing the wrong (e.g.
# lexicographically-first) binary if a dist dir ever accumulates versions.
matches="$(ls -d $launcher_glob 2>/dev/null || true)"
count="$(printf '%s' "$matches" | grep -c . || true)"
if [[ "$count" -eq 0 ]]; then
  echo "::error::smoke-boot: launcher not found: $launcher_glob"
  exit 1
fi
if [[ "$count" -gt 1 ]]; then
  echo "::error::smoke-boot: launcher glob matched $count files, expected exactly 1: $launcher_glob"
  exit 1
fi
launcher="$matches"
if [[ ! -x "$launcher" ]]; then
  echo "::error::smoke-boot: launcher not executable: $launcher"
  exit 1
fi

port_open() {
  # Probe 127.0.0.1 explicitly (not "localhost", which can resolve to ::1 first
  # while the JVM binds IPv4 0.0.0.0, giving a false "not listening").
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
  fi
}

# Fail fast if the port is already taken. The wait loop below treats "something
# is LISTENing on :port" as a healthy boot, so a leftover/unrelated listener
# could otherwise mask a crashed service. Requiring the port to be free at launch
# means a listener detected afterwards is the service we started.
if port_open; then
  echo "::error::smoke-boot: port $port is already in use before launching '$launcher'"
  exit 1
fi

log="$(mktemp)"
echo "smoke-boot: launching '$launcher' (port=$port timeout=${timeout}s)"
"$launcher" >"$log" 2>&1 &
pid=$!

# On any exit -- including an unexpected abort under `set -e` -- stop the service
# and remove the log, so a failing script can't orphan the JVM or leak temp files.
trap 'kill "$pid" 2>/dev/null || true; rm -f "$log"' EXIT

# Wait for the service to reach one of three terminal states: it opens its port
# (booted), it exits on its own (crashed), or neither happens in time (hung).
outcome="timeout"
for ((i = 0; i < timeout; i++)); do
  if port_open; then outcome="listen"; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then outcome="exited"; break; fi
  sleep 1
done

fail() {
  echo "::error::smoke-boot: $*"
  echo "----- last 80 log lines from '$launcher' -----"
  tail -n 80 "$log" || true
  exit 1
}

# Stop a still-running service. SIGTERM, then a bounded grace period, then
# SIGKILL -- so a service that ignores SIGTERM or hangs in shutdown can't leave
# the CI step running indefinitely.
stop_service() {
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 10); do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" 2>/dev/null || true
      return 0
    fi
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

case "$outcome" in
  listen)
    stop_service
    echo "smoke-boot: OK -- '$launcher' reached LISTEN on :$port"
    ;;
  exited)
    # The service died before it ever listened -- a boot crash. Its own exit
    # code is the signal; reap it (the process has already exited) and report it.
    code=0
    wait "$pid" 2>/dev/null || code=$?
    fail "'$launcher' exited on boot (exit code $code) before listening on :$port"
    ;;
  *)
    stop_service
    fail "'$launcher' did not listen on :$port within ${timeout}s"
    ;;
esac
