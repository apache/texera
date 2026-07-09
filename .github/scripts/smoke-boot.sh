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
# assert it boots (reaches a listening port) without a runtime classpath/linkage
# failure (NoClassDefFoundError, LinkageError, Jackson/Scala-module version
# conflict). That class of bug compiles and unit-tests clean but crashes the real
# `main`, so it slips through CI, which otherwise only builds + unit-tests each
# service and never starts it. See https://github.com/apache/texera/issues/6220.
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

# Resolve the (possibly globbed) launcher to a concrete executable.
launcher="$(ls $launcher_glob 2>/dev/null | head -n1 || true)"
if [[ -z "$launcher" || ! -x "$launcher" ]]; then
  echo "::error::smoke-boot: launcher not found or not executable: $launcher_glob"
  exit 1
fi

log="$(mktemp)"
echo "smoke-boot: launching '$launcher' (port=$port timeout=${timeout}s)"
"$launcher" >"$log" 2>&1 &
pid=$!

# Runtime classpath / linkage / module failures -- the class of regression this
# check exists to catch.
crash_re='NoClassDefFoundError|ClassNotFoundException|LinkageError|NoSuchMethodError|AbstractMethodError|ExceptionInInitializerError|IncompatibleClassChangeError|requires Jackson Databind'

port_open() {
  if command -v nc >/dev/null 2>&1; then
    nc -z localhost "$port" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/localhost/$port") 2>/dev/null
  fi
}

outcome="timeout"
for ((i = 0; i < timeout; i++)); do
  if port_open; then outcome="listen"; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then outcome="exited"; break; fi
  sleep 1
done

# Stop the service (it may already be gone). SIGTERM, then a bounded grace
# period, then SIGKILL -- so a service that ignores SIGTERM or hangs in shutdown
# can't leave the CI step running indefinitely.
kill "$pid" 2>/dev/null || true
for _ in $(seq 1 10); do
  if ! kill -0 "$pid" 2>/dev/null; then
    break
  fi
  sleep 1
done
if kill -0 "$pid" 2>/dev/null; then
  kill -9 "$pid" 2>/dev/null || true
fi
wait "$pid" 2>/dev/null || true

fail() {
  echo "::error::smoke-boot: $*"
  echo "----- last 80 log lines from '$launcher' -----"
  tail -n 80 "$log" || true
  exit 1
}

# A linkage/module error on boot fails regardless of whether the port came up.
if grep -qE "$crash_re" "$log"; then
  fail "'$launcher' hit a runtime classpath/linkage error on boot"
fi

case "$outcome" in
  listen)
    echo "smoke-boot: OK -- '$launcher' reached LISTEN on :$port"
    ;;
  exited)
    fail "'$launcher' exited before listening on :$port"
    ;;
  *)
    fail "'$launcher' did not listen on :$port within ${timeout}s"
    ;;
esac
