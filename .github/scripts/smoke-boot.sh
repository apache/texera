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
# assert it boots without a runtime classpath/linkage failure (NoClassDefFound-
# Error, LinkageError, Jackson/Scala-module version conflict). That class of bug
# compiles and unit-tests clean but crashes the real `main`, so it slips through
# CI, which today only builds + unit-tests each service and never starts it.
# See https://github.com/apache/texera/issues/6220.
#
# Usage:
#   smoke-boot.sh <launcher-glob> <port> <mode> [timeout_secs]
#     <launcher-glob>  path (globbable) to the dist launcher, e.g.
#                      /tmp/dists/config-service-*/bin/config-service
#     <port>           application HTTP port to wait for (TCP LISTEN)
#     <mode>           strict  -- must reach LISTEN within timeout, else FAIL.
#                      shallow -- an early exit that is NOT a linkage/module
#                                 error passes (for a service that fail-fasts on
#                                 infra this job does not provision, e.g.
#                                 file-service -> MinIO/LakeFS). A linkage/module
#                                 error still fails, even in shallow mode.
#     [timeout_secs]   how long to wait for LISTEN (default 60).
#
# Requirements:
#   * TEXERA_HOME must point at the checkout root -- services resolve their
#     config yaml from <TEXERA_HOME>/<service>/src/main/resources/...
#   * the backing postgres must already be up; the JVM connects via storage.conf
#     defaults (postgres/postgres @ localhost:5432/texera_db).

set -uo pipefail

launcher_glob="${1:?usage: smoke-boot.sh <launcher-glob> <port> <mode> [timeout]}"
port="${2:?port required}"
mode="${3:?mode required (strict|shallow)}"
timeout="${4:-60}"

# Resolve the (possibly globbed) launcher to a concrete executable.
launcher="$(ls $launcher_glob 2>/dev/null | head -n1 || true)"
if [[ -z "$launcher" || ! -x "$launcher" ]]; then
  echo "::error::smoke-boot: launcher not found or not executable: $launcher_glob"
  exit 1
fi

log="$(mktemp)"
echo "smoke-boot: launching '$launcher' (port=$port mode=$mode timeout=${timeout}s)"
"$launcher" >"$log" 2>&1 &
pid=$!

# Runtime classpath / linkage / module failures -- always fail, in any mode.
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

# Stop the service (it may already be gone).
kill "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true

fail() {
  echo "::error::smoke-boot: $*"
  echo "----- last 80 log lines from '$launcher' -----"
  tail -n 80 "$log" || true
  exit 1
}

# A linkage/module error on boot is a failure regardless of mode or outcome --
# this is the exact class of regression the check exists to catch.
if grep -qE "$crash_re" "$log"; then
  fail "'$launcher' hit a runtime classpath/linkage error on boot"
fi

case "$outcome" in
  listen)
    echo "smoke-boot: OK -- '$launcher' reached LISTEN on :$port"
    ;;
  exited)
    [[ "$mode" == "shallow" ]] || fail "'$launcher' exited before listening on :$port"
    echo "smoke-boot: OK (shallow) -- '$launcher' exited with no linkage error (unprovisioned infra tolerated)"
    ;;
  *)
    [[ "$mode" == "shallow" ]] || fail "'$launcher' did not listen on :$port within ${timeout}s"
    echo "smoke-boot: OK (shallow) -- '$launcher' survived ${timeout}s with no linkage error"
    ;;
esac
