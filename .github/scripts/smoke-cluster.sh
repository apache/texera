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

# smoke-cluster.sh -- boot a computing-unit-master and a computing-unit-worker
# from their unpacked dist and assert the worker actually JOINS the master's
# amber Pekko cluster, not merely that its process survives.
#
# Why this can't reuse smoke-boot.sh: ComputingUnitWorker.main only calls
# AmberRuntime.startActorWorker, which binds pekko.remote.artery.canonical.port=0
# (a random ephemeral port) and starts no HTTP server -- so there is no fixed
# port to probe. A worker booted standalone just retries its seed forever and
# stays up, so "did not crash" proves nothing. The real health signal is cluster
# membership, which ClusterListener prints on every membership change:
#     ---------Now we have N nodes in the cluster---------  (N = cluster.state.members.size)
# master-alone => 1, master+worker joined => 2. We wait for the "2" line -- a
# deliberate readiness signal the app emits, not a crash-scan (contrast #6332).
# See issue #6523 (raised from the #6377 review) and #6220 for the rationale.
#
# Single host, no external network: the master runs in non-cluster mode, which
# self-seeds pekko://Amber@localhost:2552; the worker is launched with NO
# --serverAddr, so it defaults its seed to localhost:2552 and skips the
# getNodeIpAddress -> http://checkip.amazonaws.com lookup (that only fires when
# --serverAddr is supplied).
#
# Usage:
#   smoke-cluster.sh <master-glob> <worker-glob> <master-http-port> [join_timeout]
#
# Requirements: run from the checkout root -- the master resolves its config via
# Utils.amberHomePath -> ./amber/src/main/resources/computing-unit-master-config.yml
# -- with postgres already provisioned (the master calls SqlServer.initConnection).

set -euo pipefail

master_glob="${1:?usage: smoke-cluster.sh <master-glob> <worker-glob> <master-http-port> [join_timeout]}"
worker_glob="${2:?worker launcher glob required}"
http_port="${3:?master http port required}"
join_timeout="${4:-40}"
master_ready_timeout="${SMOKE_MASTER_READY_TIMEOUT:-60}"

PHRASE_JOINED='Now we have 2 nodes in the cluster'

# Resolve a (globbed) launcher to exactly one executable -- same contract as
# smoke-boot.sh: erroring on >1 match avoids booting the wrong binary.
RESOLVED=""
resolve_launcher() {
  local glob="$1" role="$2" matches count
  # shellcheck disable=SC2086  # intentional: $glob must glob-expand here
  matches="$(ls -d $glob 2>/dev/null || true)"
  count="$(printf '%s' "$matches" | grep -c . || true)"
  if [[ "$count" -eq 0 ]]; then
    echo "::error::smoke-cluster: $role launcher not found: $glob"; return 1
  fi
  if [[ "$count" -gt 1 ]]; then
    echo "::error::smoke-cluster: $role launcher glob matched $count files, expected exactly 1: $glob"; return 1
  fi
  if [[ ! -x "$matches" ]]; then
    echo "::error::smoke-cluster: $role launcher not executable: $matches"; return 1
  fi
  RESOLVED="$matches"
}
resolve_launcher "$master_glob" master || exit 1
master_bin="$RESOLVED"
resolve_launcher "$worker_glob" worker || exit 1
worker_bin="$RESOLVED"

port_open() {
  # Probe 127.0.0.1 explicitly (not "localhost", which can resolve to ::1 first
  # while the JVM binds IPv4, giving a false "not listening").
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$http_port" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/127.0.0.1/$http_port") 2>/dev/null
  fi
}
if port_open; then
  echo "::error::smoke-cluster: port $http_port is already in use before launching the master"
  exit 1
fi

master_log="$(mktemp)"
worker_log="$(mktemp)"
master_pid=""
worker_pid=""

alive() { kill -0 "$1" 2>/dev/null; }

dump_logs() {
  echo "----- master (last 120 lines) -----"; tail -n 120 "$master_log" 2>/dev/null || true
  echo "----- worker (last 120 lines) -----"; tail -n 120 "$worker_log" 2>/dev/null || true
}

# Reap both JVMs (SIGTERM, grace, SIGKILL) and remove temp logs on any exit --
# including a set -e abort -- so a failing run can't orphan a JVM or leak files.
# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT` below
cleanup() {
  for p in "$worker_pid" "$master_pid"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null || true; done
  for _ in $(seq 1 10); do
    { [[ -z "$master_pid" ]] || ! alive "$master_pid"; } &&
      { [[ -z "$worker_pid" ]] || ! alive "$worker_pid"; } && break
    sleep 1
  done
  for p in "$worker_pid" "$master_pid"; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -f "$master_log" "$worker_log"
}
trap cleanup EXIT

# 1) Master: non-cluster mode self-seeds localhost:2552 and serves Dropwizard on
#    :http_port. Launch from the current dir (checkout root) so its config resolves.
echo "smoke-cluster: launching master '$master_bin'"
"$master_bin" >"$master_log" 2>&1 &
master_pid=$!

# Wait for the master to be ready: its HTTP port coming up implies the actor
# system bound :2552 and DB init finished.
ready=0
for ((i = 0; i < master_ready_timeout; i++)); do
  if port_open; then ready=1; break; fi
  if ! alive "$master_pid"; then
    echo "::error::smoke-cluster: master exited before listening on :$http_port"; dump_logs; exit 1
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  echo "::error::smoke-cluster: master did not listen on :$http_port within ${master_ready_timeout}s"; dump_logs; exit 1
fi

# NB: we deliberately do NOT wait here for the master's own "1 nodes" self-join
# line. The master opens :8085 as soon as its actor system + DB are up, but its
# own MemberUp is gated by cluster.conf's leader-actions-interval (~10s) and can
# land *after* the port is already open -- keying on it here caused a false
# failure. The worker-join assertion below is the real signal; a readiness-log
# drift is reported there instead.
echo "smoke-cluster: master up (listening on :$http_port); launching worker '$worker_bin'"

# 2) Worker: no --serverAddr => seed localhost:2552, random remote port, no egress.
"$worker_bin" >"$worker_log" 2>&1 &
worker_pid=$!

# 3) Assert the worker joins: the master observes a 2-member cluster. Poll a
#    deliberate readiness line (not a crash-scan); fail fast if either JVM dies.
joined=0
for ((i = 0; i < join_timeout; i++)); do
  if grep -Fq "$PHRASE_JOINED" "$master_log"; then joined=1; break; fi
  if ! alive "$master_pid"; then
    echo "::error::smoke-cluster: master exited before the worker joined"; dump_logs; exit 1
  fi
  if ! alive "$worker_pid"; then
    echo "::error::smoke-cluster: worker exited before joining the cluster"; dump_logs; exit 1
  fi
  sleep 1
done

if [[ "$joined" == 1 ]]; then
  echo "smoke-cluster: OK -- worker joined; master observes 2 nodes in the cluster"
  grep -F "$PHRASE_JOINED" "$master_log" | tail -n1
  exit 0
fi
echo "::error::smoke-cluster: worker did not join the cluster within ${join_timeout}s"
if ! grep -Fq "nodes in the cluster" "$master_log"; then
  echo "::error::smoke-cluster: the master logged no 'N nodes in the cluster' line at all -- ClusterListener's readiness log may have changed; this test keys on it"
fi
dump_logs
exit 1
