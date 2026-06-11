#!/usr/bin/env bash
#
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
#
# Single entry-point for all Texera benchmarks. CI calls this script
# verbatim — it does NOT reference individual benchmark main classes.
# Adding a new benchmark (e.g., a JMH suite) means appending one block
# to this script; no CI workflow change.
#
# Output convention: every benchmark writes to bench-results/ with a
# self-describing filename suffix that matches the github-action-benchmark
# `tool` parameter expected by the publish step in build.yml:
#   bench-results/<bench>-throughput.json  → tool: customBiggerIsBetter
#   bench-results/<bench>-latency.json     → tool: customSmallerIsBetter
#   bench-results/<bench>-jmh.json         → tool: jmh
# CSV / log / debug files may live alongside; the publish matrix only
# cares about the *.json files declared in build.yml.
#
# Env vars honored:
#   BENCH_NUM_BATCHES — passes through to the e2e bench (default 100).
#                       Lower for fast PR runs; higher for stable nightlies.
#   UDF_PYTHON_PATH   — Python executable for the spawned worker subprocess.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

mkdir -p bench-results

echo "=== run-benchmarks: arrow-flight-e2e ==="
sbt --error \
  "WorkflowExecutionService/Test/runMain org.apache.texera.amber.bench.ArrowFlightActorBench"

# Future benchmarks: add new blocks below. Each block should self-contain
# the run command and ensure its outputs land in bench-results/. Example
# for a future JMH suite:
#   echo "=== run-benchmarks: arrow-utils-jmh ==="
#   sbt "WorkflowExecutionService/Jmh/run -rf json -rff $REPO_ROOT/bench-results/arrow-utils-jmh.json"

echo
echo "=== bench artifacts ==="
ls -la bench-results/
