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

# Regression test for smoke-boot.sh's crash-detection regex. Pulls `crash_re`
# straight out of smoke-boot.sh (single source of truth -- no duplicated pattern
# that could drift) and checks it against fixture boot logs.
#
# Guards issue #6332: jOOQ prints a random "tip of the day" banner naming
# NoClassDefFoundError / ClassNotFoundException in prose, which must NOT read as
# a boot crash -- while real thrown linkage errors still must.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Load only the crash_re assignment line from smoke-boot.sh. Use command
# substitution (not `source <(...)`, which races the feeder process) so grep
# fully completes before eval runs. Input is our own controlled file.
eval "$(grep -E '^crash_re=' "$script_dir/smoke-boot.sh")"

if [[ -z "${crash_re:-}" ]]; then
  echo "FAIL: could not read crash_re from smoke-boot.sh" >&2
  exit 1
fi

rc=0
# assert_no_crash <description>   (boot log on stdin)
assert_no_crash() {
  if grep -qE "$crash_re"; then echo "FAIL: $1 -- matched crash_re"; rc=1; else echo "ok:   $1"; fi
}
# assert_crash <description>      (boot log on stdin)
assert_crash() {
  if grep -qE "$crash_re"; then echo "ok:   $1"; else echo "FAIL: $1 -- did not match crash_re"; rc=1; fi
}

# --- informational prose must NOT be treated as a crash (no random failures) ---
assert_no_crash "jOOQ tip of the day (#6332)" <<<"jOOQ tip of the day: A NoClassDefFoundError or ClassNotFoundException is often a sign that your jOOQ code is generated with a different version of jOOQ than runtime library you're using"
assert_no_crash "clean boot log" <<<"INFO org.eclipse.jetty.server.Server: jetty-11.0.20 started"
assert_no_crash "bare linkage-name mention without a fully-qualified type" <<<"DEBUG a NoSuchMethodError can occur when APIs drift"

# --- real thrown linkage failures must still be caught ---
assert_crash "thrown java.lang.NoClassDefFoundError" <<<"Exception in thread \"main\" java.lang.NoClassDefFoundError: com/fasterxml/jackson/databind/ObjectMapper"
assert_crash "Caused by java.lang.ClassNotFoundException" <<<"Caused by: java.lang.ClassNotFoundException: org.apache.hadoop.fs.FileSystem"
assert_crash "Jackson Databind version conflict (#6206)" <<<"com.fasterxml.jackson.module.scala.JsonScalaEnumeration requires Jackson Databind version >= 2.15 but found 2.14"

if [[ "$rc" -ne 0 ]]; then
  echo "smoke-boot regression tests FAILED"
  exit 1
fi
echo "smoke-boot regression tests passed"
