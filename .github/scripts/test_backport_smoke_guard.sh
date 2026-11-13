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

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workflow="$repo_root/.github/workflows/build.yml"

result="$(
  awk '
    /^[[:space:]]*- name:/ { guarded = 0 }
    /if:.*hashFiles\('\''.github\/scripts\/smoke-boot.sh'\''\).*!= '\'''\''/ {
      guarded = 1
    }
    /run: .github\/scripts\/smoke-boot.sh/ {
      calls += 1
      if (guarded) guarded_calls += 1
    }
    END { printf "%d:%d", calls, guarded_calls }
  ' "$workflow"
)"

if [[ "$result" != "3:3" ]]; then
  echo "FAIL: expected all three smoke-boot steps to skip when the script is absent; got $result" >&2
  exit 1
fi

echo "backport smoke-boot guard tests passed"
