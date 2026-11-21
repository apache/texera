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
labeler="$repo_root/.github/labeler.yml"

section() {
  awk -v label="$1" '
    $0 == label ":" { inside = 1; next }
    inside && /^[^[:space:]#][^:]*:$/ { exit }
    inside { print }
  ' "$labeler"
}

engine="$(section engine)"
pyamber="$(section pyamber)"

manifests=(
  amber/LICENSE-binary-java
  amber/NOTICE-binary
  amber/NOTICE-binary-python
)
for manifest in "${manifests[@]}"; do
  grep -Fq "'$manifest'" <<<"$engine" || {
    echo "FAIL: engine label does not cover $manifest" >&2
    exit 1
  }
done

grep -Fq "'amber/LICENSE-binary-python'" <<<"$pyamber" || {
  echo "FAIL: pyamber label does not cover amber/LICENSE-binary-python" >&2
  exit 1
}

if grep -Fq "'amber/LICENSE-binary-python'" <<<"$engine"; then
  echo "FAIL: Python-only manifest should not trigger the Scala engine stack" >&2
  exit 1
fi

echo "labeler manifest coverage tests passed"
