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

# Runs the unit tests for issue-claims.js, the issue-claim logic behind
# pr-assignment.yml. The infra CI job discovers this file by its test_*.sh
# name; the cases themselves live in issue-claims.test.js and use Node's
# built-in test runner, which the ubuntu and macOS runner images ship with.

set -euo pipefail

command -v node >/dev/null || { echo "node is required to run these tests" >&2; exit 1; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
node --test "$script_dir/issue-claims.test.js"
