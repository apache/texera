#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

target_branch="${1:?target branch is required}"
commit_range="${2:?commit range is required}"
workspace_branch="ci-backport-${target_branch//\//-}"

git fetch --no-tags origin "${target_branch}"

mapfile -t commits < <(git rev-list --reverse "${commit_range}")

if [[ "${#commits[@]}" -eq 0 ]]; then
  echo "No commits found in range ${commit_range}" >&2
  exit 1
fi

git checkout -B "${workspace_branch}" "origin/${target_branch}"
git cherry-pick -x "${commits[@]}"
