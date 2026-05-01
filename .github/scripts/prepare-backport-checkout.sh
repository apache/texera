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
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

if [[ "${commit_range}" != *..* ]]; then
  echo "Invalid commit range: ${commit_range}" >&2
  exit 1
fi
start_sha="${commit_range%..*}"
end_sha="${commit_range##*..}"

if [[ -z "$(git rev-list -n 1 "${commit_range}")" ]]; then
  echo "No commits found in range ${commit_range}" >&2
  exit 1
fi

# Build a single squash commit whose parent is the range start and whose tree
# matches the range end. Cherry-picking this squash onto the release branch
# applies the cumulative diff in one 3-way merge, which avoids spurious
# conflicts when intermediate commits in the range happen to overlap with
# changes already present (under different SHAs) on the release branch.
end_tree="$(git rev-parse "${end_sha}^{tree}")"
squash_sha="$(git commit-tree -p "${start_sha}" -m "ci: squashed backport of ${commit_range}" "${end_tree}")"

git checkout -B "${workspace_branch}" "origin/${target_branch}"
git cherry-pick -x "${squash_sha}"
