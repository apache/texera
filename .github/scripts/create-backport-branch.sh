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

# Builds and pushes the branch behind an auto-opened backport PR, used when the
# pre-merge backport check was red so a straight cherry-pick to the release
# branch is unsafe. The cherry-pick is committed even when it conflicts: the
# tree carries the conflict markers, and the human resolves them in the PR
# rather than starting the backport from scratch (the same approach the common
# backport bots take).
#
# Usage: create-backport-branch.sh <merge-sha> <target-branch> <pr-number>
# Writes to $GITHUB_OUTPUT (or stdout when unset): branch, version,
# had_conflict, conflict_files, subject.

set -euo pipefail

MERGE_SHA="${1:?merge sha is required}"
TARGET_BRANCH="${2:?target branch is required}"
PR_NUMBER="${3:?pr number is required}"

log() { printf '[open-backport-pr %s] %s\n' "${TARGET_BRANCH}" "$*"; }
out() { printf '%s\n' "$*" >> "${GITHUB_OUTPUT:-/dev/stdout}"; }

version="${TARGET_BRANCH#release/}"

# Human-readable slug from the squash subject: drop the trailing "(#N)" and the
# conventional-commit "type(scope): " prefix, then kebab-case and truncate.
subject="$(git log -1 --format=%s "${MERGE_SHA}")"
desc="$(printf '%s' "${subject}" \
  | sed -E 's/ \(#[0-9]+\)$//; s/^[a-z]+(\([^)]*\))?!?: *//')"
slug="$(printf '%s' "${desc}" \
  | tr '[:upper:]' '[:lower:]' \
  | tr -c 'a-z0-9' '-' \
  | sed -E 's/-+/-/g; s/^-//; s/-$//' \
  | cut -c1-40 \
  | sed -E 's/-$//')"
[[ -n "${slug}" ]] || slug="backport"
branch="backport/${PR_NUMBER}-${slug}-${version}"
log "branch=${branch}"

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

original_author="$(git log -1 --format='%an <%ae>' "${MERGE_SHA}")"
merge_message="$(git log -1 --format=%B "${MERGE_SHA}")"

git fetch --no-tags origin "${TARGET_BRANCH}"

# Feature-absent guard: if every file this commit modifies or deletes is
# missing on the target branch, the fix targets code that does not exist on
# the release (e.g. a fix for a main-only feature). Skip instead of opening a
# doomed PR. Added files don't count — a backportable fix may add files too.
preexisting="$(git diff-tree --no-commit-id --name-only -r --diff-filter=MD "${MERGE_SHA}")"
if [[ -n "${preexisting}" ]]; then
  any_present=0
  while IFS= read -r path; do
    [[ -z "${path}" ]] && continue
    if git cat-file -e "origin/${TARGET_BRANCH}:${path}" 2>/dev/null; then
      any_present=1
      break
    fi
  done <<< "${preexisting}"
  if [[ "${any_present}" -eq 0 ]]; then
    log "all modified files absent on ${TARGET_BRANCH}; feature not on release — skipping PR"
    out "feature_absent=true"
    out "version=${version}"
    exit 0
  fi
fi
out "feature_absent=false"

git checkout -B "${branch}" "origin/${TARGET_BRANCH}"

# --no-commit keeps full control of the message/author in both the clean and
# the conflicted case. On conflict the working tree keeps the markers; staging
# everything turns the unmerged entries into a normal (dirty) commit.
had_conflict=false
conflict_files=""
if ! git cherry-pick --no-commit "${MERGE_SHA}"; then
  had_conflict=true
  conflict_files="$(git diff --name-only --diff-filter=U | tr '\n' ' ')"
  log "cherry-pick conflicted in: ${conflict_files}"
  git add -A
fi
# Clear any cherry-pick sequencer state so the manual commit below is clean.
git cherry-pick --quit 2>/dev/null || true

new_message="$(printf '%s' "${merge_message}" \
  | python3 .github/scripts/compose-backport-message.py "${MERGE_SHA}")"
printf '%s\n' "${new_message}" | git commit -F - --author="${original_author}"

git push --force origin "HEAD:${branch}"
log "pushed ${branch}=$(git rev-parse HEAD)"

out "branch=${branch}"
out "version=${version}"
out "had_conflict=${had_conflict}"
out "conflict_files=${conflict_files}"
out "subject=${subject}"
