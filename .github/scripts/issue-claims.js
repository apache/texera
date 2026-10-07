// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Issue-claim decisions for .github/workflows/pr-assignment.yml. The workflow
// runs the GraphQL queries and makes every API call; these functions only
// decide, so test_issue_claims.sh can unit-test them. Each `issue` is a
// `closingIssuesReferences` node carrying `state`, `repository`, `assignees`
// and `closedByPullRequestsReferences`, as the workflow's queries fetch it;
// `comments` are the PR's REST issue comments.

"use strict";

const CLAIM_MARKER = "<!-- texera:issue-claim-conflict -->";

// Open same-repo PRs, other than `prNumber`, that also close `issue`, as
// author login -> [PR numbers]. A deleted account's PR has a null author,
// which GitHub displays as "ghost".
function otherOpenPrs(issue, repo, prNumber) {
  const byAuthor = new Map();
  for (const p of issue.closedByPullRequestsReferences.nodes) {
    if (p.state !== "OPEN" || p.number === prNumber || p.repository.nameWithOwner !== repo) {
      continue;
    }
    const login = p.author?.login ?? "ghost";
    byAuthor.set(login, [...(byAuthor.get(login) || []), p.number]);
  }
  return byAuthor;
}

// Open same-repo issues this PR closes that someone other than `opener` has
// claimed, either by being assigned or through an open PR of their own. The
// PR half is needed on its own: GitHub silently drops the opener self-assign
// for outside contributors who never commented on the issue, leaving their
// PR as the only trace of the claim.
// Returns [{ issue, claimants: [{ login, assigned, prs }] }].
function findClaimConflicts(issues, { repo, prNumber, opener }) {
  const conflicts = [];
  for (const issue of issues) {
    if (issue.state !== "OPEN" || issue.repository.nameWithOwner !== repo) continue;
    const claimants = new Map();
    for (const { login } of issue.assignees.nodes) {
      if (login !== opener) claimants.set(login, { login, assigned: true, prs: [] });
    }
    for (const [login, prs] of otherOpenPrs(issue, repo, prNumber)) {
      if (login !== opener) claimants.set(login, { login, assigned: claimants.has(login), prs });
    }
    if (claimants.size) conflicts.push({ issue: issue.number, claimants: [...claimants.values()] });
  }
  return conflicts;
}

function renderClaimComment(conflicts) {
  const rows = conflicts.flatMap(({ issue, claimants }) =>
    claimants.map(({ login, assigned, prs }) => {
      const who = `@${login}${assigned ? " (assignee)" : ""}`;
      return `| #${issue} | ${who} | ${prs.map((n) => `#${n}`).join(", ") || "—"} |`;
    }),
  );
  return [
    CLAIM_MARKER,
    "### Linked issue already claimed",
    "",
    "This PR closes an issue that someone else has already claimed:",
    "",
    "| Issue | Claimed by | Their open PR |",
    "| --- | --- | --- |",
    ...rows,
    "",
    "Please check with them before this merges. Merging closes the issue, and any " +
      "work they have in progress is left with nothing to fix.",
    "",
    "- If this PR takes over, say so here. Credit any of their work it includes, for " +
      "example with a `Co-authored-by:` trailer.",
    "- If their claim is stale, they can release it with `/untake`, or a maintainer " +
      "can unassign them.",
    "- Otherwise, remove that issue's closing keyword from this PR's description.",
    "",
    "_Refreshed whenever this PR is edited; deleted once nothing here applies._",
  ].join("\n");
}

// Assignee changes for `issue` when PR `prNumber` merges: the merged PR's
// authors (`credited`) replace the current assignees, except that anyone who
// still has their own open PR on the issue stays assigned. Unassigning them
// would hide a claim that is still in review (#8149 lost its /take claimant
// this way). If that PR later closes unmerged, the close-without-merge step
// unassigns them.
function creditChanges(issue, { repo, prNumber, credited }) {
  const current = issue.assignees.nodes.map((n) => n.login);
  const stillOpen = otherOpenPrs(issue, repo, prNumber);
  const uncredited = current.filter((l) => !credited.includes(l));
  return {
    current,
    toRemove: uncredited.filter((l) => !stillOpen.has(l)),
    toAdd: credited.filter((l) => !current.includes(l)),
    kept: uncredited.filter((l) => stillOpen.has(l)),
  };
}

// What to do with the PR's comments so that exactly one claim comment holding
// `body` is left, or none when `body` is null. Overlapping opened/edited runs
// can each post a copy before seeing the other's, so every run keeps the
// oldest copy and deletes the rest, and racing runs settle on the same one.
// Returns { create, update: comment id or null, remove: [comment ids] }.
function claimCommentChanges(comments, body) {
  const ours = comments
    .filter((c) => c.user?.type === "Bot" && (c.body || "").includes(CLAIM_MARKER))
    .sort((a, b) => a.id - b.id);
  if (body === null) return { create: false, update: null, remove: ours.map((c) => c.id) };
  const [keep, ...extras] = ours;
  return {
    create: !keep,
    update: keep && keep.body !== body ? keep.id : null,
    remove: extras.map((c) => c.id),
  };
}

// Bring the PR's claim comment in line with `readBody()`, the comment body for
// the live PR state (null when nothing is claimed). Runs for opened/edited
// events overlap, and a run can read the state before an edit and write after
// a newer run has finished, so after any write it reads the state again and
// repeats until a pass changes nothing. The run that writes last then also
// reads last, after every edit, and corrects a stale write. `skipIfClear`
// skips listing comments when a first read finds nothing to flag (a new PR
// has no claim comment yet). REST calls go through the injected functions;
// write failures are logged and the pass goes on.
async function syncClaimComment({
  readBody,
  listComments,
  createComment,
  updateComment,
  deleteComment,
  log,
  skipIfClear = false,
  maxPasses = 3,
}) {
  let posted = false;
  for (let pass = 1; pass <= maxPasses; pass++) {
    const body = await readBody();
    if (pass === 1 && body === null && skipIfClear) {
      log.info("No linked issue is claimed by someone else.");
      return;
    }
    let changes;
    try {
      changes = claimCommentChanges(await listComments(), body);
    } catch (e) {
      // Without the comment list we can't safely de-dupe; bail to avoid
      // posting a second copy.
      log.warning(`Listing comments failed: ${e.message}`);
      return;
    }
    // Never post twice: the list can lag behind our own comment.
    if (posted) changes.create = false;
    if (!changes.create && !changes.update && !changes.remove.length) {
      log.info(body ? "Claim comment is current." : "No claim comment needed.");
      return;
    }
    try {
      if (changes.create) {
        await createComment(body);
        posted = true;
        log.info("Posted claim comment.");
      }
      if (changes.update) {
        await updateComment(changes.update, body);
        log.info(`Updated claim comment ${changes.update}.`);
      }
    } catch (e) {
      log.warning(`Writing the claim comment failed: ${e.message}`);
    }
    for (const id of changes.remove) {
      try {
        await deleteComment(id);
        log.info(`Deleted claim comment ${id}.`);
      } catch (e) {
        // A racing run may have deleted it first.
        log.warning(`Deleting claim comment ${id} failed: ${e.message}`);
      }
    }
  }
  log.warning(`Claim comment still changing after ${maxPasses} passes; the next edit refreshes it.`);
}

module.exports = {
  CLAIM_MARKER,
  findClaimConflicts,
  renderClaimComment,
  creditChanges,
  claimCommentChanges,
  syncClaimComment,
};
