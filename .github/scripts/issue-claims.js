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
// and `closedByPullRequestsReferences`, as the workflow's queries fetch it.

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

module.exports = { CLAIM_MARKER, findClaimConflicts, renderClaimComment, creditChanges };
