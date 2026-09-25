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

// Unit tests for issue-claims.js, run by test_issue_claims.sh. Fixtures mirror
// the GraphQL nodes pr-assignment.yml fetches; the named cases replay the real
// incidents from #8676.

"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  CLAIM_MARKER,
  findClaimConflicts,
  renderClaimComment,
  creditChanges,
} = require("./issue-claims.js");

const REPO = "apache/texera";

function pr(number, author, { state = "OPEN", repo = REPO } = {}) {
  return {
    number,
    state,
    author: author === null ? null : { login: author },
    repository: { nameWithOwner: repo },
  };
}

function issue(number, { state = "OPEN", repo = REPO, assignees = [], prs = [] } = {}) {
  return {
    number,
    state,
    repository: { nameWithOwner: repo },
    assignees: { nodes: assignees.map((login) => ({ login })) },
    closedByPullRequestsReferences: { nodes: prs },
  };
}

const opened = (prNumber, opener) => ({ repo: REPO, prNumber, opener });

test("findClaimConflicts", async (t) => {
  await t.test("#8339 opening on #8149 flags the /take claimant and their PR", () => {
    // State when #8339 opened: the claimant is assigned and #8267 is open.
    const issues = [
      issue(8149, {
        assignees: ["Alwaysgaurav1", "aglinxinyuan"],
        prs: [pr(8267, "Alwaysgaurav1"), pr(8339, "aglinxinyuan")],
      }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(8339, "aglinxinyuan")), [
      {
        issue: 8149,
        claimants: [{ login: "Alwaysgaurav1", assigned: true, prs: [8267] }],
      },
    ]);
  });

  await t.test("an open PR is a claim without any assignee (#6674 / #8603)", () => {
    // GitHub silently dropped #8603's opener self-assign, so the issue has
    // no assignee and the open PR is the only trace of the claim.
    const issues = [
      issue(6674, {
        prs: [pr(6675, "Ma77Ball", { state: "CLOSED" }), pr(8603, "suyashj1231")],
      }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(9000, "someone-else")), [
      {
        issue: 6674,
        claimants: [{ login: "suyashj1231", assigned: false, prs: [8603] }],
      },
    ]);
  });

  await t.test("an assignee with no PR yet is a claim", () => {
    const issues = [issue(1, { assignees: ["claimer"] })];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), [
      { issue: 1, claimants: [{ login: "claimer", assigned: true, prs: [] }] },
    ]);
  });

  await t.test("the check is symmetric: the claimant's PR flags the later one", () => {
    const issues = [
      issue(8149, {
        assignees: ["Alwaysgaurav1", "aglinxinyuan"],
        prs: [pr(8267, "Alwaysgaurav1"), pr(8339, "aglinxinyuan")],
      }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(8267, "Alwaysgaurav1")), [
      {
        issue: 8149,
        claimants: [{ login: "aglinxinyuan", assigned: true, prs: [8339] }],
      },
    ]);
  });

  await t.test("the opener's own assignment and own PRs are not conflicts", () => {
    const issues = [
      issue(1, {
        assignees: ["opener"],
        prs: [pr(2, "opener"), pr(3, "opener")],
      }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), []);
  });

  await t.test("this PR itself is never a conflict, whoever GitHub lists as author", () => {
    const issues = [issue(1, { prs: [pr(2, "someone-else")] })];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), []);
  });

  await t.test("merged and closed PRs are not claims", () => {
    const issues = [
      issue(1, {
        prs: [pr(3, "a", { state: "MERGED" }), pr(4, "b", { state: "CLOSED" })],
      }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), []);
  });

  await t.test("closed issues are skipped", () => {
    const issues = [
      issue(1, { state: "CLOSED", assignees: ["claimer"], prs: [pr(3, "claimer")] }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), []);
  });

  await t.test("cross-repo issues and cross-repo PRs are skipped", () => {
    const issues = [
      issue(1, { repo: "other/repo", assignees: ["claimer"] }),
      issue(5, { prs: [pr(3, "claimer", { repo: "other/repo" })] }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), []);
  });

  await t.test("no closing issues, or unclaimed ones, flag nothing", () => {
    assert.deepEqual(findClaimConflicts([], opened(2, "opener")), []);
    assert.deepEqual(findClaimConflicts([issue(1)], opened(2, "opener")), []);
  });

  await t.test("an assigned claimant with several PRs is listed once", () => {
    const issues = [
      issue(1, { assignees: ["claimer"], prs: [pr(3, "claimer"), pr(4, "claimer")] }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), [
      { issue: 1, claimants: [{ login: "claimer", assigned: true, prs: [3, 4] }] },
    ]);
  });

  await t.test("claimants keep their order across several issues", () => {
    const issues = [
      issue(1, { assignees: ["b", "opener"], prs: [pr(4, "c"), pr(3, "b")] }),
      issue(5),
      issue(6, { prs: [pr(7, "d")] }),
    ];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), [
      {
        issue: 1,
        claimants: [
          { login: "b", assigned: true, prs: [3] },
          { login: "c", assigned: false, prs: [4] },
        ],
      },
      { issue: 6, claimants: [{ login: "d", assigned: false, prs: [7] }] },
    ]);
  });

  await t.test("a deleted account's PR is attributed to ghost", () => {
    const issues = [issue(1, { prs: [pr(3, null)] })];
    assert.deepEqual(findClaimConflicts(issues, opened(2, "opener")), [
      { issue: 1, claimants: [{ login: "ghost", assigned: false, prs: [3] }] },
    ]);
  });
});

test("renderClaimComment", async (t) => {
  await t.test("starts with the marker and lists one row per claimant", () => {
    const body = renderClaimComment([
      {
        issue: 8149,
        claimants: [{ login: "Alwaysgaurav1", assigned: true, prs: [8267] }],
      },
      {
        issue: 6674,
        claimants: [
          { login: "suyashj1231", assigned: false, prs: [8603, 8610] },
          { login: "claimer", assigned: true, prs: [] },
        ],
      },
    ]);
    assert.ok(body.startsWith(`${CLAIM_MARKER}\n`));
    const rows = body.split("\n").filter((l) => /^\| #\d/.test(l));
    assert.deepEqual(rows, [
      "| #8149 | @Alwaysgaurav1 (assignee) | #8267 |",
      "| #6674 | @suyashj1231 | #8603, #8610 |",
      "| #6674 | @claimer (assignee) | — |",
    ]);
  });

  await t.test("the marker appears exactly once", () => {
    const body = renderClaimComment([
      { issue: 1, claimants: [{ login: "a", assigned: true, prs: [] }] },
    ]);
    assert.equal(body.split(CLAIM_MARKER).length, 2);
  });
});

test("creditChanges", async (t) => {
  const merged = (prNumber, credited) => ({ repo: REPO, prNumber, credited });

  await t.test("#8339 merging keeps the #8149 claimant whose #8267 is still open", () => {
    const i = issue(8149, {
      assignees: ["Alwaysgaurav1", "aglinxinyuan"],
      prs: [pr(8339, "aglinxinyuan", { state: "MERGED" }), pr(8267, "Alwaysgaurav1")],
    });
    assert.deepEqual(creditChanges(i, merged(8339, ["aglinxinyuan"])), {
      current: ["Alwaysgaurav1", "aglinxinyuan"],
      toRemove: [],
      toAdd: [],
      kept: ["Alwaysgaurav1"],
    });
  });

  await t.test("an assignee without an open PR is replaced by the credited authors", () => {
    const i = issue(1, { assignees: ["triager"], prs: [pr(2, "author", { state: "MERGED" })] });
    assert.deepEqual(creditChanges(i, merged(2, ["author", "coauthor"])), {
      current: ["triager"],
      toRemove: ["triager"],
      toAdd: ["author", "coauthor"],
      kept: [],
    });
  });

  await t.test("a closed, merged, or cross-repo PR does not keep its author", () => {
    const i = issue(1, {
      assignees: ["a", "b", "c"],
      prs: [
        pr(3, "a", { state: "CLOSED" }),
        pr(4, "b", { state: "MERGED" }),
        pr(5, "c", { repo: "other/repo" }),
      ],
    });
    assert.deepEqual(creditChanges(i, merged(2, ["author"])).toRemove, ["a", "b", "c"]);
  });

  await t.test("the merged PR itself does not keep anyone, even if still listed OPEN", () => {
    // The merge event can race GitHub's own state update.
    const i = issue(1, { assignees: ["opener"], prs: [pr(2, "opener")] });
    assert.deepEqual(creditChanges(i, merged(2, ["author"])).toRemove, ["opener"]);
  });

  await t.test("an open PR by someone not assigned adds nobody", () => {
    const i = issue(1, { prs: [pr(3, "outsider")] });
    assert.deepEqual(creditChanges(i, merged(2, ["author"])), {
      current: [],
      toRemove: [],
      toAdd: ["author"],
      kept: [],
    });
  });

  await t.test("an issue already assigned to exactly the credited authors is a no-op", () => {
    const i = issue(1, { assignees: ["author"] });
    assert.deepEqual(creditChanges(i, merged(2, ["author"])), {
      current: ["author"],
      toRemove: [],
      toAdd: [],
      kept: [],
    });
  });
});
