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
// the GraphQL nodes and REST issue comments pr-assignment.yml fetches; the
// named cases replay the real incidents from #8676.

"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  CLAIM_MARKER,
  findClaimConflicts,
  renderClaimComment,
  creditChanges,
  claimCommentChanges,
  syncClaimComment,
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

test("claimCommentChanges", async (t) => {
  const comment = (id, body, type = "Bot") => ({ id, body, user: { type } });
  const BODY = renderClaimComment([
    { issue: 1, claimants: [{ login: "a", assigned: true, prs: [] }] },
  ]);
  const STALE = `${CLAIM_MARKER}\nold text`;
  const none = { create: false, update: null, remove: [] };

  await t.test("no claim comment yet: create one", () => {
    assert.deepEqual(claimCommentChanges([], BODY), { create: true, update: null, remove: [] });
  });

  await t.test("a current claim comment is left alone", () => {
    assert.deepEqual(claimCommentChanges([comment(5, BODY)], BODY), none);
  });

  await t.test("a stale claim comment is rewritten in place", () => {
    assert.deepEqual(claimCommentChanges([comment(5, STALE)], BODY), {
      create: false,
      update: 5,
      remove: [],
    });
  });

  await t.test("copies posted by racing runs keep the oldest, whatever the list order", () => {
    assert.deepEqual(claimCommentChanges([comment(9, BODY), comment(5, BODY)], BODY), {
      create: false,
      update: null,
      remove: [9],
    });
    assert.deepEqual(claimCommentChanges([comment(9, BODY), comment(5, STALE)], BODY), {
      create: false,
      update: 5,
      remove: [9],
    });
  });

  await t.test("nothing to flag: every copy is removed", () => {
    assert.deepEqual(claimCommentChanges([comment(9, BODY), comment(5, STALE)], null), {
      create: false,
      update: null,
      remove: [5, 9],
    });
  });

  await t.test("nothing to flag and no copy: no-op", () => {
    assert.deepEqual(claimCommentChanges([], null), none);
  });

  await t.test("human comments, unmarked bot comments and empty bodies are never ours", () => {
    const others = [
      comment(1, `quoting ${CLAIM_MARKER} in a reply`, "User"),
      comment(2, "<!-- texera:template-compliance -->\nunrelated bot note"),
      comment(3, null),
      { id: 4, body: CLAIM_MARKER, user: null },
    ];
    assert.deepEqual(claimCommentChanges(others, BODY), { create: true, update: null, remove: [] });
    assert.deepEqual(claimCommentChanges(others, null), none);
  });
});

// An in-memory PR conversation with the REST calls syncClaimComment makes.
// `lag` hides comments from listComments, like a list that hasn't caught up.
function fakeConversation({ lag = new Set() } = {}) {
  const comments = [];
  let nextId = 100;
  const warnings = [];
  const api = {
    listComments: async () => comments.filter((c) => !lag.has(c.id)).map((c) => ({ ...c })),
    createComment: async (body) => {
      const c = { id: nextId++, body, user: { type: "Bot" } };
      comments.push(c);
      return c.id;
    },
    updateComment: async (id, body) => {
      const c = comments.find((x) => x.id === id);
      if (!c) throw new Error(`comment ${id} not found`);
      c.body = body;
    },
    deleteComment: async (id) => {
      const i = comments.findIndex((x) => x.id === id);
      if (i < 0) throw new Error(`comment ${id} not found`);
      comments.splice(i, 1);
    },
    log: { info: () => {}, warning: (m) => warnings.push(m) },
  };
  return { comments, api, warnings };
}

test("syncClaimComment", async (t) => {
  const claim = (login) =>
    renderClaimComment([{ issue: 1, claimants: [{ login, assigned: true, prs: [] }] }]);
  const OLD = claim("old-claimant");
  const NEW = claim("new-claimant");
  const bodies = (conv) => conv.comments.map((c) => c.body);

  await t.test("posts the claim comment once and stops when it is current", async () => {
    const conv = fakeConversation();
    let reads = 0;
    await syncClaimComment({ ...conv.api, readBody: async () => (reads++, NEW) });
    assert.deepEqual(bodies(conv), [NEW]);
    assert.equal(reads, 2, "re-reads the state once after writing, then stops");
  });

  await t.test("a current comment is left alone after one read", async () => {
    const conv = fakeConversation();
    await conv.api.createComment(NEW);
    let reads = 0;
    await syncClaimComment({ ...conv.api, readBody: async () => (reads++, NEW) });
    assert.deepEqual(bodies(conv), [NEW]);
    assert.equal(reads, 1);
  });

  await t.test("a delayed run does not leave a warning the newer run removed", async () => {
    // Run A reads the state before an edit drops the closing keyword; run B
    // reads it after, finds nothing to flag, and finishes before A writes.
    const conv = fakeConversation();
    let state = OLD;
    let aReads = 0;
    await syncClaimComment({
      ...conv.api,
      readBody: async () => {
        if (aReads++ > 0) return state;
        const seen = state;
        state = null;
        await syncClaimComment({ ...conv.api, readBody: async () => state });
        return seen;
      },
    });
    assert.deepEqual(bodies(conv), []);
  });

  await t.test("a delayed run does not overwrite the newer run's text", async () => {
    // Run B posts NEW between run A reading OLD and A writing it.
    const conv = fakeConversation();
    let state = OLD;
    let aReads = 0;
    await syncClaimComment({
      ...conv.api,
      readBody: async () => {
        if (aReads++ > 0) return state;
        const seen = state;
        state = NEW;
        await syncClaimComment({ ...conv.api, readBody: async () => state });
        return seen;
      },
    });
    assert.deepEqual(bodies(conv), [NEW]);
  });

  await t.test("never posts twice when the re-list lags behind its own comment", async () => {
    const conv = fakeConversation({ lag: new Set([100]) });
    await syncClaimComment({ ...conv.api, readBody: async () => NEW });
    assert.deepEqual(bodies(conv), [NEW]);
  });

  await t.test("skipIfClear: nothing to flag means no list and no writes", async () => {
    const conv = fakeConversation();
    let listed = false;
    await syncClaimComment({
      ...conv.api,
      listComments: async () => ((listed = true), []),
      readBody: async () => null,
      skipIfClear: true,
    });
    assert.equal(listed, false);
  });

  await t.test("a failed list bails without writing", async () => {
    const conv = fakeConversation();
    await syncClaimComment({
      ...conv.api,
      listComments: async () => {
        throw new Error("boom");
      },
      readBody: async () => NEW,
    });
    assert.deepEqual(bodies(conv), []);
    assert.equal(conv.warnings.length, 1);
  });

  await t.test("a copy a racing run already deleted is only a warning", async () => {
    const conv = fakeConversation();
    await conv.api.createComment(NEW);
    await conv.api.createComment(NEW);
    const deleteComment = async (id) => {
      await conv.api.deleteComment(id);
      throw new Error("already gone");
    };
    await syncClaimComment({ ...conv.api, deleteComment, readBody: async () => NEW });
    assert.deepEqual(bodies(conv), [NEW]);
  });

  await t.test("gives up after maxPasses if the state keeps changing", async () => {
    const conv = fakeConversation();
    let reads = 0;
    await syncClaimComment({
      ...conv.api,
      readBody: async () => (reads++ % 2 ? OLD : NEW),
      maxPasses: 3,
    });
    assert.equal(reads, 3);
    assert.equal(conv.comments.length, 1);
    assert.match(conv.warnings.at(-1), /3 passes/);
  });
});
