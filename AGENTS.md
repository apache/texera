# AGENTS.md

## Architecture Map

Apache Texera is a multi-language monorepo: a set of Scala/sbt backend services
plus the Amber workflow execution engine, fronted by an Angular UI and Agent
TypeScript services. JVM modules are wired in [`build.sbt`](build.sbt).

| Area | Path | Detail |
| --- | --- | --- |
| Workflow execution engine (Amber) | `amber/` | [amber/README.md](amber/README.md) |
| Config service | `config-service/` | `build.sbt` (`ConfigService`) |
| Access control service | `access-control-service/` | `build.sbt` (`AccessControlService`) |
| File service | `file-service/` | `build.sbt` (`FileService`) |
| Computing-unit managing service | `computing-unit-managing-service/` | `build.sbt` (`ComputingUnitManagingService`) |
| Workflow compiling service | `workflow-compiling-service/` | `build.sbt` (`WorkflowCompilingService`) |
| Shared Scala libs | `common/` (`auth`, `config`, `dao`, `workflow-core`, `workflow-operator`, `pybuilder`) | `build.sbt` |
| Frontend (Angular) | `frontend/` | [frontend/README.md](frontend/README.md) |
| Agent service (Bun/TS, LLM agents) | `agent-service/` | `agent-service/package.json` |
| Pyright language service | `pyright-language-service/` | [pyright-language-service/README.md](pyright-language-service/README.md) |
| Deploy scripts / Dockerfiles | `bin/` | [bin/README.md](bin/README.md), [bin/k8s/README.md](bin/k8s/README.md), [bin/single-node/README.md](bin/single-node/README.md) |
| DDL | `sql/` | files therein |
| sbt build plugins | `project/` | files therein |

### Amber breakdown

| Path | Role |
| --- | --- |
| `amber/src/main/scala` | Pekko actors (controller/worker), scheduler, reconfiguration, fault tolerance, gRPC/proto |
| `amber/src/main/python/pyamber` | Python engine (pyamber) — bridge to the Scala engine |
| `amber/src/main/python/pytexera` | Python operator SDK exposed to user UDFs |

## Where Things Live

| Topic | Source of truth |
| --- | --- |
| Contribution steps, PR/commit conventions, lint/format/testing, license header | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Reporting security issues | [SECURITY.md](SECURITY.md) |
| PR template (sections to fill) | [.github/PULL_REQUEST_TEMPLATE](.github/PULL_REQUEST_TEMPLATE) |
| Issue templates | [.github/ISSUE_TEMPLATE/bug-template.yaml](.github/ISSUE_TEMPLATE/bug-template.yaml), [task-template.yaml](.github/ISSUE_TEMPLATE/task-template.yaml), [feature-template.yaml](.github/ISSUE_TEMPLATE/feature-template.yaml) |
| License-header coverage rules | [.licenserc.yaml](.licenserc.yaml) |
| Vendored-license handling for `workflow-operator` | [project/AddMetaInfLicenseFiles.scala](project/AddMetaInfLicenseFiles.scala) |
| Local single-node and k8s deployment | [bin/single-node/README.md](bin/single-node/README.md), [bin/k8s/README.md](bin/k8s/README.md) |

If a topic is covered above, **read that file** instead of asking here.

## Agent-Specific Rules

Constraints that aren't in CONTRIBUTING.md — agent behavior, not project policy.

### Scope and safety

- Keep changes narrowly scoped. No unrelated rewrites or cross-service moves
  unless the task asks for it.
- Check `git status --short` before editing; don't revert unrelated dirty files.
- Never commit secrets, local config, generated build output, caches, or
  binaries (`python_udf.conf`, `.env`, `target/`, `dist/`, `.pytest_cache/`,
  `.ruff_cache/`, local logs).

### Develop in a worktree

Leave the primary `texera/` checkout on `main`. Do all work in a fresh
`git worktree` per PR, branched off a freshly fetched `upstream/main`.

```
texera/                      # stays on main, never dirty
texera-worktrees/<branch>/   # one worktree per PR
```

- One worktree per iteration; reset to `upstream/main` at the start.
- Verify before pushing: `git log upstream/main..HEAD` should contain only
  this PR's commits.
- Remove the worktree after the PR merges.

### Branch and commit naming

Short, **Conventional Commits**. Same shape for branch and commit subject.

| Kind | Example branch | Example commit |
| --- | --- | --- |
| Feature | `feat/agent-workflow-edit` | `feat(agent-service): enable workflow edit` |
| Bug fix | `fix/marker-replay` | `fix(amber): marker replay during reconfiguration` |
| Tests | `test/pyamber-handlers` | `test(pyamber): add handler unit tests` |
| Chore | `chore/angular-21` | `chore(deps): upgrade frontend to Angular 21` |
| CI | `ci/cache-action-bump` | `ci: bump coursier/cache-action to v8.1.0` |

- Keep both under ~60 chars where possible; the body explains the rest.
- Scope matches the module (`amber`, `pyamber`, `frontend`, `agent-service`,
  `file-service`, …) — not `amber-python`.
- Don't add a `Co-authored-by:` trailer for the repo owner.

### Issues and PRs

Issue-first; both stay short.

```
issue (template + Type)  ->  PR (Closes #N, template)  ->  review  ->  merge
```

- Every change starts as an issue (minor typo, docs excepted). File against
  `apache/texera`, never a fork.
- Pick the right template **and** set the GitHub Issue **Type** (`Bug`,
  `Task`, `Feature`) — the template's `type:` frontmatter does not always
  apply on creation; set it explicitly.
- Reference the issue from the PR with `Closes #N` (or `Fixes` / `Resolves`,
  or just "related to").

Style for both:

- Short prose. Prefer **tables** and small **ASCII diagrams** (e.g.
  `A -> B -> C`, before/after blocks) over long bullet lists. Don't restate
  the diff or the template.
- Issue titles are **plain prose** — Conventional Commits is for commits and
  PR titles only.
- Task issues match `task-template.yaml` exactly: Task Summary + Task Type.
  No priority, no proposed next step, no code blocks.
- For bugs, lead with **root cause** and a **before -> after** sketch:
  ```
  Before:  reconfiguration -> replay marker -> worker hangs
  After:   reconfiguration -> replay marker -> resume from checkpoint
  ```

Frontend PRs — screenshots required. Any change with visible UI impact must
include screenshots (or a short GIF), **before / after** side by side
whenever possible:

| Before | After |
| --- | --- |
| ![before](url) | ![after](url) |

For purely visual fixes this is the primary verification — say so under
"How was this PR tested?". For interactive flows also list manual steps
(click path, browser, viewport).

### Tests come first

Test-driven. Write the test before the source change.

```
write/adjust test (red)  ->  edit source (green)  ->  refactor
```

| Situation | Order |
| --- | --- |
| New feature or behavior change | Write failing test, then implement. |
| Bug fix | Write a regression test that reproduces the bug, then fix. |
| Touching code with **no tests** | Add **characterization tests** that pin current behavior first; commit those (or include in same PR). Only then change source and update tests. |
| Refactor (no behavior change) | Tests stay green throughout — no edits to assertions. |

Coverage requirements for every test added:

- **Both directions**: positive (valid input → expected result) **and**
  negative (invalid input / error path → specific failure mode).
- **Edge cases**: empty / null / zero / max / boundary, unicode,
  concurrency/order, missing or malformed config.
- **Don't assume valid.** Never write tests that only exercise the happy
  path. If the code accepts external input (user, API, file, message), test
  what happens when it's wrong.
- For known bugs you don't fix in this PR: pin the actual buggy behavior
  with an explanatory comment, **and** add a `@pytest.mark.xfail(strict=True)`
  (or equivalent) test for the intended contract.

Don't claim "tested" without commands. Under the PR's "How was this PR
tested?" section, paste the exact `sbt testOnly` / `pytest` / `yarn test:ci`
/ `bun test` invocation.

### CI labels & gating

CI runs are **selected by PR labels**, not by file diff.

```
diff -> pr-labeler -> labels on PR -> required-checks maps labels to stacks -> CI runs
```

- Path → label rules: [`.github/labeler.yml`](.github/labeler.yml)
- Label → stacks map (`LABEL_STACKS`, source of truth):
  [`.github/workflows/required-checks.yml`](.github/workflows/required-checks.yml)

Stacks are `frontend`, `scala`, `python`, `agent-service`. Read
`LABEL_STACKS` for the current mapping — do not duplicate it here.

Rules of thumb:

- Don't fight the auto-labels. If the labeler missed something, **add** the
  label rather than editing the workflow.
- Need extra coverage the diff doesn't imply (e.g. a `common/` change you
  suspect breaks the frontend)? **Add the relevant label manually** — e.g.
  add `frontend` to also run the frontend stack.
- Empty stack union (docs-only / dev-only / `dependencies` / `feature` /
  `fix` / `refactor` / `release/*` only) skips every build stack on
  purpose; that's correct, not a bug to "fix".
- `release/*` labels select backport targets; removing one cancels that
  backport.
