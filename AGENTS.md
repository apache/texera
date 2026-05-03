# AGENTS.md

## Architecture Map

Apache Texera: Scala/sbt backend services + the Amber workflow execution
engine, an Angular UI, and the agent service. JVM modules wired in
[`build.sbt`](build.sbt).

| Area | Path | Detail |
| --- | --- | --- |
| Workflow execution engine (Amber) | `amber/` | [amber/README.md](amber/README.md) |
| Backend services | `config-service/`, `access-control-service/`, `file-service/`, `computing-unit-managing-service/`, `workflow-compiling-service/` | `build.sbt` |
| Shared Scala libs | `common/` (`auth`, `config`, `dao`, `workflow-core`, `workflow-operator`, `pybuilder`) | `build.sbt` |
| Frontend (Angular) | `frontend/` | [frontend/README.md](frontend/README.md) |
| Agent service (Bun/TS, LLM agents) | `agent-service/` | `agent-service/package.json` |
| Pyright language service | `pyright-language-service/` | [pyright-language-service/README.md](pyright-language-service/README.md) |
| Deploy scripts / Dockerfiles | `bin/` | [README](bin/README.md) / [k8s](bin/k8s/README.md) / [single-node](bin/single-node/README.md) |
| DDL, sbt plugins | `sql/`, `project/` | files therein |

### Amber breakdown

| Path | Role |
| --- | --- |
| `amber/src/main/scala` | Pekko actors, scheduler, reconfiguration, fault tolerance, gRPC/proto |
| `amber/src/main/python/pyamber` | Python engine (`pyamber`) — bridge to the Scala engine |
| `amber/src/main/python/pytexera` | Python operator SDK exposed to UDFs |

## Where Things Live

| Topic | Source of truth |
| --- | --- |
| Contribution / PR / lint / format / testing / license header | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Reporting security issues | [SECURITY.md](SECURITY.md) |
| PR template | [.github/PULL_REQUEST_TEMPLATE](.github/PULL_REQUEST_TEMPLATE) |
| Issue templates | [bug](.github/ISSUE_TEMPLATE/bug-template.yaml) / [task](.github/ISSUE_TEMPLATE/task-template.yaml) / [feature](.github/ISSUE_TEMPLATE/feature-template.yaml) |
| License-header coverage; vendored `workflow-operator` | [.licenserc.yaml](.licenserc.yaml); [project/AddMetaInfLicenseFiles.scala](project/AddMetaInfLicenseFiles.scala) |
| Local single-node / k8s deploy | [single-node](bin/single-node/README.md), [k8s](bin/k8s/README.md) |

If a topic is above, **read that file** instead of asking here.

## Agent-Specific Rules

### Scope and safety

- Narrowly scoped changes. No unrelated rewrites or cross-service moves.
- `git status --short` before editing; don't revert unrelated dirty files.
- Never commit secrets / local config / build output / caches / binaries
  (`python_udf.conf`, `.env`, `target/`, `dist/`, `.pytest_cache/`,
  `.ruff_cache/`, logs).

### Develop in a worktree

Leave `texera/` on `main`. One worktree per PR, branched off a freshly
fetched `upstream/main`.

```
texera/                      # stays on main, never dirty
texera-worktrees/<branch>/   # one worktree per PR
```

Reset to `upstream/main` at start; `git log upstream/main..HEAD` should
contain only this PR's commits before pushing; remove the worktree after
merge.

### Environment

| Component | Version |
| --- | --- |
| Java | JDK 11 |
| Scala | 2.13 |
| Python | 3.12 |
| Node | 24 |

One Python venv shared across worktrees, sibling of the texera checkout:

```
<workspace>/
├── texera/                   # main checkout
├── texera-worktrees/<br>/    # per-PR worktrees
└── venv312/                  # shared Python 3.12 venv
```

```bash
python3.12 -m venv ../venv312 && source ../venv312/bin/activate
pip install -r amber/requirements.txt -r amber/operator-requirements.txt
```

Tests that spawn Python workers need an interpreter path. Edit `python.path`
in [`udf.conf`](common/config/src/main/resources/udf.conf) or
`export UDF_PYTHON_PATH="$(pwd)/../venv312/bin/python"` (env var overrides).
Without it, `sbt` Python-integration tests fail to launch a worker.

### Branch and commit naming

Short, **Conventional Commits**, same shape for branch and commit subject.

| Kind | Branch | Commit |
| --- | --- | --- |
| Feature | `feat/agent-workflow-edit` | `feat(agent-service): enable workflow edit` |
| Bug fix | `fix/marker-replay` | `fix(amber): marker replay during reconfiguration` |
| Tests | `test/pyamber-handlers` | `test(pyamber): add handler unit tests` |
| Chore | `chore/angular-21` | `chore(deps): upgrade frontend to Angular 21` |
| CI | `ci/cache-action-bump` | `ci: bump coursier/cache-action to v8.1.0` |

Both ≤ ~60 chars. For code changes, if you use a scope, use the module name
(`amber`, `pyamber`, `frontend`, `agent-service`, `file-service`, …) — not
`amber-python`. Use `chore(deps): ...` for dependency-only updates, and
`ci: ...` for CI-only changes. No `Co-authored-by:` trailer for the repo
owner.

### Issues and PRs

Issue-first; both stay short.

```
issue (template + Type)  ->  PR (Closes #N, template)  ->  review  ->  merge
```

- Every change starts as an issue (minor typo / docs excepted). File against
  `apache/texera`, never a fork.
- Pick the right template **and** set the GitHub Issue **Type** explicitly
  (`Bug` / `Task` / `Feature`); the template's `type:` frontmatter doesn't
  always apply on creation.
- Reference the issue: `Closes #N` (or `Fixes` / `Resolves`, or "related to").
- Issue titles are **plain prose**; never use the Conventional Commits
  format (`type(scope): ...`) — that prefix is for commit and PR titles only.
- Task issues match `task-template.yaml` exactly.
- Prefer **tables** and small **ASCII diagrams** over long bullets. Don't
  restate the diff or the template.
- For bugs, lead with **root cause** and a **before -> after** sketch:
  ```
  Before:  reconfiguration -> replay marker -> worker hangs
  After:   reconfiguration -> replay marker -> resume from checkpoint
  ```
- **Frontend PRs**: any visible UI change requires screenshots / GIF,
  **before / after** side by side. For purely visual fixes that's the
  primary verification under "How was this PR tested?"; interactive flows
  also list manual steps (click path, browser, viewport).

### Tests come first

TDD. Write the test before the source change.

```
write/adjust test (red)  ->  edit source (green)  ->  refactor
```

| Situation | Order |
| --- | --- |
| New feature / behavior change | Failing test, then implement. |
| Bug fix | Regression test reproducing the bug, then fix. |
| Code with **no tests** | **Characterization tests** pin current behavior first; only then change source. |
| Refactor (no behavior change) | Tests stay green throughout — no assertion edits. |

Every test must cover:

- **Both directions**: positive (valid → expected) **and** negative (invalid
  / error → specific failure mode).
- **Edge cases**: empty / null / zero / max / boundary, unicode,
  concurrency/order, missing or malformed config.
- **Don't assume valid.** External input (user / API / file / message) must
  be tested with bad input.

Don't claim "tested" without commands. Paste the exact `sbt testOnly` /
`pytest` / `yarn test:ci` / `bun test` invocation under "How was this PR
tested?".

### CI labels & gating

CI runs are **selected by PR labels**, not by file diff.

```
diff -> pr-labeler -> labels on PR -> required-checks maps labels to stacks -> CI runs
```

- Path → label rules: [`.github/labeler.yml`](.github/labeler.yml)
- Label → stacks (`LABEL_STACKS`, source of truth):
  [`.github/workflows/required-checks.yml`](.github/workflows/required-checks.yml).
  Read it directly; don't duplicate the mapping here.
- Need extra coverage the diff doesn't imply (e.g. a `common/` change you
  suspect breaks the frontend)? **Add the relevant label manually**.
- Empty stack union (docs-only / dev-only / `dependencies` / `feature` /
  `fix` / `refactor` / `release/*` only) skips every build stack on purpose.
- `release/*` labels select backport targets; removing one cancels that
  backport.
