# AGENTS.md — frontend

Scoped agent rules for `frontend/`. Loaded automatically on top of the repo-root [`AGENTS.md`](../AGENTS.md). Use [`README.md`](README.md) for commands and prerequisites; [`TESTING.md`](TESTING.md) for the testing reference.

## Stack

Angular (standalone components) · Vitest (unit tests, jsdom default; Playwright Chromium via `gui:test-browser` for SVG / pointer geometry) · `@angular/build` builder · Yarn (Berry, ships in-repo).

## Layout — where to look and where to put things

The tree is split by user-facing area, not by Angular role. New code goes next to its feature, never into a flat type-bucket.

| If you're touching…                                                             | Look in…                                                        |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| The workflow editor (operator graph, property panel, result panel, code editor) | `src/app/workspace/`                                            |
| The dashboard (workflows, datasets, projects, computing units, admin)           | `src/app/dashboard/`                                            |
| The public hub (discover and share workflows)                                   | `src/app/hub/`                                                  |
| Cross-cutting services, types, formly extensions, shared utils                  | `src/app/common/`                                               |
| Shared test infrastructure (`commonTestProviders`, mock GUI config service)     | `src/app/common/testing/`                                       |
| Operator metadata and the canonical `Stub…Service` doubles                      | `src/app/workspace/service/operator-metadata/`                  |
| Vitest configuration (jsdom default; browser mode)                              | `vitest.config.ts`, `vitest.browser.config.ts`                  |
| Per-spec inclusion / exclusion routing                                          | `angular.json` (`gui:test` and `gui:test-browser` targets)      |
| ProxyZone setup that makes `fakeAsync` work under Vitest                        | `src/test-zone-setup.ts`                                        |
| Generated protobuf TS (do not edit by hand)                                     | `src/app/common/type/proto/**`                                  |
| Vendored third-party formly type files (separate license)                       | `src/app/common/formly/{array,object,multischema,null}.type.ts` |

Placement rules:

- **Components, services, and types live next to their feature.** A new workspace service goes in `src/app/workspace/service/<feature>/`, not in a flat global bucket.
- **`Stub…Service` doubles live next to the real service** (`stub-operator-metadata.service.ts` sits alongside `operator-metadata.service.ts`). Specs import the stub by name; this keeps the mock surface consistent across the codebase.
- **Types shared across more than one feature area** go in `src/app/common/type/`. Types owned by one feature stay with that feature.

## Conventions

- **Components are standalone.** Declare them in `imports:`, never `declarations:` (the latter errors at compile time). The same applies inside `TestBed.configureTestingModule({...})`.
- **Run `yarn format:fix` before pushing**; `yarn format:ci` mirrors what CI runs. ESLint and Prettier are wired together via `prettier-eslint`.
- **Reuse shared test infrastructure** before inventing parallel one-off mocks. If a service already has a `Stub…Service`, extend the stub rather than ship a new partial mock from inside a spec.
- **Do not hand-edit the files listed as generated or vendored above** — protobuf TS is produced by codegen, and the formly type files come from upstream under a different license.

## Before writing or fixing a spec

Read [`TESTING.md`](TESTING.md) — the canonical testing reference for both humans and agents. It ships the recipes, anti-patterns, jsdom-vs-browser-mode decision, and coverage troubleshooting checklist. The three rules that surface most often in PR review:

1. Call `fixture.detectChanges()` at least once. Without it `.component.html` coverage stays at 0 % even when the spec passes.
2. Standalone components go in `imports:`, not `declarations:`.
3. `beforeEach` is `async () => { ... }`, not `waitForAsync(() => …)`.

## Pointers

- **Commands and prerequisites** (dev server, build, test, format): [`README.md`](README.md).
- **Testing reference** (recipes, anti-patterns, coverage troubleshooting, jsdom-vs-browser-mode decision): [`TESTING.md`](TESTING.md).
- **Repo-wide testing philosophy** ("Tests come first" — TDD, characterization tests, every test covers a specific failure mode): [`../AGENTS.md`](../AGENTS.md).
- **PR / commit / branch conventions** (Conventional Commits, fork-based workflow, license header check, the four-section PR template): [`../CONTRIBUTING.md`](../CONTRIBUTING.md).
- **Architecture map** (where the backend services live, what they own): [`../AGENTS.md`](../AGENTS.md) "Architecture Map".
- **Coverage dashboard** for this repo: [app.codecov.io/gh/apache/texera](https://app.codecov.io/gh/apache/texera).
- **Vitest browser-mode setup rationale**: [#5017](https://github.com/apache/texera/pull/5017).
