<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# AGENTS.md — frontend

Scoped agent rules for `frontend/`. Loaded automatically on top of the repo-root [`AGENTS.md`](../AGENTS.md).

**Start at [`README.md`](README.md)** for the stack, prerequisites, common commands, the testing entry-point, and the project layout. This file only carries what is not already in the README — placement rules, agent-relevant conventions, the testing checklist, and deeper pointers.

## Placement rules

The layout table in [`README.md`](README.md) tells you where things live; these rules tell you where new things go.

- **Components, services, and types live next to their feature.** A new workspace service goes in `src/app/workspace/service/<feature>/`, not in a flat global bucket.
- **`Stub…Service` doubles live next to the real service** (`stub-operator-metadata.service.ts` sits alongside `operator-metadata.service.ts`). Specs import the stub by name; this keeps the mock surface consistent across the codebase.
- **Types shared across more than one feature area** go in `src/app/common/type/`. Types owned by a single feature stay with that feature.
- **Do not hand-edit codegen or vendored files:**
  - `src/app/common/type/proto/**` — generated protobuf TypeScript.
  - `src/app/common/formly/{array,object,multischema,null}.type.ts` — vendored upstream under a separate license.

## Conventions

- **Components are standalone.** Declare them in `imports:`, never `declarations:` (the latter errors at compile time). The same applies inside `TestBed.configureTestingModule({...})`.
- **Run `yarn format:fix` before pushing**; `yarn format:ci` mirrors what CI runs. ESLint and Prettier are wired together via `prettier-eslint`.
- **Reuse shared test infrastructure** before inventing parallel one-off mocks. If a service already has a `Stub…Service`, extend the stub rather than ship a new partial mock from inside a spec.

## Before writing or fixing a spec

Read [`TESTING.md`](TESTING.md) — the canonical testing reference for both humans and agents. It ships the recipes, anti-patterns, jsdom-vs-browser-mode decision, and coverage troubleshooting checklist. The three rules that surface most often in PR review:

1. Call `fixture.detectChanges()` at least once. Without it `.component.html` coverage stays at 0 % even when the spec passes.
2. Standalone components go in `imports:`, not `declarations:`.
3. `beforeEach` is `async () => { ... }`, not `waitForAsync(() => …)`.

## Pointers

- **Repo-wide testing philosophy** ("Tests come first" — TDD, characterization tests, every test covers a specific failure mode): [`../AGENTS.md`](../AGENTS.md) §"Tests come first".
- **Architecture map** (where the backend services live, what they own): [`../AGENTS.md`](../AGENTS.md) §"Architecture Map".
- **Coverage dashboard**: [app.codecov.io/gh/apache/texera](https://app.codecov.io/gh/apache/texera).
- **Vitest browser-mode setup rationale**: [#5017](https://github.com/apache/texera/pull/5017).
