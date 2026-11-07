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

Scoped rules for `frontend/`, in addition to the repo-root [`AGENTS.md`](../AGENTS.md).

Start with [`README.md`](README.md) for commands and layout, and [`TESTING.md`](TESTING.md) before writing a test.

## Placement and ownership

- Keep React components, state and API clients beside their feature under `src/react/`.
- Keep framework-independent graph/types/numerical code under `src/core/` using its existing feature structure. React and shared-core changes run through the same production build.
- Do not hand-edit `src/core/common/type/proto/**`; regenerate it with `bin/frontend-proto-gen.sh` from the repository root.
- Preserve existing user-facing assertions when moving shared code. The retained graph, JointUI, notebook and heatmap specs are imported by the React runner's `legacy-*.spec.ts` entry points.

## Verification

- Tests come first, with both successful and rejected inputs, boundaries and recovery paths.
- Use Testing Library role/label queries for React behavior. Use real Chromium for SVG/pointer geometry and Monaco editing; keep browser specs named `*.browser.spec.tsx`.
- Close shared documents, sockets, subscriptions and uploads. Restore globals/timers after tests.
- Run `yarn format:ci`, `yarn test:ci`, `yarn test:browser`, `yarn build` and `yarn test:e2e` as appropriate to the change. Report exact commands and any unavailable live-backend validation.
- Visible frontend changes still need before/after screenshots or GIFs under the repository contribution rules.

The default development and production commands use Vite. `dist/index.html` and its static/version/license artifacts are deployment contracts; keep backend/Docker paths consistent.
