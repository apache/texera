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

# Frontend testing guide

Tests come first: reproduce a failure, implement the behavior, then refactor. Preserve user-facing assertions when retaining existing graph and execution code. Include valid input, rejected input, empty/boundary values and failure recovery.

## Runners

| Runner | Purpose | Command |
| --- | --- | --- |
| Vitest + jsdom | API contracts, stores, schema forms and page interactions | `yarn test` |
| Vitest + Chromium | Actual SVG layout, virtual file-tree geometry and Monaco editing | `yarn test:browser` |
| Node test runner | Vite assets, route fallback, worker licenses and build integration | `yarn test:tools` |
| Production browser harness | Workspace editing/execution and independent collaborative browser contexts | `yarn build && yarn test:e2e` |

Unit configuration is `vitest.react.config.mts`; it selects `src/react/**/*.spec.{ts,tsx}`, the version tests and build-version tests, and excludes browser suites. `legacy-*.spec.ts` entry points execute the retained `src/core` behavior assertions without copying their tests into a second runner. Browser configuration is `vitest.browser.config.mts`, selecting only `*.browser.spec.{ts,tsx}`. Each runner produces its own JUnit report (`junit.xml`, `junit-browser.xml`).

```bash
yarn test src/react/resources/storage-api.spec.ts
yarn test src/react/workspace/workflow-session.spec.ts
yarn test:browser src/react/code-editor/workspace.browser.spec.tsx
yarn test:ci
```

Coverage uses V8 and includes the React application plus the shared core; lcov is written under `coverage/react`. Production bootstrap and generated protobuf files are excluded from coverage instrumentation. CI runs unit, Chromium, tooling and production browser checks on the supported operating systems.

## React interactions

Use Testing Library queries by role or label and assert what a reader can see or do. Drive the real component, including loading, empty, failed and retry states. Keep schema input validation, resource access checks and navigation guards active in tests.

Use `findBy*` or `waitFor` for asynchronous state. Wrap external events such as a simulated socket message, timer advancement or a real Playwright click in `act` when they update React. Clear fake timers and restore globals after the test. Close sockets, shared documents, subscriptions and upload requests during cleanup.

For transport tests, inject `ApiClient`'s fetch seam and assert the complete endpoint, method, query and request body. Test malformed responses and HTTP failures. Signed object-storage downloads must not receive the application bearer token. File upload tests retain the original resumable multipart assertions and verify the React upload controls separately.

## Real browsers

Install Chromium with `yarn playwright install chromium`. Browser tests import `page` from `vitest/browser`; the runner has the Monaco stylesheet/WASM integration and the minimal browser Buffer setup. Dependency optimization includes the protocol CommonJS entry points, while Monaco's constructable stylesheet modules stay outside prebundling. Shared Yjs modules are deduplicated.

Use browser mode for actual geometry, pointer hit testing and Monaco's native input surface. Avoid replacing those surfaces with jsdom geometry stubs. The resource tree suite verifies a bounded window for large trees, fixed row geometry, scrolling and hidden/empty recovery. The editor suite uses actual Monaco keyboard editing and shared Y.Text convergence.

`tools/workspace-browser.mjs` serves the production bundle with deterministic HTTP fixtures and an actual local y-websocket relay. It exercises independent browser contexts rather than placing two editors in one document. Set `TEXERA_FRONTEND_DIST` to override the bundle directory and `TEXERA_BROWSER_ARTIFACTS` to redirect screenshots/reports (default `playwright-report`). These checks do not replace verification against a deployed backend, production identity providers or real object storage.

## Build and licensing checks

```bash
yarn build
yarn test:tools
python -X utf8 ../bin/licensing/check_binary_deps.py npm dist/3rdpartylicenses.json
```

Build contracts preserve static assets, association files, version consistency, deep links, service proxy boundaries and worker-only license entries. When adding a runtime dependency, inspect the emitted license manifest and update `LICENSE-binary` under the applicable license while preserving asset attributions.
