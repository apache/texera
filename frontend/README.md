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

# Texera React UI

The web UI for [Apache Texera](https://github.com/apache/texera): React, TypeScript, Vite, Vitest and Yarn. It talks to the existing JVM services and agent service through the same HTTP and WebSocket endpoints.

## Setup

Use Node 24 and the Yarn version pinned in `package.json`. Yarn is included under `.yarn/releases/`.

```bash
cd frontend
yarn install --immutable
yarn start
```

The UI listens on http://localhost:4200. `yarn start` also starts the collaborative editing relay on port 1234. Prefer [`bin/local-dev.sh`](../bin/local-dev/README.md) to start the complete local stack. `proxy.config.json` maps API, execution WebSocket and collaboration traffic to the native services.

## Commands

| Task | Command |
| --- | --- |
| Development UI and collaboration relay | `yarn start` |
| Production build | `yarn build` |
| Unit tests in jsdom | `yarn test` |
| Unit tests with V8/lcov coverage and build contracts | `yarn test:ci` |
| Real Chromium component tests | `yarn test:browser` |
| Build, proxy, worker and license contract tests | `yarn test:tools` |
| Production workspace end-to-end tests | `yarn build && yarn test:e2e` |
| Type check | `yarn tsc --project tsconfig.json --noEmit` |
| Lint | `yarn lint` |
| Format / check formatting | `yarn format:fix` / `yarn format:ci` |

Install the browser once with `yarn playwright install chromium` (use `--with-deps` on Linux CI). `start:react`, `build:react` and `test:react` remain command aliases for existing development scripts.

## Deployment

`yarn build` generates the build number, type checks the application and writes `dist/index.html` plus the compiled assets. The backend and Docker image continue serving `frontend/dist`. Deep links use the root document base, preserving collaboration URLs on direct workspace navigation.

The build retains all `src/assets`, `.well-known` association files, `assets/version.json`, Monaco workers/WASM and `3rdpartylicenses.json`. The license manifest includes packages from both application and worker chunks; it is checked against `LICENSE-binary`. Set `SOURCE_DATE_EPOCH` for a reproducible version number. Deployment feature switches and branding remain server configuration rather than build-time flags.

## Layout

| Path | Contents |
| --- | --- |
| `src/react/workspace/` | Shared workflow session, canvas, form view, execution and editor controls |
| `src/react/forms/`, `src/react/code-editor/` | Schema forms, specialized widgets, Monaco and language clients |
| `src/react/workflows/`, `src/react/hub/`, `src/react/resources/` | Private and public resource pages and administration |
| `src/react/core/` | Authentication, transport and runtime configuration |
| `src/core/` | Framework-independent graph, execution types, numerical helpers and retained behavior tests |
| `src/assets/` | Application images, static files and generated version manifest |
| `tools/` | Asset/license/worker build integration and production browser harness |

Read [`TESTING.md`](TESTING.md) for test conventions and verification scope.
