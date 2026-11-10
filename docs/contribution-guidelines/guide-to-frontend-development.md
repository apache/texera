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

---
title: "Guide to Frontend Development"
weight: 30
---

# Developing the React frontend

Texera uses React, TypeScript and Vite. Components and feature state live under `frontend/src/react`; framework-independent graph, execution types and numerical helpers live under `frontend/src/core`.

Start with [frontend setup and commands](https://github.com/apache/texera/blob/main/frontend/README.md), then read the [testing guide](https://github.com/apache/texera/blob/main/frontend/TESTING.md). Use Node 24 and the pinned Yarn release.

```bash
cd frontend
yarn install --immutable
yarn start
```

The development UI listens on http://localhost:4200 and reloads saved changes. The same command starts the shared-editing relay. To run the complete native development stack, follow [bin/local-dev](https://github.com/apache/texera/blob/main/bin/local-dev/README.md). IntelliJ can run the `start` script from `frontend/package.json`.

Keep components, API clients, styles and tests beside the feature they implement. Use `.tsx` files for components and `.spec.tsx` for component behavior. Shared non-visual functions can use `.ts` and `.spec.ts`. Test user input, API failures, access restrictions, empty states and cleanup; use actual Chromium for SVG hit testing and Monaco editing.

```bash
yarn test
yarn test:browser
yarn format:ci
yarn build
yarn test:e2e
```

The production build writes `frontend/dist`, retaining the backend deployment path, static assets, version manifest and bundled-license inventory. Runtime feature switches and branding come from server configuration. Read the repository contribution rules before preparing a PR, including the required before/after screenshots for visible changes.

Original guide by Yinan Zhou; updated for the React frontend.
