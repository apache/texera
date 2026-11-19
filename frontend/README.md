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

# Texera Angular UI

The web UI for [Apache Texera](https://github.com/apache/texera). An Angular single-page app that talks to the JVM backend services (`amber`, `access-control-service`, `file-service`, …) and to the agent service.

Angular (standalone components) · Vitest (unit tests) · `@angular/build` builder · Yarn (Berry).

## Setup

Requires Node.js and Yarn — see the `engines` field in `package.json` for the supported versions. Yarn ships in-repo via `.yarn/`, no separate install.

```bash
cd frontend
yarn install
```

## Common commands

| What                                                  | Command                              |
| ----------------------------------------------------- | ------------------------------------ |
| Dev server (UI + y-websocket sidecar)                 | `yarn start` → http://localhost:4200 |
| Production build                                      | `yarn build`                         |
| Unit tests (jsdom, watch off)                         | `yarn test`                          |
| Unit tests in real browser mode (Playwright Chromium) | `ng run gui:test-browser`            |
| Unit tests with coverage in lcov form (CI shape)      | `yarn test:ci`                       |
| Format (Prettier + ESLint --fix)                      | `yarn format:fix`                    |
| Format check (CI shape)                               | `yarn format:ci`                     |
| Lint only                                             | `yarn lint`                          |

Run `ng help` for the full Angular CLI surface.

## Testing

Tests come first — write the failing test before the source change.

The full testing reference (Vitest stack, recipes, anti-patterns, coverage troubleshooting) is in [`TESTING.md`](TESTING.md).

## Workflow macros

Select operators, open **Operators → Macros**, enter a name, and choose **Create from selection**.
Drag saved macros from that panel into any workflow. Macros may contain other macros.
Dragging a macro captures its latest saved revision. Each usage keeps that immutable snapshot until explicitly updated.
Double-click a usage to open its pinned graph on the main canvas; title-bar breadcrumbs return to its parents.
Any pinned revision is editable with write access to its source and parent; saving creates new revisions without
changing historical snapshots. Older revisions use isolated drafts, including an older revision of the same macro.
Unexpected source changes stop the save so they can be reviewed before retrying. The property panel's revision
selector and **Use latest** action preserve connections to surviving ports and values of surviving parameters.
Removed ports disconnect their links; removed parameters stop forwarding through ancestors. Normal validation reports
disconnected inputs or incompatible retained values. Undo restores the previous pin, wiring and forwarded interface.
Saving through a parent creates normal workflow revisions along that breadcrumb path and updates only that usage.
Editing the definition directly at `/user/macros/:id` leaves all existing usages pinned.
The regular history panel supports preview and restore; restoring creates a new latest revision.
The canvas shows the selected instance's live row counts, workers, execution state and runtime, including nested macros.
Changing the viewed graph clears its execution statistics. **Save editable copy** creates an independent definition,
including for imported snapshots without accessible sources. Macro details beside the title list accessible direct and nested usages.
In a definition, two saved graph operators show Inputs and Outputs. Select a node and use + to add a port, then draw ordinary connections;
select a port and use − to remove it. Select a port to edit its name in the normal Property panel.
If a boundary operator is removed, add it again from Utilities; each role is offered only while missing.
Unconnected ports remain visible and must be wired or removed before expansion.
The port's Property panel also exposes an existing operator port. Replacing an internal input connection requires an explicit action.
Inputs can fan out; each output port has one source. Ports, links and positions use the normal shared graph and Undo/Redo.
The backend compiles these boundary operators away; they never create runtime workers.
Use **Choose Form View fields** in the Property panel, or **Edit** in Form View, to select a definition's public
parameters. The same fields appear on macro usages; a parent can select them for its own Form View.
Removing a macro parameter also removes its forwarding Form View fields and saved overrides along the edited parent path.
Undo/Redo restores those bindings and values, including after autosave; other usages keep their saved revisions.
A macro definition runs through a workflow. Compilation recursively expands it into ordinary operators.
Workflow JSON exports include pinned definitions, so importing does not depend on the original deployment.
External datasets and connections still need access on the destination deployment.
The editor and agent send pinned graphs to the shared Scala compiler in `common/workflow-compiler`.
Compilation and execution use the same expansion; its returned paths and port mappings drive nested schemas, results and statistics.
The TypeScript macro package retains the snapshot contract and presentation helpers.
Agent edits preserve pinned snapshots, and agent compilation and execution use the same expanded operators as the editor.
Exports share one immutable definition body per version, including the saved boundary operators and connections.

## Project layout

| Path                                           | What lives here                                                                                             |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `src/app/workspace/`                           | Workflow editor — operator graph, property panel, result panel, code editor.                                |
| `src/app/dashboard/`                           | User dashboard — workflows, datasets, computing units, admin.                                               |
| `src/app/hub/`                                 | Public hub — discover and share workflows.                                                                  |
| `src/app/common/`                              | Cross-cutting services, types, formly extensions, and shared test helpers (`common/testing/test-utils.ts`). |
| `src/app/workspace/service/operator-metadata/` | Operator metadata service + the `Stub…Service` test doubles other specs reuse.                              |
| `vitest.config.ts`, `vitest.browser.config.ts` | Test-runner configs (jsdom default; Playwright Chromium for SVG/pointer-heavy specs).                       |
| `src/test-zone-setup.ts`                       | Vitest setup file — wraps `it`/`test` in an Angular ProxyZone so `fakeAsync` works.                         |
