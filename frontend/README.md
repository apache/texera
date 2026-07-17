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

## Project layout

| Path                                           | What lives here                                                                                             |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `src/app/workspace/`                           | Workflow editor — operator graph, property panel, result panel, code editor.                                |
| `src/app/dashboard/`                           | User dashboard — workflows, datasets, projects, computing units, admin.                                     |
| `src/app/hub/`                                 | Public hub — discover and share workflows.                                                                  |
| `src/app/common/`                              | Cross-cutting services, types, formly extensions, and shared test helpers (`common/testing/test-utils.ts`). |
| `src/app/workspace/service/operator-metadata/` | Operator metadata service + the `Stub…Service` test doubles other specs reuse.                              |
| `vitest.config.ts`, `vitest.browser.config.ts` | Test-runner configs (jsdom default; Playwright Chromium for SVG/pointer-heavy specs).                       |
| `src/test-zone-setup.ts`                       | Vitest setup file — wraps `it`/`test` in an Angular ProxyZone so `fakeAsync` works.                         |
