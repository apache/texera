/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { afterEach, test } from "node:test";
import {
  collectBundledLicenses,
  createReactBuildPlugin,
  createWorkerLicensePlugin,
  isReactDocumentRequest,
  toViteProxy,
} from "./react-build.mjs";

const temporaryRoots = [];
test("deep-link pages resolve collaboration and other relative service URLs from the application root", async () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const html = await readFile(join(root, "react.html"), "utf8");
  assert.match(html, /<base\s+href="\/"\s*\/?>/);
});
afterEach(async () => {
  await Promise.all(temporaryRoots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), "texera-react-build-"));
  temporaryRoots.push(root);
  return root;
}

async function put(root, path, contents) {
  const file = join(root, path);
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, contents);
  return file;
}

test("the default React build emits the deployment artifact consumed by the backend and Docker", async () => {
  const { loadConfigFromFile } = await import("vite");
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const loaded = await loadConfigFromFile({ command: "build", mode: "production" }, join(root, "vite.config.mts"));
  assert.equal(loaded.config.build.outDir, join(root, "dist"));
});

test("default development, CI and browser commands invoke the React toolchain", async () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const pkg = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  assert.match(pkg.scripts.start, /vite/);
  assert.match(pkg.scripts.build, /vite.*build/);
  assert.match(pkg.scripts["test:ci"], /vitest.*coverage/);
  assert.match(pkg.scripts["test:browser"], /vitest.*vitest\.browser\.config\.mts/);
  assert.match(pkg.scripts["test:tools"], /node --test/);
  for (const name of [...Object.keys(pkg.dependencies), ...Object.keys(pkg.devDependencies)])
    assert.ok(
      !/^@angular|^@nx\/|^@ngx|^ngx-|angular|^ng-zorro|^zone\.js$/.test(name),
      `obsolete Angular package ${name}`
    );
  const ci = await readFile(join(root, "../.github/workflows/build.yml"), "utf8");
  assert.match(ci, /yarn --cwd frontend (?:run )?test:browser/);
  assert.doesNotMatch(ci, /ng run gui:test-browser/);
});

test("the framework-independent core retains the original graph, editor and numerical contracts", async () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  for (const path of [
    "workflow-graph/model/workflow-graph.spec.ts",
    "workflow-graph/model/shared-model.spec.ts",
    "joint-ui/joint-ui.service.spec.ts",
    "notebook-migration/migration-llm.spec.ts",
    "heatmap/heatmap-scoring.spec.ts",
    "heatmap/heatmap-color.spec.ts",
    "heatmap/heatmap-overlay-persistence.spec.ts",
    "heatmap/runtime-statistics-mapper.spec.ts",
    "workflow-status/performance-metrics.spec.ts",
  ])
    assert.match(await readFile(join(root, "src/core/workspace/service", path), "utf8"), /\b(?:it|test)\(/);
});

test("the frontend has no Angular build entry points after the replacement", async () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  for (const path of ["angular.json", "custom-webpack.config.js", "nx.json", "src/main.ts", "src/test-zone-setup.ts"])
    await assert.rejects(readFile(join(root, path)), { code: "ENOENT" });
});

test("React development and test scripts select ESM-native configuration files", async () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const pkg = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  assert.match(pkg.scripts["start:react"], /vite\.config\.mts/);
  assert.match(pkg.scripts["build:react"], /vite\.config\.mts/);
  assert.match(pkg.scripts["test:react"], /vitest\.react\.config\.mts/);
  const { loadConfigFromFile } = await import("vite");
  const vite = await loadConfigFromFile(
    { command: "serve", mode: "development" },
    join(root, "vite.config.mts"),
    root,
    "silent",
    undefined,
    "native"
  );
  const vitest = await loadConfigFromFile(
    { command: "serve", mode: "test" },
    join(root, "vitest.react.config.mts"),
    root,
    "silent",
    undefined,
    "native"
  );
  assert.equal(vite.config.server.port, 4200);
  assert.deepEqual(vitest.config.test.include, [
    "src/react/**/*.spec.{ts,tsx}",
    "src/build-version.spec.ts",
    "src/environments/version.spec.ts",
  ]);
  assert.deepEqual(vitest.config.test.exclude, ["src/react/**/*.browser.spec.{ts,tsx}"]);
  const browser = await loadConfigFromFile(
    { command: "serve", mode: "test" },
    join(root, "vitest.browser.config.mts"),
    root,
    "silent",
    undefined,
    "native"
  );
  assert.deepEqual(browser.config.test.include, ["src/react/**/*.browser.spec.{ts,tsx}"]);
  assert.deepEqual(browser.config.test.setupFiles, ["src/browser-buffer-polyfill.ts"]);
});

test("dev proxy preserves backend selection, path boundaries and WebSocket protocols", () => {
  const proxy = toViteProxy({
    "/api/agents/*/react": { target: "http://localhost:3001", ws: true },
    "/api/config/**": { target: "http://localhost:9094", changeOrigin: true },
    "/api": { target: "http://localhost:8080", changeOrigin: false },
    "/wsapi": { target: "http://localhost:8085", ws: true },
    "/rtc": { target: "http://localhost:1234", ws: true },
  });
  const route = url => Object.entries(proxy).find(([pattern]) => new RegExp(pattern).test(url))?.[1];
  assert.equal(route("/api/agents/a/react?session=1").target, "http://localhost:3001");
  assert.equal(route("/api/agents/a/react").ws, true);
  assert.equal(route("/api/config/pre-login").target, "http://localhost:9094");
  assert.equal(route("/api/config").target, "http://localhost:9094");
  assert.equal(route("/api/workflow/1").target, "http://localhost:8080");
  assert.equal(route("/wsapi/workflow").ws, true);
  assert.equal(route("/rtc/workflow-id").ws, true);
  assert.equal(route("/apiary"), undefined);
  assert.equal(route("/rtcalendar"), undefined);
  assert.equal(route("/api/agents/a/b/react").target, "http://localhost:8080");
});

test("history fallback serves application documents while leaving APIs and assets alone", () => {
  for (const url of ["/", "/login", "/user/workflow/12?version=3", "/hub/dataset/result/detail/1"]) {
    assert.equal(isReactDocumentRequest(url, "text/html,application/xhtml+xml"), true, url);
  }
  for (const url of [
    "/api/config/pre-login",
    "/wsapi/workflow",
    "/rtc/1",
    "/assets/version.json",
    "/.well-known/token",
    "/@vite/client",
  ]) {
    assert.equal(isReactDocumentRequest(url, "text/html"), false, url);
  }
  assert.equal(isReactDocumentRequest("/user/workflow/1", "application/json"), false);
  assert.equal(isReactDocumentRequest("/user/workflow/1", undefined), false);
});

test("license manifest includes bundled packages and excludes unused installed dependencies", async () => {
  const root = await fixture();
  const react = await put(
    root,
    "node_modules/react/package.json",
    JSON.stringify({ name: "react", version: "19.2.0", license: "MIT" })
  );
  const dom = await put(
    root,
    "node_modules/react-dom/package.json",
    JSON.stringify({ name: "react-dom", version: "19.2.0", license: "MIT" })
  );
  await put(
    root,
    "node_modules/unused/package.json",
    JSON.stringify({ name: "unused", version: "1.0.0", license: "GPL-3.0" })
  );
  const modules = [
    react.replace("package.json", "index.js"),
    react.replace("package.json", "jsx-runtime.js?commonjs-proxy"),
    dom.replace("package.json", "client.js"),
    join(root, "src/react/main.tsx"),
    "\0virtual:react",
  ];
  assert.deepEqual(await collectBundledLicenses(modules), [
    { name: "react", version: "19.2.0", license: "MIT" },
    { name: "react-dom", version: "19.2.0", license: "MIT" },
  ]);
  assert.deepEqual(await collectBundledLicenses([]), []);
});

test("license collection handles scoped and nested dependencies with their own versions", async () => {
  const root = await fixture();
  const paths = [];
  for (const [path, name, version] of [
    ["@vendor/widget", "@vendor/widget", "1.0.0"],
    ["widget/node_modules/shared", "shared", "2.0.0"],
    ["shared", "shared", "1.0.0"],
  ]) {
    const manifest = await put(
      root,
      `node_modules/${path}/package.json`,
      JSON.stringify({ name, version, license: { type: "Apache-2.0" } })
    );
    paths.push(manifest.replace("package.json", "index.js"));
  }
  assert.deepEqual(await collectBundledLicenses(paths), [
    { name: "@vendor/widget", version: "1.0.0", license: "Apache-2.0" },
    { name: "shared", version: "1.0.0", license: "Apache-2.0" },
    { name: "shared", version: "2.0.0", license: "Apache-2.0" },
  ]);
});

test("unreadable or incomplete bundled package metadata fails the build", async () => {
  const root = await fixture();
  const manifest = await put(root, "node_modules/broken/package.json", "{");
  await assert.rejects(collectBundledLicenses([manifest.replace("package.json", "index.js")]), /package metadata/i);
  await writeFile(manifest, JSON.stringify({ name: "broken", version: "1.0.0" }));
  await assert.rejects(collectBundledLicenses([manifest.replace("package.json", "index.js")]), /license/i);
});

test("production plugin preserves assets, version, association files and the backend index name", async () => {
  const root = await fixture();
  const outDir = join(root, "dist");
  await put(root, "src/assets/version.json", '{"buildNumber":"test","version":"1"}');
  await put(root, "src/assets/logos/favicon-32x32.png", new Uint8Array([137, 80, 78, 71]));
  await put(root, ".well-known/apple-developer-domain-association.txt", "association");
  await put(root, "dist/react.html", '<div id="root"></div>');
  const emitted = [];
  const plugin = createReactBuildPlugin({ root, outDir });
  await plugin.generateBundle.call({ emitFile: asset => emitted.push(asset) }, {}, {});
  assert.equal(
    emitted.find(asset => asset.fileName === "assets/version.json").source,
    '{"buildNumber":"test","version":"1"}'
  );
  assert.deepEqual(
    Buffer.from(emitted.find(asset => asset.fileName === "assets/logos/favicon-32x32.png").source),
    Buffer.from([137, 80, 78, 71])
  );
  assert.equal(
    emitted.find(asset => asset.fileName === ".well-known/apple-developer-domain-association.txt").source,
    "association"
  );
  assert.deepEqual(JSON.parse(emitted.find(asset => asset.fileName === "3rdpartylicenses.json").source), []);
  await plugin.closeBundle();
  assert.equal(await readFile(join(outDir, "index.html"), "utf8"), '<div id="root"></div>');
  await assert.rejects(readFile(join(outDir, "react.html")), { code: "ENOENT" });
});

test("production build refuses a missing version manifest instead of shipping a broken update check", async () => {
  const root = await fixture();
  await put(root, "src/assets/logos/logo.svg", "<svg/>");
  const plugin = createReactBuildPlugin({ root, outDir: join(root, "dist") });
  await assert.rejects(plugin.generateBundle.call({ emitFile() {} }, {}, {}), /version manifest/i);
});

test("closing the dev server does not try to rename production output", async () => {
  const root = await fixture();
  const plugin = createReactBuildPlugin({ root, outDir: join(root, "dist") });
  plugin.configResolved({ command: "serve" });
  await plugin.closeBundle();
});
test("the deployed index honors a caller's output-directory override", async () => {
  const root = await fixture();
  const override = join(root, "custom-output");
  await put(override, "react.html", "Texera");
  const plugin = createReactBuildPlugin({ root, outDir: join(root, "default-output") });
  plugin.configResolved({ command: "build", root, build: { outDir: override } });
  await put(root, "src/assets/version.json", '{"buildNumber":"test","version":"1"}');
  await plugin.generateBundle.call({ emitFile() {} }, {}, {});
  await plugin.closeBundle();
  assert.equal(await readFile(join(override, "index.html"), "utf8"), "Texera");
});

test("a failed compilation can close without a secondary missing-output error", async () => {
  const root = await fixture();
  const plugin = createReactBuildPlugin({ root, outDir: join(root, "dist") });
  plugin.configResolved({ command: "build" });
  await plugin.closeBundle();
});

test("real Vite build emits the deployable index and an accurate React runtime license manifest", async () => {
  const { build } = await import("vite");
  const require = createRequire(import.meta.url);
  const root = await fixture();
  const outDir = join(root, "dist");
  await put(root, "src/assets/version.json", '{"buildNumber":"test","version":"1"}');
  await put(root, "react.html", '<div id="root"></div><script type="module" src="/entry.js"></script>');
  await put(
    root,
    "entry.js",
    `import {createRoot} from ${JSON.stringify(require.resolve("react-dom/client").replaceAll("\\", "/"))}; import {createElement} from ${JSON.stringify(require.resolve("react").replaceAll("\\", "/"))}; createRoot(document.querySelector("#root")).render(createElement("h1", {}, "Texera"));`
  );
  await build({
    configFile: false,
    root,
    publicDir: false,
    logLevel: "silent",
    plugins: [createReactBuildPlugin({ root, outDir })],
    build: { outDir, rolldownOptions: { input: join(root, "react.html") } },
  });
  const html = await readFile(join(outDir, "index.html"), "utf8");
  assert.match(html, /\/assets\/[^" ]+\.js/);
  assert.equal(JSON.parse(await readFile(join(outDir, "assets/version.json"), "utf8")).buildNumber, "test");
  const licenses = JSON.parse(await readFile(join(outDir, "3rdpartylicenses.json"), "utf8"));
  assert.deepEqual(
    licenses.map(entry => entry.name),
    ["react", "react-dom", "scheduler"]
  );
  assert.ok(licenses.every(entry => entry.license === "MIT" && typeof entry.version === "string"));
});

test("production licenses include dependencies emitted only in worker chunks", async () => {
  const { build } = await import("vite");
  const root = await fixture();
  const outDir = join(root, "dist");
  await put(root, "src/assets/version.json", '{"buildNumber":"test","version":"1"}');
  await put(root, "react.html", '<script type="module" src="/entry.js"></script>');
  await put(root, "entry.js", 'new Worker(new URL("./worker.js", import.meta.url), {type:"module"});');
  await put(root, "worker.js", 'import {message} from "worker-only"; postMessage(message);');
  await put(
    root,
    "node_modules/worker-only/package.json",
    JSON.stringify({ name: "worker-only", version: "1.2.3", license: "MIT", type: "module", exports: "./index.js" })
  );
  await put(root, "node_modules/worker-only/index.js", 'export const message="worker";');
  const workerModuleIds = new Set();
  await build({
    configFile: false,
    root,
    publicDir: false,
    logLevel: "silent",
    plugins: [createReactBuildPlugin({ root, outDir, workerModuleIds })],
    worker: { format: "es", plugins: () => [createWorkerLicensePlugin(workerModuleIds)] },
    build: { outDir, rolldownOptions: { input: join(root, "react.html") } },
  });
  const licenses = JSON.parse(await readFile(join(outDir, "3rdpartylicenses.json"), "utf8"));
  assert.deepEqual(licenses, [{ name: "worker-only", version: "1.2.3", license: "MIT" }]);
});

test("real Vite server serves React deep links and assets while leaving API requests separate", async () => {
  const { createServer } = await import("vite");
  const root = await fixture();
  await put(root, "react.html", "<h1>Texera React</h1>");
  await put(root, "src/assets/version.json", '{"buildNumber":"dev","version":"1"}');
  const server = await createServer({
    configFile: false,
    root,
    publicDir: false,
    logLevel: "silent",
    plugins: [createReactBuildPlugin({ root, outDir: join(root, "dist") })],
    server: { host: "127.0.0.1", port: 0 },
  });
  try {
    await server.listen();
    const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
    const document = await fetch(`${origin}/user/workflow/42`, { headers: { accept: "text/html" } });
    assert.equal(document.status, 200);
    assert.match(await document.text(), /Texera React/);
    const manifest = await fetch(`${origin}/assets/version.json`);
    assert.equal(manifest.status, 200);
    assert.equal((await manifest.json()).buildNumber, "dev");
    const api = await fetch(`${origin}/api/config/pre-login`, { headers: { accept: "text/html" } });
    assert.equal(api.status, 404);
    assert.doesNotMatch(await api.text(), /Texera React/);
  } finally {
    await server.close();
  }
});
