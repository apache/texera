/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { build } from "vite";
import { isMonacoStylesheet, monacoStylesheets } from "./monaco-vite.mjs";

test("only codingame Monaco styles receive constructable sheet handling", () => {
  assert.equal(isMonacoStylesheet("C:/app/node_modules/@codingame/monaco-vscode-api/vscode/a.css"), true);
  assert.equal(isMonacoStylesheet("/app/node_modules/monaco-editor/a.css"), true);
  assert.equal(isMonacoStylesheet("/app/src/react/styles.css"), false);
  assert.equal(isMonacoStylesheet("/app/node_modules/monaco-breakpoints/a.css"), false);
});
test("ordinary and explicit inline CSS requests retain Vite behavior", async () => {
  const plugin = monacoStylesheets();
  const context = { resolve: async () => ({ id: "/app/src/react/styles.css" }) };
  assert.equal(await plugin.resolveId.call(context, "./styles.css", "/app/src/main.ts"), undefined);
  assert.equal(await plugin.resolveId.call(context, "./a.css?inline", "/app/node_modules/@codingame/x.js"), undefined);
});
test("real Vite build keeps stylesheet objects and rewrites font assets inside them", async () => {
  const root = await mkdtemp(join(tmpdir(), "texera-monaco-css-"));
  try {
    const dir = join(root, "node_modules/@codingame/monaco-vscode-fixture");
    await mkdir(dir, { recursive: true });
    await writeFile(
      join(dir, "package.json"),
      JSON.stringify({ name: "@codingame/monaco-vscode-fixture", version: "1.0.0", main: "index.js", license: "MIT" })
    );
    await writeFile(join(dir, "index.js"), 'export {default} from "./editor.css";');
    await writeFile(
      join(dir, "editor.css"),
      '@font-face{font-family:fixture;src:url("./font.woff2")} .fixture{color:red}'
    );
    await writeFile(join(dir, "font.woff2"), "fixture-font");
    await writeFile(join(root, "index.html"), '<script type="module" src="/main.js"></script>');
    await writeFile(
      join(root, "main.js"),
      'import sheet from "@codingame/monaco-vscode-fixture"; window.fixtureSheet=sheet;'
    );
    await build({
      configFile: false,
      root,
      plugins: [monacoStylesheets()],
      logLevel: "silent",
      build: { assetsInlineLimit: 0 },
    });
    const files = await readdir(join(root, "dist/assets"));
    assert.ok(files.some(file => file.endsWith(".woff2")));
    const js = await readFile(
      join(
        root,
        "dist/assets",
        files.find(file => file.endsWith(".js"))
      ),
      "utf8"
    );
    assert.match(js, /CSSStyleSheet/);
    assert.match(js, /replaceSync/);
    assert.match(js, /font-[^/]+\.woff2/);
  } finally {
    assert.equal(dirname(root), tmpdir());
    assert.match(basename(root), /^texera-monaco-css-/);
    await rm(root, { recursive: true, force: true });
  }
});
