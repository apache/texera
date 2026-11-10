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

import react from "@vitejs/plugin-react";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import { createReactBuildPlugin, createWorkerLicensePlugin, toViteProxy } from "./tools/react-build.mjs";
import { monacoStylesheets } from "./tools/monaco-vite.mjs";

const root = dirname(fileURLToPath(import.meta.url));
// Backend FileAssetsBundle and Docker consume this production directory.
const outDir = resolve(root, "dist");
const proxy = toViteProxy(JSON.parse(readFileSync(resolve(root, "proxy.config.json"), "utf8")));
const workerModuleIds = new Set<string>();

export default defineConfig(({ command }) => ({
  root,
  publicDir: false,
  plugins: [monacoStylesheets(), react(), createReactBuildPlugin({ root, outDir, workerModuleIds })],
  optimizeDeps: {
    include: [
      "y-monaco",
      "yjs",
      "vscode-jsonrpc/browser",
      "vscode-jsonrpc",
      "vscode-languageserver-protocol",
      "vscode-languageserver-types",
      "vscode-languageserver-textdocument",
      "vscode-ws-jsonrpc",
      "vscode-languageserver-protocol/browser",
      "vscode-languageclient/browser",
      "monaco-languageclient > vscode-languageserver-protocol/browser",
      "monaco-languageclient > vscode-languageserver-protocol",
      "monaco-languageclient > vscode-languageserver-protocol > vscode-jsonrpc",
      "monaco-languageclient > vscode-languageserver-protocol > vscode-jsonrpc/browser",
    ],
    exclude: [
      "monaco-editor",
      "vscode",
      "monaco-editor-wrapper",
      "monaco-languageclient",
      ...readdirSync(resolve(root, "node_modules/@codingame")).map(name => `@codingame/${name}`),
    ],
  },
  worker: { format: "es", plugins: () => [monacoStylesheets(), createWorkerLicensePlugin(workerModuleIds)] },
  assetsInclude: ["**/*.wasm"],
  resolve: {
    dedupe: ["yjs", "y-protocols", "lib0"],
    alias:
      command === "build"
        ? [
            {
              find: /^(.*\/)?environments\/version(?:\.ts)?$/,
              replacement: resolve(root, "src/environments/version.prod.ts"),
            },
          ]
        : [],
  },
  server: { port: 4200, strictPort: true, proxy },
  preview: { port: 4200, strictPort: true, proxy },
  build: {
    outDir,
    emptyOutDir: true,
    sourcemap: false,
    rolldownOptions: { input: resolve(root, "react.html") },
  },
}));
