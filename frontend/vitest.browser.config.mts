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
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import { monacoStylesheets } from "./tools/monaco-vite.mjs";
import { readdirSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [monacoStylesheets(), react()],
  optimizeDeps: {
    include: [
      "buffer",
      "yjs",
      "@lezer/python",
      "ajv",
      "concaveman",
      "d3-shape",
      "dagre",
      "graphlib",
      "html2canvas",
      "jointjs",
      "lodash-es",
      "lodash",
      "monaco-breakpoints",
      "rxjs",
      "rxjs/operators",
      "uuid",
      "y-websocket",
      "y-monaco",
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
  resolve: { dedupe: ["yjs", "y-protocols", "lib0"] },
  worker: { format: "es", plugins: () => [monacoStylesheets()] },
  assetsInclude: ["**/*.wasm"],
  test: {
    include: ["src/react/**/*.browser.spec.{ts,tsx}"],
    setupFiles: ["src/browser-buffer-polyfill.ts"],
    globals: true,
    reporters: ["default", ["junit", { outputFile: "junit-browser.xml" }]],
    testTimeout: 30000,
    hookTimeout: 30000,
    browser: { enabled: true, provider: playwright(), headless: true, instances: [{ browser: "chromium" }] },
  },
});
