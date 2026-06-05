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

// monaco-editor-wrapper + monaco-vscode-files-service-override dereference
// Node's Buffer (including Buffer.allocUnsafe) at module-evaluation time.
// Under jsdom-mode Vitest, the test runs on Node and Buffer is built-in. In
// browser-mode (Playwright/Chromium), Buffer doesn't exist, so importing
// monaco-editor-wrapper crashes at load. The `buffer` npm package is the
// browser polyfill of Node's API; expose its `Buffer` (and a minimal
// `process` shim) on the global so monaco's internals find them at the same
// names they'd find Node's. Has to run before any monaco import is
// evaluated, which is why this is wired into vitest.browser.config.ts as the
// FIRST setupFile — setupFiles run in order, before any test module loads.
//
// We use a namespace import (rather than a named or default import) because
// Vite's dep-optimizer rewrites `buffer@5`'s CJS exports in a way that
// exposes neither a `Buffer` named export nor a `default` export at
// module-eval time. The namespace object always has the optimizer-injected
// shape, so we read `Buffer` off it dynamically.
import * as bufferModule from "buffer";

const { Buffer } = bufferModule as unknown as { Buffer: typeof globalThis.Buffer };

(globalThis as unknown as { Buffer: typeof Buffer }).Buffer = Buffer;
(globalThis as unknown as { global: typeof globalThis }).global = globalThis;
if (!(globalThis as unknown as { process?: { env: Record<string, string> } }).process) {
  (globalThis as unknown as { process: { env: Record<string, string> } }).process = { env: {} };
}
