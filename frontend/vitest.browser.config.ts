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

import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

const uuidBrowser = fileURLToPath(new URL("./node_modules/uuid/dist/esm-browser/index.js", import.meta.url));
const lib0Webcrypto = fileURLToPath(new URL("./node_modules/lib0/webcrypto.js", import.meta.url));

// Browser-mode config for specs that need real DOM/SVG geometry
// (getScreenCTM, getBoundingClientRect, pointer-event hit testing).
// jsdom's polyfill in src/jsdom-svg-polyfill.ts returns identity stubs,
// which is enough to instantiate jointjs but not to compute layout that
// click/hit tests depend on. See #4866.
export default defineConfig({
  // Vite's default resolution picks node entries for transitive deps
  // because @angular/build:unit-test sets a server-like environment.
  // Force the browser entry for the two offenders pulled in by
  // workflow-graph services (uuid + lib0/webcrypto via yjs).
  resolve: {
    conditions: ["browser", "module", "import", "default"],
    alias: [
      { find: /^uuid$/, replacement: uuidBrowser },
      { find: /^lib0\/webcrypto$/, replacement: lib0Webcrypto },
    ],
  },
  test: {
    // Emit a JUnit-XML report alongside the default console reporter so
    // Codecov Test Analytics can ingest browser-mode failures and detect
    // flakies on main. Written to a distinct filename so the upload step
    // can disambiguate it from the unit-test report.
    reporters: ["default", ["junit", { outputFile: "junit-browser.xml" }]],
    globals: true,
    setupFiles: ["src/test-zone-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      headless: true,
      instances: [{ browser: "chromium" }],
    },
    server: {
      deps: {
        inline: [/monaco-breakpoints/, /^uuid$/, /^lib0\//],
      },
    },
  },
});
