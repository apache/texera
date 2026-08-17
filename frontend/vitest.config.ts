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

import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Emit a JUnit-XML report alongside the default console reporter so
    // Codecov Test Analytics can ingest failing-test stack traces and
    // detect flakies on main. `default` stays first so CI logs read the
    // same as before.
    reporters: ["default", ["junit", { outputFile: "junit.xml" }]],
    // Make describe/it/expect/vi/beforeEach/etc available as globals so
    // existing Jasmine-style specs don't need a per-file import sweep.
    // Paired with `vitest/globals` triple-slash in src/vitest-globals.d.ts.
    globals: true,
    // Wrap `it`/`test` so each spec body runs inside an Angular ProxyZone,
    // which Angular's `fakeAsync` requires. Karma+Jasmine installed this
    // implicitly; the @angular/build:unit-test path doesn't.
    setupFiles: ["src/test-zone-setup.ts"],
    // Headroom over Vitest's defaults (5s test / 10s hook) for the shared
    // macos-latest runners, whose wall time swings ~2x run to run: the same
    // test that takes ~400ms on ubuntu has been observed at 11s+ in a
    // beforeEach on a loaded macOS runner, and the leg was failing on pure
    // timeouts in a different spec nearly every time (#7713). Roughly 4x/3x
    // the worst observed times; a genuinely hung test still fails, just a
    // few seconds later on a 9-17 minute leg.
    testTimeout: 20000,
    hookTimeout: 30000,
    // Per-spec exclusions live in `angular.json` (the unit-test builder
    // applies them at the discovery stage, before Vitest's own filter,
    // which is what the Vitest team recommends — see the Vite warning
    // when this list is duplicated here.)
  },
});
