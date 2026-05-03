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
    // Make describe/it/expect/vi/beforeEach/etc available as globals so
    // existing Jasmine-style specs don't need a per-file import sweep.
    // Paired with `vitest/globals` triple-slash in src/vitest-globals.d.ts.
    globals: true,
    // Per-spec exclusions live in `angular.json` (the unit-test builder
    // applies them at the discovery stage, before Vitest's own filter,
    // which is what the Vitest team recommends — see the Vite warning
    // when this list is duplicated here.)
  },
});
