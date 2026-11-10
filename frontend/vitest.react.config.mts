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
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    include: ["src/react/**/*.spec.{ts,tsx}", "src/build-version.spec.ts", "src/environments/version.spec.ts"],
    exclude: ["src/react/**/*.browser.spec.{ts,tsx}"],
    reporters: ["default", ["junit", { outputFile: "junit.xml" }]],
    coverage: {
      provider: "v8",
      include: ["src/react/**/*.{ts,tsx}", "src/core/**/*.ts"],
      exclude: [
        "src/react/**/*.spec.{ts,tsx}",
        "src/react/main.tsx",
        "src/core/**/*.spec.ts",
        "src/core/common/type/proto/**",
      ],
      reporter: ["text", "lcov"],
      reportsDirectory: "coverage/react",
    },
    maxWorkers: 2,
    testTimeout: 20000,
    hookTimeout: 30000,
  },
});
