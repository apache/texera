// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const workflow = name => fs.readFileSync(path.join(__dirname, "../workflows", name + ".yml"), "utf8");

for (const name of ["precheck", "benchmarks", "check-header"]) {
  test(`${name}: manual disable overrides healthy fleet on first-hop routing`, () => {
    const expr = workflow(name).match(/runs-on: \$\{\{ (.*?) \}\}/)[1];
    // Model the GitHub expression's PR label projection as an ordinary array.
    const js = expr.replace("github.event.pull_request.labels.*.name", "labels");
    for (const enabled of ["false", "true", ""]) {
      for (const available of ["false", "true", ""]) {
        const result = vm.runInNewContext(js, {
          github: { event_name: "pull_request" }, labels: ["ci:self-hosted"],
          vars: { ARC_ENABLED: enabled, ARC_AVAILABLE: available },
          contains: (values, value) => values.includes(value),
        });
        assert.equal(result, enabled === "false" || available === "false" ? "ubuntu-slim" : "arc-light-linux");
      }
    }
  });
}

test("manual disable overrides labeled PR and merge-queue build tiers", () => {
  const text = workflow("precheck");
  const block = text.slice(text.indexOf("if (selfHosted &&"), text.indexOf("// Heartbeat fallback:"));
  for (const enabled of ["false", "true", ""]) {
    for (const available of ["false", "true", ""]) {
      const tiers = { heavy_runner: "arc-heavy-linux", medium_runner: "arc-medium-linux", light_runner: "arc-light-linux" };
      vm.runInNewContext(block, { selfHosted: true, tiers, process: { env: { ARC_ENABLED: enabled, ARC_AVAILABLE: available } }, core: { warning() {} } });
      assert.equal(tiers.heavy_runner, enabled === "false" || available === "false" ? "ubuntu-latest" : "arc-heavy-linux");
      assert.equal(tiers.medium_runner, enabled === "false" || available === "false" ? "ubuntu-latest" : "arc-medium-linux");
      assert.equal(tiers.light_runner, enabled === "false" || available === "false" ? "ubuntu-latest" : "arc-light-linux");
    }
  }
});

test("manual disable overrides benchmark routing", () => {
  const text = workflow("benchmarks");
  const start = text.indexOf('if (linuxRunner === "arc-bench-linux" &&');
  const end = text.indexOf('if (linuxRunner === "arc-bench-linux" && process.env.ARC_STATUS_TOKEN)', start);
  for (const enabled of ["false", "true", ""]) {
    const result = vm.runInNewContext('let linuxRunner = "arc-bench-linux";\n' + text.slice(start, end) + '\nlinuxRunner;', {
      process: { env: { ARC_ENABLED: enabled, ARC_AVAILABLE: "true" } }, core: { warning() {} },
    });
    assert.equal(result, enabled === "false" ? "ubuntu-latest" : "arc-bench-linux");
  }
});
