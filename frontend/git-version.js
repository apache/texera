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

const { gitDescribeSync } = require("git-describe");
const { version } = require("./package.json");
const { resolve, relative } = require("path");
const { writeFileSync, existsSync, mkdirSync } = require("fs-extra");

const gitInfo = gitDescribeSync({
  dirtyMark: false,
  dirtySemver: false,
});

gitInfo.version = version;

if (!existsSync(__dirname + "/src/environments")) {
  mkdirSync(__dirname + "/src/environments");
}
const file = resolve(__dirname, "src", "environments", "version.ts");
writeFileSync(
  file,
  `// IMPORTANT: THIS FILE IS AUTO GENERATED! DO NOT MANUALLY EDIT OR CHECKIN!
/* tslint:disable */
export const Version = ${JSON.stringify(gitInfo, null, 4)};
/* tslint:enable */
`,
  { encoding: "utf-8" }
);

console.log(`Wrote version info ${gitInfo.raw} to ${relative(resolve(__dirname, ".."), file)}`);
