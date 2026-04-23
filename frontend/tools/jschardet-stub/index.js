/*
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

// No-op replacement for the jschardet npm package.
//
// The upstream `jschardet` package (LGPL-2.1) is declared as a direct
// dependency of `@codingame/monaco-vscode-api` and is pulled in via a
// dynamic `await import('jschardet')` inside the VS Code textfile
// encoding-guessing service. Texera does not use that code path --
// Monaco is only used as a Yjs-backed code editor and never opens
// arbitrary binary files that require charset detection.
//
// To keep the LGPL code out of the Apache binary distribution we
// redirect the `jschardet` module name to this stub via Yarn's
// `resolutions`. The stub preserves the public API surface so the
// dynamic import resolves successfully and any defensive caller
// receives a safe "no guess" answer.

function detect() {
  return null;
}

function detectAll() {
  return [];
}

function enableDebug() {
  // no-op
}

module.exports = { detect: detect, detectAll: detectAll, enableDebug: enableDebug };
module.exports.default = module.exports;
