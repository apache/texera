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

// No-op replacement for the LGPL-2.1 `jschardet` package, which is
// ASF Category X. Redirected here via `resolutions` in
// frontend/package.json. The upstream call site lives in
// @codingame/monaco-vscode-api's encoding service and is only reached
// when opening binary files through Monaco, which Texera never does.

module.exports = {
  detect: () => null,
};
module.exports.default = module.exports;
