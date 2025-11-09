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

module.exports = {
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
        include: [
          require("path").resolve(__dirname, "node_modules/monaco-editor"),
          require("path").resolve(__dirname, "node_modules/monaco-breakpoints")
        ],
      },
    ],
    // this is required for loading .wasm (and other) files.
    // For context, see https://stackoverflow.com/a/75252098 and https://github.com/angular/angular-cli/issues/24617
    parser: {
      javascript: {
        url: true,
      },
    },
  },
};
