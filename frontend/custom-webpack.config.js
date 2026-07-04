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

const path = require("path");
const { LicenseWebpackPlugin } = require("license-webpack-plugin");

const nodeModule = (...segments) => path.resolve(__dirname, "node_modules", ...segments);
const codingameCssRe = /node_modules[\\/](?:@codingame[\\/]monaco-vscode-[^\\/]+|monaco-editor|vscode)[\\/].*\.css$/;

module.exports = {
  module: {
    rules: [
      {
        // codingame monaco-vscode-* ships raw assets (svg/ttf/png/woff*) that
        // webpack must emit as static files rather than try to parse as JS.
        test: /\.(svg|ttf|woff2?|png|jpg|jpeg|gif)$/,
        include: [nodeModule("@codingame")],
        type: "asset/resource",
      },
      {
        test: /\.css$/,
        oneOf: [
          {
            // codingame monaco-vscode-* CSS ships as Constructable Stylesheet
            // imports — must skip style-loader and use css-loader's
            // `exportType: 'css-style-sheet'`.
            // https://github.com/CodinGame/monaco-vscode-api/wiki/Troubleshooting
            test: codingameCssRe,
            use: [
              {
                loader: "css-loader",
                options: { esModule: false, exportType: "css-style-sheet", url: true, import: true },
              },
            ],
          },
          {
            // monaco-breakpoints ships a plain stylesheet that needs
            // style-loader so it injects at runtime.
            include: [nodeModule("monaco-breakpoints")],
            use: ["style-loader", "css-loader"],
          },
        ],
      },
    ],
    // Angular's webpack config disables both `new URL(...)` asset modules
    // (url: false) and `new Worker(new URL(...))` handling (worker: false
    // unless webWorkerTsConfig is set). monaco-vscode-api needs both: the
    // textmate service fetches onig.wasm via `new URL(..., import.meta.url)`,
    // and the editor/textmate/extension-host workers are spawned the same way.
    // Without this, webpack inlines `import.meta.url` as a build-machine
    // file:// path and everything 404s at runtime (no syntax highlighting).
    // See https://github.com/angular/angular-cli/issues/24617
    parser: {
      javascript: {
        url: true,
        worker: true,
      },
    },
  },
  resolve: {
    // css-loader emits relative imports (e.g. '../../../../../../../css-loader/
    // dist/runtime/api.js') computed from the source CSS location. The codingame
    // monaco-vscode-* packages live one namespace level deeper (under
    // `node_modules/@codingame/...`) than css-loader assumes, so the emitted
    // path lands at `node_modules/@codingame/css-loader/...` instead of
    // `node_modules/css-loader/...`. Alias the missing leg back to the real
    // install so webpack can resolve the runtime files.
    alias: {
      [nodeModule("@codingame/css-loader")]: nodeModule("css-loader"),
      [nodeModule("@codingame/style-loader")]: nodeModule("style-loader"),
    },
  },
  plugins: [
    new LicenseWebpackPlugin({
      perChunkOutput: false,
      outputFilename: "3rdpartylicenses.json",
      // Some codingame monaco-vscode-* sub-modules don't expose a license file
      // license-webpack-plugin can find; treat that as soft instead of fatal.
      handleMissingLicenseText: () => null,
      renderLicenses: (modules) =>
        JSON.stringify(
          modules
            .filter((m) => m.packageJson?.name && m.packageJson?.version)
            .map((m) => ({
              name: m.packageJson.name,
              version: m.packageJson.version,
              license: m.licenseId,
            }))
            .sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version)),
          null,
          2,
        ),
    }),
  ],
};
