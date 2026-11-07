/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const PREFIX = "\0texera-monaco-style:";
const NO_NODE_FS = "\0texera-monaco-browser-fs";
export function isMonacoStylesheet(id) {
  return /\/node_modules\/(?:@codingame\/monaco-vscode-[^/]+|monaco-editor|vscode)\/.*\.css$/.test(
    id.replaceAll("\\", "/")
  );
}

// Codingame consumes actual stylesheet objects. Vite's ?inline performs the
// CSS processing (including @import and asset URL rewriting) without injecting
// a second global <style>; the resulting text becomes a constructable sheet.
export function monacoStylesheets() {
  return {
    name: "texera-monaco-stylesheets",
    enforce: "pre",
    async resolveId(source, importer) {
      if (source.startsWith(PREFIX) || source === NO_NODE_FS) return source;
      if (source === "node:fs/promises" && importer?.replaceAll("\\", "/").includes("/@vscode/diff/"))
        return NO_NODE_FS;
      if (!source.endsWith(".css")) return undefined;
      const resolved = await this.resolve(source, importer, { skipSelf: true });
      if (resolved && !resolved.external && isMonacoStylesheet(resolved.id)) return PREFIX + resolved.id + ".js";
    },
    load(id) {
      if (id === NO_NODE_FS)
        return 'export async function readFile(){throw new Error("Node filesystem is unavailable in the browser.")}';
      if (!id.startsWith(PREFIX)) return undefined;
      const path = id.slice(PREFIX.length, -3);
      return `import css from ${JSON.stringify(path + "?inline")};const sheet=new CSSStyleSheet();sheet.replaceSync(css);export default sheet;`;
    },
  };
}
