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

import { readFile, readdir, rename } from "node:fs/promises";
import { extname, join, relative, resolve, sep } from "node:path";

// Angular's dev proxy accepts globs; Vite matches a literal prefix or a regex.
// Convert the existing routing table without losing its ordered specificity.
export function toViteProxy(proxy) {
  return Object.fromEntries(
    Object.entries(proxy).map(([route, options]) => {
      const descendants = route.endsWith("/**");
      const pattern = (descendants ? route.slice(0, -3) : route)
        .split("*")
        .map(part => part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
        .join("[^/]+");
      return [`^${pattern}${descendants ? "(?:/.*)?(?:[?]|$)" : "(?:[/?]|$)"}`, { ...options }];
    })
  );
}

export function isReactDocumentRequest(url, accept) {
  const pathname = new URL(url, "http://localhost").pathname;
  return (
    typeof accept === "string" &&
    accept.includes("text/html") &&
    !/^\/(?:api|wsapi|rtc|assets|\.well-known)(?:\/|$)/.test(pathname) &&
    !pathname.startsWith("/@") &&
    !extname(pathname)
  );
}

function packageDirectory(moduleId) {
  const normalized = moduleId.split("?")[0].replaceAll("\\", "/");
  const marker = "/node_modules/";
  const index = normalized.lastIndexOf(marker);
  if (index < 0 || normalized.startsWith("\0")) return undefined;
  const packageParts = normalized.slice(index + marker.length).split("/");
  const count = packageParts[0].startsWith("@") ? 2 : 1;
  return normalized.slice(0, index + marker.length) + packageParts.slice(0, count).join("/");
}

// Restrict the manifest to modules retained in emitted chunks, rather than all
// installed packages. The binary-license audit consumes this exact schema.
export async function collectBundledLicenses(moduleIds) {
  const packages = new Map();
  for (const moduleId of moduleIds) {
    const directory = packageDirectory(moduleId);
    if (!directory || packages.has(directory)) continue;
    let metadata;
    try {
      metadata = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
    } catch (cause) {
      throw new Error(`Cannot read bundled package metadata: ${directory}`, { cause });
    }
    const license = typeof metadata.license === "string" ? metadata.license : metadata.license?.type;
    if (!metadata.name || !metadata.version || typeof license !== "string" || !license.trim()) {
      throw new Error(`Bundled package metadata must declare name, version and license: ${directory}`);
    }
    packages.set(directory, { name: metadata.name, version: metadata.version, license });
  }
  const unique = new Map([...packages.values()].map(entry => [`${entry.name}@${entry.version}`, entry]));
  return [...unique.values()].sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version));
}

async function filesBelow(directory) {
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (error.code === "ENOENT") return [];
    throw error;
  }
  const files = [];
  for (const entry of entries) {
    if (entry.name === ".gitkeep") continue;
    const file = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await filesBelow(file)));
    else if (entry.isFile()) files.push(file);
  }
  return files;
}

const contentTypes = {
  ".css": "text/css",
  ".csv": "text/csv",
  ".html": "text/html",
  ".ico": "image/x-icon",
  ".js": "text/javascript",
  ".json": "application/json",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".txt": "text/plain",
  ".wasm": "application/wasm",
  ".webmanifest": "application/manifest+json",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
};

export function createWorkerLicensePlugin(moduleIds) {
  return {
    name: "texera-worker-licenses",
    generateBundle(_options, bundle) {
      for (const output of Object.values(bundle))
        if (output.type === "chunk") for (const id of Object.keys(output.modules)) moduleIds.add(id);
    },
  };
}

export function createReactBuildPlugin({ root, outDir, workerModuleIds = new Set() }) {
  let building = true;
  let generated = false;
  return {
    name: "texera-react-build",
    enforce: "post",
    configResolved(config) {
      building = config.command === "build";
      if (config.build?.outDir) outDir = resolve(config.root ?? root, config.build.outDir);
    },
    buildStart() {
      workerModuleIds.clear();
    },
    configureServer(server) {
      server.middlewares.use(async (request, response, next) => {
        try {
          const pathname = decodeURIComponent(new URL(request.url, "http://localhost").pathname);
          const assetRoot = pathname.startsWith("/assets/")
            ? resolve(root, "src/assets")
            : pathname.startsWith("/.well-known/")
              ? resolve(root, ".well-known")
              : undefined;
          if (assetRoot) {
            const prefix = pathname.startsWith("/assets/") ? "/assets/" : "/.well-known/";
            const file = resolve(assetRoot, pathname.slice(prefix.length));
            if (!file.startsWith(assetRoot + sep)) {
              response.statusCode = 403;
              response.end();
              return;
            }
            try {
              const contents = await readFile(file);
              response.setHeader("Content-Type", contentTypes[extname(file)] ?? "application/octet-stream");
              response.end(contents);
            } catch (error) {
              if (error.code !== "ENOENT" && error.code !== "EISDIR") throw error;
              response.statusCode = 404;
              response.end();
            }
            return;
          }
          if (isReactDocumentRequest(request.url, request.headers.accept)) request.url = "/react.html";
          next();
        } catch (error) {
          next(error);
        }
      });
    },
    async generateBundle(_options, bundle) {
      const assetsRoot = join(root, "src/assets");
      try {
        const version = JSON.parse(await readFile(join(assetsRoot, "version.json"), "utf8"));
        if (typeof version.buildNumber !== "string" || !version.buildNumber || typeof version.version !== "string") {
          throw new Error("Invalid version payload");
        }
      } catch (cause) {
        throw new Error("Missing or invalid version manifest; run node build-version.js before building React", {
          cause,
        });
      }
      for (const [directory, prefix] of [
        [assetsRoot, "assets"],
        [join(root, ".well-known"), ".well-known"],
      ]) {
        for (const file of await filesBelow(directory)) {
          const contents = await readFile(file);
          this.emitFile({
            type: "asset",
            fileName: `${prefix}/${relative(directory, file).split(sep).join("/")}`,
            source: [".json", ".txt", ".svg", ".webmanifest", ".html", ".css"].includes(extname(file))
              ? contents.toString("utf8")
              : contents,
          });
        }
      }
      const moduleIds = Object.values(bundle).flatMap(output =>
        output.type === "chunk" ? Object.keys(output.modules) : []
      );
      this.emitFile({
        type: "asset",
        fileName: "3rdpartylicenses.json",
        source: JSON.stringify(await collectBundledLicenses([...moduleIds, ...workerModuleIds]), null, 2) + "\n",
      });
      generated = true;
    },
    async closeBundle() {
      // Keep the server's long-standing entry path while the source HTML has a
      // distinct name during migration. No backend or Docker path changes.
      if (building && generated) await rename(join(outDir, "react.html"), join(outDir, "index.html"));
    },
  };
}
