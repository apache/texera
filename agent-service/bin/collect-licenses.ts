#!/usr/bin/env bun
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

// Walk node_modules and emit a {name, version, license}[] manifest in
// the same shape license-webpack-plugin produces for the frontend.
// Run after `bun install --production --frozen-lockfile`. Output goes
// to stdout so a CI step can redirect it to dist/3rdpartylicenses.json.

import { readdir, readFile, stat } from "node:fs/promises";
import { join } from "node:path";

type Entry = { name: string; version: string; license: string };

function normalizeLicense(license: unknown): string {
  if (typeof license === "string") return license;
  if (license && typeof license === "object") {
    // legacy { type, url } form, or { license: "X", licenses: [...] }
    const obj = license as Record<string, unknown>;
    if (typeof obj.type === "string") return obj.type;
    if (Array.isArray(obj)) {
      return obj.map((l) => normalizeLicense(l)).filter(Boolean).join(" OR ");
    }
  }
  return "UNKNOWN";
}

async function readPackageJson(dir: string): Promise<Entry | null> {
  try {
    const raw = await readFile(join(dir, "package.json"), "utf8");
    const pkg = JSON.parse(raw);
    if (!pkg.name || !pkg.version) return null;
    const license = pkg.license ?? pkg.licenses ?? "UNKNOWN";
    return {
      name: pkg.name,
      version: pkg.version,
      license: normalizeLicense(license),
    };
  } catch {
    return null;
  }
}

async function walk(nm: string): Promise<Entry[]> {
  const entries: Entry[] = [];
  const top = await readdir(nm);
  for (const name of top) {
    if (name.startsWith(".")) continue;
    const path = join(nm, name);
    const st = await stat(path);
    if (!st.isDirectory()) continue;
    if (name.startsWith("@")) {
      // scoped: walk one more level
      const inner = await readdir(path);
      for (const sub of inner) {
        if (sub.startsWith(".")) continue;
        const e = await readPackageJson(join(path, sub));
        if (e) entries.push(e);
      }
    } else {
      const e = await readPackageJson(path);
      if (e) entries.push(e);
    }
  }
  return entries;
}

const nm = join(import.meta.dir, "..", "node_modules");
const entries = await walk(nm);
entries.sort((a, b) =>
  a.name === b.name ? a.version.localeCompare(b.version) : a.name.localeCompare(b.name),
);
process.stdout.write(JSON.stringify(entries, null, 2) + "\n");
