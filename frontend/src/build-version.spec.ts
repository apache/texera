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

import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { renderVersionArtifacts } from "../build-version";

describe("build-version: renderVersionArtifacts", () => {
  const VERSION = "1.2.3-incubating";
  const BUILD = "1.2.3-incubating.250101010";

  describe("with an explicit build number (deterministic)", () => {
    it("embeds both the build number and version into version.prod.ts", () => {
      const { prodTs } = renderVersionArtifacts(VERSION, BUILD);
      expect(prodTs).toContain("export const Version");
      expect(prodTs).toContain(`buildNumber: "${BUILD}"`);
      expect(prodTs).toContain(`version: "${VERSION}"`);
    });

    it("marks version.prod.ts as auto-generated so it is not hand-edited", () => {
      const { prodTs } = renderVersionArtifacts(VERSION, BUILD);
      expect(prodTs).toContain("AUTO-GENERATED");
      expect(prodTs.endsWith("\n")).toBe(true);
    });

    it("produces a manifest that the running app can parse, carrying the same build number", () => {
      const { manifestJson, buildNumber } = renderVersionArtifacts(VERSION, BUILD);
      expect(buildNumber).toBe(BUILD);
      expect(manifestJson.endsWith("\n")).toBe(true);
      expect(JSON.parse(manifestJson)).toEqual({ buildNumber: BUILD, version: VERSION });
    });

    it("keeps the bundle's build number and the manifest's build number identical (the comparison the service relies on)", () => {
      const { prodTs, manifestJson } = renderVersionArtifacts(VERSION, BUILD);
      expect(prodTs).toContain(`"${BUILD}"`);
      expect(JSON.parse(manifestJson).buildNumber).toBe(BUILD);
    });

    it("emits a TypeScript module that can be evaluated to the expected Version object", () => {
      const { prodTs } = renderVersionArtifacts(VERSION, BUILD);
      const evaluated = new Function(`${prodTs.replace("export ", "")} return Version;`)();
      expect(evaluated).toEqual({ buildNumber: BUILD, version: VERSION });
    });
  });

  describe("string-safety of interpolated values", () => {
    it("JSON-escapes a version containing quotes so the generated module stays valid", () => {
      const tricky = '1.0.0"; throw new Error("x';
      const { prodTs } = renderVersionArtifacts(tricky, BUILD);
      const evaluated = new Function(`${prodTs.replace("export ", "")} return Version;`)();
      expect(evaluated.version).toBe(tricky);
    });
  });

  describe("with a generated build number (default argument)", () => {
    it("derives a non-empty build number from the version when none is supplied", () => {
      const { buildNumber, manifestJson } = renderVersionArtifacts(VERSION);
      expect(typeof buildNumber).toBe("string");
      expect(buildNumber.length).toBeGreaterThan(0);
      // build-number-generator prefixes the generated number with the version.
      expect(buildNumber.startsWith(VERSION)).toBe(true);
      expect(JSON.parse(manifestJson).buildNumber).toBe(buildNumber);
    });
  });
});

// The suite above covers the pure renderer. What actually runs in CI is the
// script's entry point: `node build-version.js`, whose main() decides WHICH of
// the two rendered artifacts goes to WHICH path. Angular's production
// fileReplacements reads src/environments/version.prod.ts and the running app
// fetches src/assets/version.json, so crossing those two writes would ship a
// bundle whose version banner never matches its manifest -- and nothing else in
// the repo would notice, because both files are gitignored build outputs.
//
// main() is deliberately not exported (module.exports exposes only
// renderVersionArtifacts), so the only way in without editing the script is to
// load it the way Node does when it is the program: build a Module, install it
// as process.mainModule, and let the `require.main === module` guard fire.
describe("build-version: main()", () => {
  // A CJS require. It resolves to the same module instances build-version.js
  // itself requires, which is what makes the fs stub below land on the object
  // the script destructures `writeFileSync` out of. (An `import * as fs` would
  // not: esbuild refuses assignment to an import binding, and it would be a
  // different namespace object anyway.)
  //
  // Anchored on the Vitest root -- frontend/, where build-version.js sits --
  // rather than on import.meta.url, which is NOT stable here: the unit-test
  // builder rewrites this spec to a synthetic frontend/spec-build-version.js
  // under `--coverage` and leaves it at src/build-version.spec.ts without, so a
  // "../" relative to it lands in a different directory in each mode.
  const requireCjs = createRequire(join(process.cwd(), "build-version.spec.anchor.cjs"));
  const NodeModule = requireCjs("node:module") as any;
  const fs = requireCjs("fs") as { writeFileSync: unknown };
  const scriptPath: string = requireCjs.resolve("./build-version.js");

  type Write = { path: string; data: string };

  // Runs build-version.js exactly as `node build-version.js` would, with
  // writeFileSync and console.log captured instead of hitting the disk.
  function loadScript(asMain: boolean): { writes: Write[]; logs: string[] } {
    const writes: Write[] = [];
    const logs: string[] = [];

    const realWriteFileSync = fs.writeFileSync;
    const realMainModule = process.mainModule;
    const cachedEntry = NodeModule._cache[scriptPath];
    const logSpy = vi.spyOn(console, "log").mockImplementation((...args: unknown[]) => {
      logs.push(args.join(" "));
    });

    // Stubbed before the load, because the script destructures writeFileSync at
    // module-evaluation time -- a later swap would be invisible to it.
    fs.writeFileSync = (target: unknown, data: unknown) =>
      void writes.push({ path: String(target), data: String(data) });

    try {
      // A fresh Module: the guard compares object identity, so the script must
      // be evaluated again rather than served from the require cache.
      delete NodeModule._cache[scriptPath];
      const scriptModule = new NodeModule(scriptPath, null);
      scriptModule.filename = scriptPath;
      scriptModule.paths = NodeModule._nodeModulePaths(dirname(scriptPath));
      // `require.main` is whatever process.mainModule holds. When the script is
      // NOT the program, some OTHER module is -- that is the situation the guard
      // exists for -- so stand a different module up rather than leaving
      // process.mainModule unset, which would let a guard as loose as
      // `require.main != null` pass for the wrong reason.
      process.mainModule = asMain ? scriptModule : new NodeModule(scriptPath + ".importer.js", null);
      NodeModule._cache[scriptPath] = scriptModule;
      scriptModule.load(scriptPath);
      return { writes, logs };
    } finally {
      fs.writeFileSync = realWriteFileSync;
      process.mainModule = realMainModule;
      if (cachedEntry) {
        NodeModule._cache[scriptPath] = cachedEntry;
      } else {
        delete NodeModule._cache[scriptPath];
      }
      logSpy.mockRestore();
    }
  }

  it("sends the bundle constant and the manifest to the two paths the prod build reads", () => {
    const { writes } = loadScript(true);

    expect(writes.length).toBe(2);

    // Each artifact is matched to its own destination, not merely counted: a
    // version that wrote the right number of files to the right two paths with
    // the contents swapped would still be broken.
    const prodTsWrite = writes.find(w => w.path.endsWith("version.prod.ts"));
    const manifestWrite = writes.find(w => w.path.endsWith("version.json"));
    expect(prodTsWrite).toBeDefined();
    expect(manifestWrite).toBeDefined();

    // The whole path, not a tail of it. A tail match cannot tell
    // frontend/src/environments/version.prod.ts apart from the same suffix one
    // directory up, and only the first is where Angular's fileReplacements
    // looks and where the app fetches the manifest from -- a base directory the
    // script resolved wrongly would ship the static "dev" version silently.
    // Anchored on the script's own directory rather than process.cwd() so it
    // holds in both runner modes.
    expect(prodTsWrite!.path).toBe(join(dirname(scriptPath), "src", "environments", "version.prod.ts"));
    expect(prodTsWrite!.data).toContain("export const Version");
    expect(prodTsWrite!.data).toContain("AUTO-GENERATED");

    expect(manifestWrite!.path).toBe(join(dirname(scriptPath), "src", "assets", "version.json"));
    const manifest = JSON.parse(manifestWrite!.data);

    // The version comes from package.json, not from a literal in the script.
    const pkgVersion: string = requireCjs("./package.json").version;
    expect(manifest.version).toBe(pkgVersion);

    // An anchor that does NOT come out of main()'s own output. Every other
    // assertion here reads the build number back from what main() wrote, so
    // they agree with each other no matter what it was; only this one says what
    // the value has to BE. The generator's contract is `<version>.<digits>`, so
    // a main() that passed a constant, or passed the version as its own build
    // number, fails here and nowhere else.
    expect(manifest.buildNumber.startsWith(`${pkgVersion}.`)).toBe(true);
    expect(manifest.buildNumber.slice(pkgVersion.length + 1)).toMatch(/^\d+$/);

    // The comparison the running app makes: the number baked into the bundle
    // and the number served in the manifest have to be the same one.
    expect(prodTsWrite!.data).toContain(JSON.stringify(manifest.buildNumber));
    expect(prodTsWrite!.data).toContain(JSON.stringify(manifest.version));
  });

  it("announces the build number it produced", () => {
    const { writes, logs } = loadScript(true);
    const manifest = JSON.parse(writes.find(w => w.path.endsWith("version.json"))!.data);
    expect(logs).toEqual([`build-version: ${manifest.buildNumber}`]);
  });

  it("writes nothing when the script is merely required rather than run", () => {
    // The other half of the `require.main === module` guard: importing the
    // module for its renderer -- which this spec's own top-level import does --
    // must not touch the working tree.
    const { writes, logs } = loadScript(false);
    expect(writes).toEqual([]);
    expect(logs).toEqual([]);
  });
});
