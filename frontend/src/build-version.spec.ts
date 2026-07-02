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
