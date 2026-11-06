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

/**
 * Empties jsdom's CSS-selector cache between tests.
 *
 * jsdom answers `querySelector` through nwsapi, which memoizes every selector together with the
 * nodes it last matched, for the lifetime of the document. The unit-test builder runs a worker's
 * spec files against one document, and TestBed queries a fresh `#root<N>` per fixture, so every
 * finished fixture, and through its `__ngContext__` the whole TestBed graph, stays cached until the
 * worker exits. The tables are emptied in place because jsdom's engine reads the exported objects.
 *
 * `beforeEach` runs after the previous test's TestBed teardown, whose own queries would otherwise be
 * cached again; `afterAll` covers the file's last test. This is a setupFile (see `vitest.config.ts`).
 */
type NwsapiCaches = { selectResolvers: Record<string, unknown>; matchResolvers: Record<string, unknown> };

function resetSelectorCache(): void {
  const document = globalThis.document as unknown as Record<symbol, { _nwsapi?: NwsapiCaches } | undefined>;
  if (!document) return;
  // jsdom keeps its implementation object behind the DOM wrapper, under a symbol it does not export.
  const implSymbol = Object.getOwnPropertySymbols(document).find(symbol => symbol.description === "impl");
  const nwsapi = implSymbol && document[implSymbol]?._nwsapi;
  if (!nwsapi) return;
  for (const cache of [nwsapi.selectResolvers, nwsapi.matchResolvers]) {
    for (const selector of Object.keys(cache)) delete cache[selector];
  }
}

beforeEach(resetSelectorCache);
afterAll(resetSelectorCache);
