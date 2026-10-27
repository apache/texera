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

/** The two memo tables nwsapi exports on its engine object. */
type NwsapiCaches = {
  selectResolvers: Record<string, unknown>;
  matchResolvers: Record<string, unknown>;
};

/**
 * Empties jsdom's CSS-selector cache.
 *
 * jsdom answers `querySelector` and `matches` through nwsapi, which memoizes every selector it
 * compiles together with the nodes that selector last matched, for the lifetime of the document.
 * The unit-test builder runs every spec file a worker picks up against one document, and TestBed
 * queries a fresh `#root<N>` selector for each fixture it creates. So every fixture's root element
 * stays cached, and with it, through the element's `__ngContext__`, the component tree, its
 * injector and its NgZone, until the worker exits.
 *
 * The tables are emptied in place: jsdom's engine reads the exported objects, so replacing them
 * would release nothing. Returns false when the cache is not there to empty: before the document's
 * first query, or if jsdom stops using nwsapi. worker-memory.spec.ts fails in the latter case, so
 * this gets revisited instead of silently turning into a no-op.
 */
export function resetJsdomSelectorCache(): boolean {
  const document = globalThis.document as unknown as Record<symbol, { _nwsapi?: NwsapiCaches } | undefined>;
  if (!document) return false;
  // jsdom keeps its implementation object behind the DOM wrapper, under a symbol it does not export.
  const implSymbol = Object.getOwnPropertySymbols(document).find(symbol => symbol.description === "impl");
  const nwsapi = implSymbol && document[implSymbol]?._nwsapi;
  if (!nwsapi) return false;
  for (const cache of [nwsapi.selectResolvers, nwsapi.matchResolvers]) {
    for (const selector of Object.keys(cache)) delete cache[selector];
  }
  return true;
}
