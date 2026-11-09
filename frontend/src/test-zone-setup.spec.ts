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

import { fakeAsync, tick, waitForAsync } from "@angular/core/testing";

/**
 * Guards the ProxyZone that `test-zone-setup.ts` installs around every spec.
 *
 * The flake it fixes (RangeError from unbounded proxy nesting, see
 * apache/texera#6593) only surfaces once enough spec files have run on one
 * Vitest worker, so it is not reproducible from a single spec. These tests
 * instead pin the invariant that prevents it: the proxy is forked directly
 * from the root zone (exactly one level deep) and stays functional for
 * Angular's fakeAsync/waitForAsync, both of which require an active ProxyZone.
 */

// `Zone` is a global installed by `zone.js/testing`. Declare the slice used here.
declare const Zone: {
  root: unknown;
  current: { parent: unknown; get: (key: string) => unknown };
};

describe("test-zone-setup", () => {
  it("runs each spec body inside a ProxyZone forked directly from the root zone", () => {
    // A ProxyZoneSpec must be in scope, or fakeAsync/waitForAsync would throw
    // "Expected to be running in 'ProxyZone'".
    expect(Zone.current.get("ProxyZoneSpec")).toBeTruthy();
    // Forked from Zone.root => parent is the root zone => depth is bounded to
    // one regardless of how many spec files ran before this one.
    expect(Zone.current.parent).toBe(Zone.root);
  });

  it("supports fakeAsync, which requires an active ProxyZone", fakeAsync(() => {
    let fired = false;
    setTimeout(() => (fired = true), 100);
    expect(fired).toBe(false);
    tick(100);
    expect(fired).toBe(true);
  }));

  it("supports waitForAsync, which requires an active ProxyZone", waitForAsync(() => {
    // waitForAsync calls ProxyZoneSpec.assertPresent() and swaps the proxy's
    // delegate; reaching the assertion at all proves the proxy is present and
    // its delegate is restored cleanly afterwards.
    Promise.resolve().then(() => expect(true).toBe(true));
  }));
});
