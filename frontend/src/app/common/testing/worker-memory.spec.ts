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

import { setFlagsFromString } from "node:v8";
import { runInNewContext } from "node:vm";
import { resetJsdomSelectorCache } from "./jsdom-selector-cache";

/**
 * The unit-test builder runs every spec file a worker picks up in one process, against one jsdom
 * document and one copy of Vitest. Anything worker-wide that holds on to a test's objects keeps
 * that test's whole TestBed graph alive until the worker exits. On the 7 GB macOS runners that
 * growth stalled the VM long enough to time unrelated tests out. These pin the two worker-wide
 * holders the suite depends on being released.
 */

// Workers run without --expose-gc, but V8 still hands `gc` to contexts created after the flag is set.
setFlagsFromString("--expose-gc");
const gc = runInNewContext("gc") as () => void;

/** A WeakRef keeps its target alive until the task that created it ends, so collect in a later one. */
async function collectGarbage(): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, 0));
  gc();
}

describe("memory shared by the spec files of a test worker", () => {
  it("a mock does not keep what it was called with alive once nothing references the mock", async () => {
    const argumentRef = (() => {
      const argument = {};
      vi.fn()(argument);
      return new WeakRef(argument);
    })();

    await collectGarbage();

    // Vitest 4's mock registry is a strong Set that is never pruned. The patch in
    // .yarn/patches backports Vitest 5's weak one; this fails if a version bump drops the patch.
    expect(argumentRef.deref()).toBeUndefined();
  });

  it("jsdom releases a queried element once its selector cache is reset", async () => {
    const elementRef = (() => {
      const element = document.body.appendChild(document.createElement("div"));
      element.id = "worker-memory-probe";
      expect(document.querySelector("#worker-memory-probe")).toBe(element);
      element.remove();
      return new WeakRef(element);
    })();

    await collectGarbage();
    // still held by the cached query, which is why the reset exists
    expect(elementRef.deref()).toBeDefined();

    expect(resetJsdomSelectorCache()).toBe(true);
    await collectGarbage();
    expect(elementRef.deref()).toBeUndefined();
  });
});
