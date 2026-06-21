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

import { GlobalErrorHandler, RELOAD_GUARD_KEY, isChunkLoadError } from "./global-error-handler.service";

// Records reloads instead of navigating, so the guard logic is observable.
class TestableGlobalErrorHandler extends GlobalErrorHandler {
  public reloadCount = 0;
  protected override reload(): void {
    this.reloadCount++;
  }
}

describe("isChunkLoadError", () => {
  it("detects chunk-load failures", () => {
    expect(isChunkLoadError({ name: "ChunkLoadError" })).toBe(true);
    expect(isChunkLoadError(new Error("Loading chunk 5 failed."))).toBe(true);
    expect(isChunkLoadError(new Error("Failed to fetch dynamically imported module: http://x/y.js"))).toBe(true);
    expect(isChunkLoadError("ChunkLoadError: Loading chunk vendors failed")).toBe(true);
  });

  it("ignores unrelated errors", () => {
    expect(isChunkLoadError(new Error("something broke"))).toBe(false);
    expect(isChunkLoadError(new TypeError("x is not a function"))).toBe(false);
    expect(isChunkLoadError(null)).toBe(false);
    expect(isChunkLoadError(undefined)).toBe(false);
    expect(isChunkLoadError({})).toBe(false);
  });
});

describe("GlobalErrorHandler", () => {
  let handler: TestableGlobalErrorHandler;

  beforeEach(() => {
    sessionStorage.clear();
    handler = new TestableGlobalErrorHandler();
  });

  it("reloads once on a chunk-load error and records the guard", () => {
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
    expect(sessionStorage.getItem(RELOAD_GUARD_KEY)).not.toBeNull();
  });

  it("does not reload again within the guard window", () => {
    handler.handleError(new Error("Loading chunk 3 failed."));
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
  });

  it("does not reload on a non-chunk error", () => {
    handler.handleError(new Error("totally unrelated"));
    expect(handler.reloadCount).toBe(0);
  });
});
