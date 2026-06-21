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

import { ErrorHandler } from "@angular/core";
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

  it("detects chunk-load failures from a plain object message", () => {
    expect(isChunkLoadError({ message: "Loading chunk 12 failed." })).toBe(true);
  });

  it("matches case-insensitively", () => {
    expect(isChunkLoadError("CHUNKLOADERROR")).toBe(true);
    expect(isChunkLoadError(new Error("Error loading Dynamically Imported Module"))).toBe(true);
  });

  it("ignores unrelated errors", () => {
    expect(isChunkLoadError(new Error("something broke"))).toBe(false);
    expect(isChunkLoadError(new TypeError("x is not a function"))).toBe(false);
    expect(isChunkLoadError(null)).toBe(false);
    expect(isChunkLoadError(undefined)).toBe(false);
    expect(isChunkLoadError({})).toBe(false);
  });

  it("ignores errors whose name matches loosely but is not ChunkLoadError", () => {
    expect(isChunkLoadError({ name: "TypeError", message: "boom" })).toBe(false);
  });

  it("ignores values with a non-string message and non-string body", () => {
    expect(isChunkLoadError({ message: 42 })).toBe(false);
    expect(isChunkLoadError({ message: { nested: "Loading chunk 1 failed." } })).toBe(false);
    expect(isChunkLoadError(1234)).toBe(false);
    expect(isChunkLoadError(true)).toBe(false);
  });
});

describe("GlobalErrorHandler", () => {
  let handler: TestableGlobalErrorHandler;
  let defaultHandlerSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    sessionStorage.clear();
    // Suppress (and observe) the Angular default handler's console.error.
    defaultHandlerSpy = vi.spyOn(ErrorHandler.prototype, "handleError").mockImplementation(() => {});
    handler = new TestableGlobalErrorHandler();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    sessionStorage.clear();
  });

  it("reloads once on a chunk-load error and records the guard", () => {
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
    expect(sessionStorage.getItem(RELOAD_GUARD_KEY)).not.toBeNull();
  });

  it("does not forward a recovered chunk error to the default handler", () => {
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(defaultHandlerSpy).not.toHaveBeenCalled();
  });

  it("does not reload again within the guard window", () => {
    handler.handleError(new Error("Loading chunk 3 failed."));
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
  });

  it("forwards a chunk error to the default handler once reload is guarded", () => {
    handler.handleError(new Error("Loading chunk 3 failed.")); // reloads, no forward
    handler.handleError(new Error("Loading chunk 3 failed.")); // guarded -> forwards
    expect(handler.reloadCount).toBe(1);
    expect(defaultHandlerSpy).toHaveBeenCalledTimes(1);
  });

  it("does not reload on a non-chunk error and forwards it to the default handler", () => {
    const error = new Error("totally unrelated");
    handler.handleError(error);
    expect(handler.reloadCount).toBe(0);
    expect(defaultHandlerSpy).toHaveBeenCalledWith(error);
  });

  it("reloads again once the guard window has elapsed", () => {
    // A guard timestamp far in the past is treated as expired.
    sessionStorage.setItem(RELOAD_GUARD_KEY, "1000");
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
  });

  it("does not reload when a fresh guard timestamp is present", () => {
    sessionStorage.setItem(RELOAD_GUARD_KEY, String(Date.now()));
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(0);
  });

  it("reloads when the stored guard value is not a usable number", () => {
    sessionStorage.setItem(RELOAD_GUARD_KEY, "not-a-number");
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
  });

  it("reloads when the stored guard value is non-positive", () => {
    sessionStorage.setItem(RELOAD_GUARD_KEY, "0");
    handler.handleError(new Error("Loading chunk 3 failed."));
    expect(handler.reloadCount).toBe(1);
  });
});

describe("GlobalErrorHandler.reload (default implementation)", () => {
  beforeEach(() => {
    sessionStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    sessionStorage.clear();
  });

  it("delegates to window.location.reload on a chunk-load error", () => {
    // jsdom marks window.location.reload non-configurable, so swap the
    // whole location object for one with a stubbed reload, then restore it.
    const originalLocation = window.location;
    const reloadMock = vi.fn();
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, reload: reloadMock },
    });
    try {
      const handler = new GlobalErrorHandler();
      handler.handleError(new Error("Loading chunk 7 failed."));
      expect(reloadMock).toHaveBeenCalledTimes(1);
    } finally {
      Object.defineProperty(window, "location", { configurable: true, value: originalLocation });
    }
  });
});
