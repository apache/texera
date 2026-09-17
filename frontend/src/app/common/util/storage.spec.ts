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

import {
  jsonCast,
  localGetObject,
  localRemoveObject,
  localSetObject,
  sessionGetObject,
  sessionRemoveObject,
  sessionSetObject,
} from "./storage";

/**
 * Builds a Map-backed stand-in for the Web Storage API so the tests can
 * observe reads/writes without depending on the environment's real storage.
 */
function createMockStorage() {
  const store = new Map<string, string>();
  return {
    setItem: vi.fn((key: string, value: string) => {
      store.set(key, value);
    }),
    getItem: vi.fn((key: string): string | null => (store.has(key) ? (store.get(key) as string) : null)),
    removeItem: vi.fn((key: string) => {
      store.delete(key);
    }),
    _store: store,
  };
}

describe("storage util", () => {
  let localMock: ReturnType<typeof createMockStorage>;
  let sessionMock: ReturnType<typeof createMockStorage>;

  beforeEach(() => {
    localMock = createMockStorage();
    sessionMock = createMockStorage();
    vi.stubGlobal("localStorage", localMock);
    vi.stubGlobal("sessionStorage", sessionMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe("jsonCast", () => {
    it("parses a JSON string back into its original value", () => {
      expect(jsonCast<{ a: number }>('{"a":1}')).toEqual({ a: 1 });
      expect(jsonCast<number[]>("[1,2,3]")).toEqual([1, 2, 3]);
    });

    it("throws when the input is not valid JSON", () => {
      expect(() => jsonCast<unknown>("not json")).toThrow();
    });
  });

  describe("localStorage helpers", () => {
    it("stringifies the object when setting", () => {
      localSetObject("key", { hello: "world", n: 2 });
      expect(localMock.setItem).toHaveBeenCalledWith("key", '{"hello":"world","n":2}');
    });

    it("round-trips an object through set then get", () => {
      const value = { id: 7, tags: ["a", "b"], nested: { ok: true } };
      localSetObject("obj", value);
      expect(localGetObject<typeof value>("obj")).toEqual(value);
    });

    it("returns undefined when the key is absent", () => {
      expect(localGetObject("missing")).toBeUndefined();
      expect(localMock.getItem).toHaveBeenCalledWith("missing");
    });

    it("returns undefined for an empty-string value (falsy guard)", () => {
      localMock._store.set("empty", "");
      expect(localGetObject("empty")).toBeUndefined();
    });

    it("removes the stored key", () => {
      localSetObject("gone", 123);
      localRemoveObject("gone");
      expect(localMock.removeItem).toHaveBeenCalledWith("gone");
      expect(localGetObject("gone")).toBeUndefined();
    });

    it("preserves falsy-but-defined values such as false and 0", () => {
      localSetObject("flag", false);
      expect(localGetObject<boolean>("flag")).toBe(false);
      localSetObject("zero", 0);
      expect(localGetObject<number>("zero")).toBe(0);
    });
  });

  describe("sessionStorage helpers", () => {
    it("stringifies the object when setting", () => {
      sessionSetObject("key", [1, 2]);
      expect(sessionMock.setItem).toHaveBeenCalledWith("key", "[1,2]");
    });

    it("round-trips an object through set then get", () => {
      const value = { user: "texera", roles: ["admin"] };
      sessionSetObject("obj", value);
      expect(sessionGetObject<typeof value>("obj")).toEqual(value);
    });

    it("returns null (not undefined) when the key is absent", () => {
      expect(sessionGetObject("missing")).toBeNull();
    });

    it("removes the stored key", () => {
      sessionSetObject("gone", "x");
      sessionRemoveObject("gone");
      expect(sessionMock.removeItem).toHaveBeenCalledWith("gone");
      expect(sessionGetObject("gone")).toBeNull();
    });
  });
});
