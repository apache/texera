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

import * as Y from "yjs";
import { createYTypeFromObject, updateYTypeFromObject, YType } from "./shared-editing.interface";

/**
 * Attaches a freshly created YType to a real Y.Doc so that subsequent in-place
 * mutations behave exactly as they would in production, then returns it.
 */
function attach<T extends object>(doc: Y.Doc, key: string, yValue: YType<T>): YType<T> {
  const container = doc.getMap("container");
  container.set(key, yValue as unknown);
  return container.get(key) as unknown as YType<T>;
}

describe("createYTypeFromObject", () => {
  it("returns primitive numbers and booleans unchanged", () => {
    expect(createYTypeFromObject(42 as any)).toBe(42);
    expect(createYTypeFromObject(true as any)).toBe(true);
    expect(createYTypeFromObject(false as any)).toBe(false);
  });

  it("returns null and undefined as-is", () => {
    expect(createYTypeFromObject(null as any)).toBeNull();
    expect(createYTypeFromObject(undefined as any)).toBeUndefined();
  });

  it("converts a string into a Y.Text carrying the same content", () => {
    const doc = new Y.Doc();
    const created = createYTypeFromObject("hello" as any);
    expect(created).toBeInstanceOf(Y.Text);
    const yText = attach(doc, "s", created) as unknown as Y.Text;
    expect(yText.toJSON()).toBe("hello");
  });

  it("converts an array into a Y.Array preserving order and values", () => {
    const doc = new Y.Doc();
    const created = createYTypeFromObject([1, 2, 3]);
    expect(created).toBeInstanceOf(Y.Array);
    const yArray = attach(doc, "arr", created) as unknown as Y.Array<any>;
    expect(yArray.toJSON()).toEqual([1, 2, 3]);
  });

  it("skips undefined entries when building a Y.Array", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "arr", createYTypeFromObject([1, undefined, 2] as any)) as unknown as Y.Array<any>;
    expect(yArray.toJSON()).toEqual([1, 2]);
  });

  it("converts a plain object into a Y.Map whose string values become Y.Text", () => {
    const doc = new Y.Doc();
    const created = createYTypeFromObject({ count: 1, label: "s" });
    expect(created).toBeInstanceOf(Y.Map);
    const yMap = attach(doc, "m", created) as unknown as Y.Map<any>;
    expect(yMap.get("count")).toBe(1);
    expect(yMap.get("label")).toBeInstanceOf(Y.Text);
    expect(yMap.toJSON()).toEqual({ count: 1, label: "s" });
  });

  it("skips undefined object values", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ a: 1, b: undefined } as any)) as unknown as Y.Map<any>;
    expect(yMap.has("a")).toBe(true);
    expect(yMap.has("b")).toBe(false);
    expect(yMap.toJSON()).toEqual({ a: 1 });
  });

  it("recursively converts nested objects and arrays", () => {
    const doc = new Y.Doc();
    const yMap = attach(
      doc,
      "m",
      createYTypeFromObject({
        name: "n",
        tags: ["a", "b"],
        meta: { count: 2, active: true },
      })
    ) as unknown as Y.Map<any>;

    expect(yMap.get("name")).toBeInstanceOf(Y.Text);
    expect(yMap.get("tags")).toBeInstanceOf(Y.Array);
    expect(yMap.get("meta")).toBeInstanceOf(Y.Map);
    expect((yMap.get("meta") as Y.Map<any>).get("name" as any)).toBeUndefined();
    expect(yMap.toJSON()).toEqual({ name: "n", tags: ["a", "b"], meta: { count: 2, active: true } });
  });

  it("throws a TypeError for unsupported object kinds", () => {
    expect(() => createYTypeFromObject(new Date())).toThrow(TypeError);
  });
});

describe("updateYTypeFromObject", () => {
  it("returns false for null or undefined arguments", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ a: 1 }));
    expect(updateYTypeFromObject(yMap, null as any)).toBe(false);
    expect(updateYTypeFromObject(null as any, { a: 1 })).toBe(false);
  });

  it("returns false when the top-level new value is a primitive", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ a: 1 }));
    expect(updateYTypeFromObject(yMap, 5 as any)).toBe(false);
    expect(updateYTypeFromObject(yMap, true as any)).toBe(false);
  });

  it("returns false and leaves the value untouched on a structural type mismatch", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "arr", createYTypeFromObject([1, 2]));
    expect(updateYTypeFromObject(yArray, { a: 1 } as any)).toBe(false);
    expect((yArray as unknown as Y.Array<any>).toJSON()).toEqual([1, 2]);
  });

  it("updates a Y.Map's scalar and string fields in place", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ a: 1, b: "hello" }));
    const originalText = (yMap as unknown as Y.Map<any>).get("b") as Y.Text;

    const result = updateYTypeFromObject(yMap, { a: 2, b: "world" });

    expect(result).toBe(true);
    expect((yMap as unknown as Y.Map<any>).toJSON()).toEqual({ a: 2, b: "world" });
    // The Y.Text was mutated in place rather than replaced.
    expect((yMap as unknown as Y.Map<any>).get("b")).toBe(originalText);
    expect(originalText.toJSON()).toBe("world");
  });

  it("adds new object keys but leaves keys absent from the new object in place", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ keep: 1, drop: 2 }));

    updateYTypeFromObject(yMap, { keep: 1, add: 3 } as any);

    const asMap = yMap as unknown as Y.Map<any>;
    expect(asMap.get("keep")).toBe(1);
    // A key present only in the new object is added.
    expect(asMap.has("add")).toBe(true);
    expect(asMap.get("add")).toBe(3);
    // A key omitted from the new object is retained (an undefined new value never deletes).
    expect(asMap.get("drop")).toBe(2);
  });

  it("appends new items to a Y.Array in place", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "arr", createYTypeFromObject([1, 2, 3]));

    updateYTypeFromObject(yArray, [1, 2, 3, 4]);

    expect((yArray as unknown as Y.Array<any>).toJSON()).toEqual([1, 2, 3, 4]);
  });

  it("deletes removed items from a Y.Array in place", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "arr", createYTypeFromObject([1, 2, 3]));

    updateYTypeFromObject(yArray, [1, 3]);

    expect((yArray as unknown as Y.Array<any>).toJSON()).toEqual([1, 3]);
  });

  it("mutates an existing array element in place when it stays an object", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "arr", createYTypeFromObject([{ id: 1, v: "a" }]));
    const originalElement = (yArray as unknown as Y.Array<any>).get(0) as Y.Map<any>;

    updateYTypeFromObject(yArray, [{ id: 1, v: "b" }]);

    const asArray = yArray as unknown as Y.Array<any>;
    expect(asArray.toJSON()).toEqual([{ id: 1, v: "b" }]);
    // The nested Y.Map element is reused, not swapped out.
    expect(asArray.get(0)).toBe(originalElement);
  });

  it("recursively updates deeply nested structures", () => {
    const doc = new Y.Doc();
    const yObj = attach(doc, "o", createYTypeFromObject({ user: { name: "alice", roles: ["admin"] }, active: true }));

    updateYTypeFromObject(yObj, { user: { name: "bob", roles: ["admin", "dev"] }, active: false });

    expect((yObj as unknown as Y.Map<any>).toJSON()).toEqual({
      user: { name: "bob", roles: ["admin", "dev"] },
      active: false,
    });
  });
});
