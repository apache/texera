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

  it("adds new object keys and removes the ones the new object no longer carries", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ keep: 1, drop: 2 }));

    updateYTypeFromObject(yMap, { keep: 1, add: 3 } as any);

    const asMap = yMap as unknown as Y.Map<any>;
    expect(asMap.get("keep")).toBe(1);
    // A key present only in the new object is added.
    expect(asMap.has("add")).toBe(true);
    expect(asMap.get("add")).toBe(3);
    // A key the new object has dropped is deleted rather than left behind. Clearing
    // an operator property is exactly this: the editor sends the properties without
    // it, and anything retained here is a value the user removed and can no longer see.
    expect(asMap.has("drop")).toBe(false);
  });

  it("keeps a key whose new value is explicitly undefined", () => {
    const doc = new Y.Doc();
    const yMap = attach(doc, "m", createYTypeFromObject({ a: 1 }));

    updateYTypeFromObject(yMap, { a: undefined } as any);

    // Carrying the key with an undefined value is not the same as dropping it, and
    // only the latter means removal.
    expect((yMap as unknown as Y.Map<any>).get("a")).toBe(1);
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

/**
 * The blocks above drive the shapes production actually stores. These cover the arms that
 * only a boxed String, a `typeof` the switch lists but no caller passes, or a particular
 * array alignment can reach.
 */
describe("createYTypeFromObject edge kinds", () => {
  it("returns values of the non-storable primitive kinds unchanged", () => {
    const fn = () => 0;
    const sym = Symbol("s");
    const big = BigInt(9);

    expect(createYTypeFromObject(fn as any)).toBe(fn);
    expect(createYTypeFromObject(sym as any)).toBe(sym);
    expect(createYTypeFromObject(big as any)).toBe(big);
  });

  it("converts a boxed String object into a Y.Text, losing its content", () => {
    // `typeof` a boxed String is "object", so it reaches the constructor-name check rather
    // than the "string" case above it.
    const yText = createYTypeFromObject(new String("boxed") as unknown as object) as unknown as Y.Text;

    expect(yText).toBeInstanceOf(Y.Text);
    // Characterizing a defect, not an intent: Y.Text's constructor only accepts a primitive,
    // so the boxed value is dropped and the text comes out empty. Change this to "boxed"
    // when the branch unwraps the box (`String(obj)`) before constructing the Y.Text.
    expect(yText.toJSON()).toBe("");
  });
});

describe("updateYTypeFromObject edge kinds", () => {
  it("returns false for the non-storable primitive kinds", () => {
    const doc = new Y.Doc();
    const yText = attach(doc, "t", createYTypeFromObject("hi" as unknown as object));

    expect(updateYTypeFromObject(yText, (() => 0) as any)).toBe(false);
    expect(updateYTypeFromObject(yText, Symbol("s") as any)).toBe(false);
    expect(updateYTypeFromObject(yText, BigInt(9) as any)).toBe(false);
  });

  it("leaves a Y.Text untouched when the new string is identical", () => {
    const doc = new Y.Doc();
    const yText = attach(doc, "t", createYTypeFromObject("same" as unknown as object)) as unknown as Y.Text;
    const before = Y.encodeStateAsUpdate(doc);

    expect(updateYTypeFromObject(yText as unknown as YType<object>, "same" as any)).toBe(true);

    expect(yText.toJSON()).toBe("same");
    // An in-place delete+insert would have produced new document state.
    expect(Y.encodeStateAsUpdate(doc)).toEqual(before);
  });

  it("updates a Y.Text from a boxed String", () => {
    const doc = new Y.Doc();
    const yText = attach(doc, "t", createYTypeFromObject("old" as unknown as object)) as unknown as Y.Text;

    // Unlike the constructor, `insert` coerces the boxed value, so this arm does carry the
    // content through.
    expect(updateYTypeFromObject(yText as unknown as YType<object>, new String("new") as any)).toBe(true);

    expect(yText.toJSON()).toBe("new");
  });

  it("stores an undefined array entry as null", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "a", createYTypeFromObject(["keep"])) as unknown as Y.Array<any>;

    expect(updateYTypeFromObject(yArray as unknown as YType<object>, ["keep", undefined] as any)).toBe(true);

    expect(yArray.toJSON()).toEqual(["keep", null]);
  });

  it("replaces an array element outright when its kind changes", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "a", createYTypeFromObject([{ a: 1 }, "tail"])) as unknown as Y.Array<any>;

    // A number cannot be updated into the Y.Map that held the object, so the element is
    // deleted and re-inserted rather than mutated.
    expect(updateYTypeFromObject(yArray as unknown as YType<object>, [7, "tail"] as any)).toBe(true);

    expect(yArray.toJSON()).toEqual([7, "tail"]);
  });

  it("aligns an array whose additions sit before its removals", () => {
    const doc = new Y.Doc();
    const yArray = attach(doc, "a", createYTypeFromObject(["a", "b", "c"])) as unknown as Y.Array<any>;

    // The longest common subsequence is "a","c"; reaching it forces the walk through both
    // of its advance arms.
    expect(updateYTypeFromObject(yArray as unknown as YType<object>, ["a", "x", "y", "c"] as any)).toBe(true);

    expect(yArray.toJSON()).toEqual(["a", "x", "y", "c"]);
  });
});

describe("updateYTypeFromObject unsupported kinds", () => {
  it("returns false for a matching type it has no merge strategy for", () => {
    // Both sides report the same constructor, so the type-mismatch guard lets them through,
    // but the dispatch below only knows String, Array and Object.
    const oldStandIn = { toJSON: () => new Date(0) } as unknown as YType<object>;

    expect(updateYTypeFromObject(oldStandIn, new Date(1) as unknown as object)).toBe(false);
  });
});

describe("updateYTypeFromObject identical boxed strings", () => {
  it("writes nothing when the new value is the very String object already stored", () => {
    // The String branch is only reachable for a boxed String, and a boxed String never
    // compares equal to the primitive a Y.Text reports — so the only way to reach the
    // "already up to date" arm is for both sides to be the same object. A stand-in whose
    // toJSON returns that object is what makes the comparison meet.
    const boxed = new String("same");
    const oldStandIn = { toJSON: () => boxed } as unknown as YType<object>;

    expect(updateYTypeFromObject(oldStandIn, boxed as unknown as object)).toBe(true);
  });
});
