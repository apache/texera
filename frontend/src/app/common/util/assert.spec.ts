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

import { assert, assertType, asType, isNotNull, isNull, isType, nonNull } from "./assert";

describe("assertType", () => {
  it("does not throw for defined values, including falsy ones", () => {
    expect(() => assertType<number>(0)).not.toThrow();
    expect(() => assertType<string>("")).not.toThrow();
    expect(() => assertType<boolean>(false)).not.toThrow();
    expect(() => assertType<object>({})).not.toThrow();
  });

  it("throws a TypeError for null", () => {
    expect(() => assertType<number>(null)).toThrow(TypeError);
  });

  it("throws a TypeError for undefined", () => {
    expect(() => assertType<number>(undefined)).toThrow(TypeError);
  });

  it("reports the received value in the error message", () => {
    expect(() => assertType<number>(null)).toThrow("received null");
    expect(() => assertType<number>(undefined)).toThrow("received undefined");
  });
});

describe("assert", () => {
  it("does not throw when the condition is true", () => {
    expect(() => assert(true)).not.toThrow();
  });

  it("throws an Error when the condition is false", () => {
    expect(() => assert(false)).toThrow(Error);
  });

  it("uses the supplied message on failure", () => {
    expect(() => assert(false, "boom")).toThrow("boom");
  });

  it("throws with an empty message when none is provided", () => {
    let err: unknown;
    try {
      assert(false);
    } catch (e) {
      err = e;
    }
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toBe("");
  });
});

describe("isType", () => {
  it("matches primitive types by typeof", () => {
    expect(isType(1, "number")).toBe(true);
    expect(isType("x", "string")).toBe(true);
    expect(isType(true, "boolean")).toBe(true);
  });

  it("returns false when the primitive type does not match", () => {
    expect(isType("1", "number")).toBe(false);
    expect(isType(1, "string")).toBe(false);
    expect(isType(0, "boolean")).toBe(false);
  });

  it("matches instances via instanceof for constructor types", () => {
    expect(isType(new Date(), Date)).toBe(true);
    expect(isType([], Array)).toBe(true);
  });

  it("returns false for instances of an unrelated constructor", () => {
    expect(isType({}, Date)).toBe(false);
    expect(isType(new Date(), Array)).toBe(false);
  });
});

describe("asType", () => {
  it("returns the value unchanged when the type matches", () => {
    expect(asType(5, "number")).toBe(5);
    const date = new Date();
    expect(asType(date, Date)).toBe(date);
  });

  it("throws a TypeError when the type does not match", () => {
    expect(() => asType("nope", "number")).toThrow(TypeError);
    expect(() => asType({}, Date)).toThrow(TypeError);
  });
});

describe("isNull", () => {
  it("returns true for null and undefined", () => {
    expect(isNull(null)).toBe(true);
    expect(isNull(undefined)).toBe(true);
  });

  it("returns false for defined values, including falsy ones", () => {
    expect(isNull(0)).toBe(false);
    expect(isNull("")).toBe(false);
    expect(isNull(false)).toBe(false);
  });
});

describe("isNotNull", () => {
  it("returns false for null and undefined", () => {
    expect(isNotNull(null)).toBe(false);
    expect(isNotNull(undefined)).toBe(false);
  });

  it("returns true for defined values, including falsy ones", () => {
    expect(isNotNull(0)).toBe(true);
    expect(isNotNull("")).toBe(true);
    expect(isNotNull(false)).toBe(true);
  });

  it("is the logical inverse of isNull", () => {
    for (const value of [null, undefined, 0, "", false, {}, []]) {
      expect(isNotNull(value)).toBe(!isNull(value));
    }
  });
});

describe("nonNull", () => {
  it("returns the value unchanged for defined values", () => {
    expect(nonNull(0)).toBe(0);
    expect(nonNull("")).toBe("");
    const obj = { a: 1 };
    expect(nonNull(obj)).toBe(obj);
  });

  it("throws a TypeError for null", () => {
    expect(() => nonNull(null)).toThrow(TypeError);
  });

  it("throws a TypeError for undefined", () => {
    expect(() => nonNull(undefined)).toThrow(TypeError);
  });
});
