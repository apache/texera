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

import { assert, asType, isType } from "./assert";

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
