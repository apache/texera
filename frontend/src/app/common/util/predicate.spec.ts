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

import { isDefined, isNonNullObject } from "./predicate";

describe("isNonNullObject", () => {
  it("should return true for plain objects", () => {
    expect(isNonNullObject({})).toBe(true);
    expect(isNonNullObject({ a: 1 })).toBe(true);
  });

  it("should return true for arrays", () => {
    // Arrays intentionally pass: the predicate checks "non-null object" in the
    // `typeof` sense, and `typeof [] === "object"`.
    expect(isNonNullObject([])).toBe(true);
    expect(isNonNullObject([1, 2, 3])).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isNonNullObject(null)).toBe(false);
    expect(isNonNullObject(undefined)).toBe(false);
  });

  it("should return false for primitives", () => {
    expect(isNonNullObject(42)).toBe(false);
    expect(isNonNullObject("string")).toBe(false);
    expect(isNonNullObject(true)).toBe(false);
  });

  it("should return false for functions", () => {
    // `typeof fn === "function"`, not "object", so functions are rejected.
    expect(isNonNullObject(() => {})).toBe(false);
  });
});

describe("isDefined", () => {
  it("should return true for defined values", () => {
    expect(isDefined(42)).toBe(true);
    expect(isDefined("string")).toBe(true);
    expect(isDefined({})).toBe(true);
  });

  it("should return true for falsy but defined values", () => {
    expect(isDefined(0)).toBe(true);
    expect(isDefined("")).toBe(true);
    expect(isDefined(false)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDefined(null)).toBe(false);
    expect(isDefined(undefined)).toBe(false);
  });
});
