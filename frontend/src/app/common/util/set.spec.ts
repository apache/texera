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

import { intersection } from "./set";

describe("intersection", () => {
  it("returns only the elements common to both sets", () => {
    const a = new Set([1, 2, 3, 4]);
    const b = new Set([3, 4, 5, 6]);
    expect(intersection(a, b)).toEqual(new Set([3, 4]));
  });

  it("returns an empty set when there are no common elements", () => {
    expect(intersection(new Set([1, 2]), new Set([3, 4]))).toEqual(new Set());
  });

  it("returns an empty set when either input is empty", () => {
    expect(intersection(new Set(), new Set([1, 2]))).toEqual(new Set());
    expect(intersection(new Set([1, 2]), new Set())).toEqual(new Set());
  });

  it("works with string elements", () => {
    const a = new Set(["x", "y", "z"]);
    const b = new Set(["y", "z", "w"]);
    expect(intersection(a, b)).toEqual(new Set(["y", "z"]));
  });

  it("uses reference identity for object elements (SameValueZero)", () => {
    const shared = { id: 1 };
    const a = new Set([shared, { id: 2 }]);
    const b = new Set([shared, { id: 2 }]); // second object is a distinct reference
    expect(intersection(a, b)).toEqual(new Set([shared]));
  });

  it("does not mutate either input set", () => {
    const a = new Set([1, 2, 3]);
    const b = new Set([2, 3, 4]);
    intersection(a, b);
    expect(a).toEqual(new Set([1, 2, 3]));
    expect(b).toEqual(new Set([2, 3, 4]));
  });

  it("returns a brand-new set, not one of the inputs", () => {
    const a = new Set([1, 2]);
    const b = new Set([1, 2]);
    const result = intersection(a, b);
    expect(result).not.toBe(a);
    expect(result).not.toBe(b);
    expect(result).toEqual(new Set([1, 2]));
  });

  it("is symmetric in the elements it selects", () => {
    const a = new Set([1, 2, 3]);
    const b = new Set([2, 3, 4]);
    expect(intersection(a, b)).toEqual(intersection(b, a));
  });
});
