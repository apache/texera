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

import { replaceOneImmutable } from "./array-utils";

describe("replaceOneImmutable", () => {
  it("should return a new array reference with the matched element replaced", () => {
    const input = [1, 2, 3];
    const result = replaceOneImmutable(input, x => x === 2, 20);

    expect(result).not.toBe(input);
    expect(result).toEqual([1, 20, 3]);
  });

  it("should not mutate the original input array", () => {
    const input = [1, 2, 3];
    replaceOneImmutable(input, x => x === 2, 20);

    expect(input).toEqual([1, 2, 3]);
  });

  it("should return the same array reference when the predicate matches nothing", () => {
    const input = [1, 2, 3];
    const result = replaceOneImmutable(input, x => x === 999, 20);

    expect(result).toBe(input);
  });

  it("should pass the element index as the second argument to the predicate", () => {
    const input = ["a", "b", "c"];
    const seenIndices: number[] = [];

    replaceOneImmutable(
      input,
      (t, idx) => {
        seenIndices.push(idx);
        return t === "b";
      },
      "z"
    );

    expect(seenIndices).toEqual([0, 1]);
  });
});
