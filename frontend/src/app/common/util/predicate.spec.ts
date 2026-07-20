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

import { isDefined } from "./predicate";

describe("isDefined", () => {
  it("returns false for undefined", () => {
    expect(isDefined(undefined)).toBe(false);
  });

  it("returns false for null", () => {
    expect(isDefined(null)).toBe(false);
  });

  it("returns true for defined primitive values", () => {
    expect(isDefined(0)).toBe(true);
    expect(isDefined("")).toBe(true);
    expect(isDefined(false)).toBe(true);
    expect(isDefined(NaN)).toBe(true);
  });

  it("returns true for defined object and array values", () => {
    expect(isDefined({})).toBe(true);
    expect(isDefined([])).toBe(true);
    expect(isDefined(() => undefined)).toBe(true);
  });

  it("narrows the type so the value is usable without a nullable guard", () => {
    const maybe: string | undefined = "hello";
    if (isDefined(maybe)) {
      // If narrowing failed this would not compile; assert runtime behavior too.
      expect(maybe.toUpperCase()).toBe("HELLO");
    } else {
      throw new Error("expected value to be defined");
    }
  });

  it("filters nullish entries out of a collection", () => {
    const values: (number | null | undefined)[] = [1, null, 2, undefined, 3];
    const defined: number[] = values.filter(isDefined);
    expect(defined).toEqual([1, 2, 3]);
  });
});
