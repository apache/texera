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

import { describe, it, expect } from "vitest";
import {
  FILTER_CONDITIONS,
  parseFilterPredicatesResponse,
  parseWorkerCountResponse,
} from "./agent-proposal-requests";

describe("parseFilterPredicatesResponse", () => {
  it("accepts a single well-formed predicate", () => {
    const out = parseFilterPredicatesResponse({
      predicates: [{ attribute: "country", condition: "=", value: "US" }],
      reasoning: "downstream groups by country",
    });
    expect(out).toEqual({
      predicates: [{ attribute: "country", condition: "=", value: "US" }],
      reasoning: "downstream groups by country",
    });
  });

  it("accepts up to 5 predicates and preserves order", () => {
    const predicates = [
      { attribute: "a", condition: "is not null", value: "" },
      { attribute: "b", condition: ">", value: "0" },
      { attribute: "c", condition: "=", value: "x" },
      { attribute: "d", condition: "contains", value: "foo" },
      { attribute: "e", condition: "regex", value: "^x" },
    ];
    const out = parseFilterPredicatesResponse({ predicates, reasoning: "r" });
    expect(out?.predicates.map(p => p.attribute)).toEqual(["a", "b", "c", "d", "e"]);
  });

  it("accepts every condition in the FILTER_CONDITIONS enum", () => {
    for (const c of FILTER_CONDITIONS) {
      const out = parseFilterPredicatesResponse({
        predicates: [{ attribute: "x", condition: c, value: c === "is null" || c === "is not null" ? "" : "v" }],
        reasoning: "r",
      });
      expect(out, `condition ${c} should parse`).toBeDefined();
    }
  });

  it.each([
    ["null body", null],
    ["string body", "oops"],
    ["empty object", {}],
    ["missing predicates", { reasoning: "r" }],
    ["missing reasoning", { predicates: [{ attribute: "a", condition: "=", value: "v" }] }],
    ["empty predicates array", { predicates: [], reasoning: "r" }],
    [
      "more than 5 predicates",
      {
        predicates: Array.from({ length: 6 }, () => ({ attribute: "a", condition: "=", value: "v" })),
        reasoning: "r",
      },
    ],
    ["empty reasoning", { predicates: [{ attribute: "a", condition: "=", value: "v" }], reasoning: "" }],
    [
      "unknown condition",
      { predicates: [{ attribute: "a", condition: "starts with", value: "v" }], reasoning: "r" },
    ],
    [
      "missing attribute on a row",
      { predicates: [{ condition: "=", value: "v" }], reasoning: "r" },
    ],
    [
      "non-string value on a row",
      { predicates: [{ attribute: "a", condition: "=", value: 42 }], reasoning: "r" },
    ],
  ])("returns undefined for malformed response (%s)", (_label, raw) => {
    expect(parseFilterPredicatesResponse(raw)).toBeUndefined();
  });

  it("rejects the whole proposal if any row is malformed (all-or-nothing)", () => {
    const out = parseFilterPredicatesResponse({
      predicates: [
        { attribute: "ok", condition: "=", value: "v" },
        { attribute: "", condition: "=", value: "v" }, // bad
      ],
      reasoning: "r",
    });
    expect(out).toBeUndefined();
  });
});

describe("parseWorkerCountResponse", () => {
  it("accepts a well-formed integer in [1, 64]", () => {
    expect(parseWorkerCountResponse({ workers: 4, reasoning: "r" })).toEqual({ workers: 4, reasoning: "r" });
    expect(parseWorkerCountResponse({ workers: 1, reasoning: "r" })?.workers).toBe(1);
    expect(parseWorkerCountResponse({ workers: 64, reasoning: "r" })?.workers).toBe(64);
  });

  it.each([
    ["null body", null],
    ["empty object", {}],
    ["zero workers", { workers: 0, reasoning: "r" }],
    ["negative workers", { workers: -1, reasoning: "r" }],
    ["fractional workers", { workers: 4.5, reasoning: "r" }],
    ["above max", { workers: 65, reasoning: "r" }],
    ["string workers", { workers: "4", reasoning: "r" }],
    ["missing reasoning", { workers: 4 }],
    ["empty reasoning", { workers: 4, reasoning: "" }],
    ["non-string reasoning", { workers: 4, reasoning: 1 }],
  ])("returns undefined for malformed response (%s)", (_label, raw) => {
    expect(parseWorkerCountResponse(raw)).toBeUndefined();
  });
});
