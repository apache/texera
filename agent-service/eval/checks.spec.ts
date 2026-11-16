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

import { describe, expect, test } from "bun:test";
import { allOf, fail, mentionsAll, mentionsInOrder, mentionsNumber, pass } from "./checks";

describe("mentionsInOrder", () => {
  test("passes when every name appears in the given order, whatever the case", () => {
    expect(
      mentionsInOrder("1. technology, 2. Furniture, 3. Office Supplies", ["Technology", "Furniture", "Office Supplies"])
        .pass
    ).toBe(true);
  });

  test("fails when the names appear in another order", () => {
    const v = mentionsInOrder("Furniture first, then Technology", ["Technology", "Furniture"]);
    expect(v).toEqual({ pass: false, reason: "names Technology, Furniture out of order" });
  });

  test("names the ones that are missing", () => {
    expect(mentionsInOrder("Technology only", ["Technology", "Furniture", "Toys"])).toEqual({
      pass: false,
      reason: "does not mention Furniture, Toys",
    });
  });

  test("fails on an empty answer", () => {
    expect(mentionsInOrder("", ["Technology"]).pass).toBe(false);
  });
});

describe("mentionsNumber", () => {
  test.each([
    ["It keeps 120 orders.", 120],
    ["It keeps **120** orders.", 120],
    ["120", 120],
    ["kept 1,234 rows", 1234],
    ["kept 1234 rows", 1234],
  ])("finds the number in %p", (text, value) => {
    expect(mentionsNumber(text, value).pass).toBe(true);
  });

  test.each([
    ["It keeps 1120 orders.", 120],
    ["It keeps 120.5 orders on average.", 120],
    ["between 12,0 and 13", 120],
    ["a total of 1,120", 120],
    ["no number here", 120],
    ["", 0],
  ])("does not take part of another number in %p", (text, value) => {
    expect(mentionsNumber(text, value).pass).toBe(false);
  });
});

describe("mentionsAll", () => {
  test("passes when every phrase appears, ignoring case", () => {
    expect(
      mentionsAll("Set Snapshot, delta updates and SINGLE SNAPSHOT", ["set snapshot", "delta", "single snapshot"]).pass
    ).toBe(true);
  });

  test("lists the missing phrases", () => {
    expect(mentionsAll("Set Snapshot only", ["set snapshot", "delta"])).toEqual({
      pass: false,
      reason: "does not mention delta",
    });
  });
});

describe("allOf", () => {
  test("passes with every reason when all pass", () => {
    expect(allOf(pass("a"), pass("b"))).toEqual({ pass: true, reason: "a; b" });
  });

  test("gives the first failure", () => {
    expect(allOf(pass("a"), fail("b"), fail("c"))).toEqual({ pass: false, reason: "b" });
  });
});
