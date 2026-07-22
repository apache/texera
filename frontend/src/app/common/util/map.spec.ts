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

import { mapToRecord, recordToMap } from "./map";

describe("mapToRecord", () => {
  it("converts a populated Map into an equivalent plain record", () => {
    const map = new Map<string, any>([
      ["a", 1],
      ["b", "two"],
      ["c", { nested: true }],
    ]);
    expect(mapToRecord(map)).toEqual({ a: 1, b: "two", c: { nested: true } });
  });

  it("returns an empty object for an empty Map", () => {
    expect(mapToRecord(new Map())).toEqual({});
  });

  it("overwrites an existing key with the latest value", () => {
    const map = new Map<string, any>();
    map.set("x", 1);
    map.set("y", 2);
    map.set("x", 99); // overwrite
    expect(mapToRecord(map)).toEqual({ x: 99, y: 2 });
  });

  it("survives a JSON.stringify round-trip (its stated purpose)", () => {
    const map = new Map<string, any>([
      ["k", [1, 2, 3]],
      ["flag", false],
    ]);
    const json = JSON.stringify(mapToRecord(map));
    expect(JSON.parse(json)).toEqual({ k: [1, 2, 3], flag: false });
  });
});

describe("recordToMap", () => {
  it("converts a plain record into an equivalent Map", () => {
    const map = recordToMap({ a: 1, b: "two" });
    expect(map).toBeInstanceOf(Map);
    expect(map.get("a")).toBe(1);
    expect(map.get("b")).toBe("two");
    expect(map.size).toBe(2);
  });

  it("returns an empty Map for an empty record", () => {
    expect(recordToMap({}).size).toBe(0);
  });

  it("only copies own enumerable keys, not inherited ones", () => {
    const proto = { inherited: "nope" };
    const record = Object.create(proto);
    record.own = "yes";
    const map = recordToMap(record);
    expect(map.has("own")).toBe(true);
    expect(map.has("inherited")).toBe(false);
  });
});

describe("mapToRecord / recordToMap round-trip", () => {
  it("reconstructs the original Map contents", () => {
    const original = new Map<string, any>([
      ["one", 1],
      ["two", { deep: [true, null] }],
    ]);
    const rebuilt = recordToMap(mapToRecord(original));
    expect(rebuilt).toEqual(original);
  });
});
