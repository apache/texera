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

import { NzSelectItemInterface } from "ng-zorro-antd/select";
import { datasetMatchesQuery, filterDatasetOption } from "./dataset-search.util";

describe("datasetMatchesQuery", () => {
  it("matches by name, case-insensitively", () => {
    expect(datasetMatchesQuery("iris", 17, "iris")).toBe(true);
    expect(datasetMatchesQuery("iris", 17, "IR")).toBe(true);
    expect(datasetMatchesQuery("iris", 17, "ris")).toBe(true); // substring
  });

  it("matches by numeric id", () => {
    expect(datasetMatchesQuery("iris", 17, "17")).toBe(true);
    expect(datasetMatchesQuery("iris", 17, "1")).toBe(true); // substring of id
  });

  it("matches the id typed with the leading # shown in the dropdown label", () => {
    expect(datasetMatchesQuery("iris", 17, "#17")).toBe(true);
    expect(datasetMatchesQuery("iris", 17, "#1")).toBe(true); // prefix of the displayed #17
    expect(datasetMatchesQuery("iris", 17, "#99")).toBe(false);
    expect(datasetMatchesQuery(null, 17, "#17")).toBe(true); // id-only match still works
  });

  it("does not match when neither name nor id contains the query", () => {
    expect(datasetMatchesQuery("iris", 17, "test")).toBe(false);
    expect(datasetMatchesQuery("iris", 17, "99")).toBe(false);
  });

  it("treats an empty or whitespace query as matching everything", () => {
    expect(datasetMatchesQuery("iris", 17, "")).toBe(true);
    expect(datasetMatchesQuery("iris", 17, "   ")).toBe(true);
  });

  it("trims and lowercases the query", () => {
    expect(datasetMatchesQuery("Iris", 17, "  IRIS  ")).toBe(true);
  });

  it("handles missing name or id safely", () => {
    expect(datasetMatchesQuery(null, 17, "17")).toBe(true);
    expect(datasetMatchesQuery(undefined, null, "iris")).toBe(false);
    expect(datasetMatchesQuery("iris", null, "iris")).toBe(true);
    expect(datasetMatchesQuery("iris", undefined, "3")).toBe(false);
  });

  it("matches when the query appears in the name but not the id (and vice versa)", () => {
    expect(datasetMatchesQuery("customers", 42, "cust")).toBe(true); // name only
    expect(datasetMatchesQuery("customers", 42, "42")).toBe(true); // id only
  });

  it("does not partially match across the name/id boundary", () => {
    // "s4" is not a substring of the name "iris" nor of the id "3"
    expect(datasetMatchesQuery("iris", 3, "s4")).toBe(false);
  });
});

describe("filterDatasetOption", () => {
  // Build an nz-select option carrying a DashboardDataset-shaped value, like the real
  // dropdown does. Only the fields the filter reads (name, did) matter here.
  const optionFor = (name: string, did: number): NzSelectItemInterface =>
    ({ nzLabel: name, nzValue: { dataset: { name, did } } }) as unknown as NzSelectItemInterface;

  it("matches by dataset name pulled from the option value", () => {
    expect(filterDatasetOption("iris", optionFor("iris", 17))).toBe(true);
    expect(filterDatasetOption("IR", optionFor("iris", 17))).toBe(true);
    expect(filterDatasetOption("test", optionFor("iris", 17))).toBe(false);
  });

  it("matches by dataset #id pulled from the option value", () => {
    expect(filterDatasetOption("17", optionFor("iris", 17))).toBe(true);
    expect(filterDatasetOption("99", optionFor("iris", 17))).toBe(false);
  });

  it("matches the #<id> form typed as displayed in the UI label", () => {
    expect(filterDatasetOption("#17", optionFor("iris", 17))).toBe(true);
    expect(filterDatasetOption("#99", optionFor("iris", 17))).toBe(false);
  });

  it("is safe (and matches only on empty query) when the option has no value", () => {
    const emptyOption = { nzLabel: null, nzValue: null } as unknown as NzSelectItemInterface;
    expect(filterDatasetOption("", emptyOption)).toBe(true); // empty query → matches all
    expect(filterDatasetOption("iris", emptyOption)).toBe(false); // no data → no match, no throw
  });

  it("treats a null/undefined query as an empty query (matches everything)", () => {
    expect(filterDatasetOption(null as unknown as string, optionFor("iris", 17))).toBe(true);
    expect(filterDatasetOption(undefined as unknown as string, optionFor("iris", 17))).toBe(true);
  });
});
