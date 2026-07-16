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

import { SearchFilterParameters, toQueryStrings } from "./search-filter-parameters";
import { SortMethod } from "./sort-method";

function makeEmptyFilter(): SearchFilterParameters {
  return {
    createDateStart: null,
    createDateEnd: null,
    modifiedDateStart: null,
    modifiedDateEnd: null,
    owners: [],
    ids: [],
    operators: [],
    projectIds: [],
  };
}

describe("toQueryStrings", () => {
  it("should return an empty string for empty keywords, an empty filter, and no optional arguments", () => {
    expect(toQueryStrings([], makeEmptyFilter())).toBe("");
  });

  it("should emit one query parameter per keyword, preserving order", () => {
    expect(toQueryStrings(["alpha", "beta"], makeEmptyFilter())).toBe("query=alpha&query=beta");
  });

  it("should trim keywords and drop whitespace-only keywords", () => {
    expect(toQueryStrings(["  alpha  ", "   "], makeEmptyFilter())).toBe("query=alpha");
  });

  it("should URL-encode keyword values", () => {
    expect(toQueryStrings(["a b&c=d"], makeEmptyFilter())).toBe("query=a%20b%26c%3Dd");
  });

  // The date assertions below pin the CURRENT behavior: dates are serialized via
  // toISOString(), i.e. as the UTC calendar day. Callers pass local-midnight Dates,
  // so in UTC+ timezones the emitted day is one earlier than the day the user picked
  // (known off-by-one bug, tracked separately). The Date literals here are anchored
  // to 12:00 UTC so these tests are stable in every timezone.
  it("should serialize all four date filters as UTC YYYY-MM-DD in a fixed order", () => {
    const filter = makeEmptyFilter();
    filter.createDateStart = new Date("2024-01-15T12:00:00Z");
    filter.createDateEnd = new Date("2024-02-20T12:00:00Z");
    filter.modifiedDateStart = new Date("2024-03-05T12:00:00Z");
    filter.modifiedDateEnd = new Date("2024-04-10T12:00:00Z");

    expect(toQueryStrings([], filter)).toBe(
      "createDateStart=2024-01-15&createDateEnd=2024-02-20&modifiedDateStart=2024-03-05&modifiedDateEnd=2024-04-10"
    );
  });

  it("should omit date filters that are null", () => {
    const filter = makeEmptyFilter();
    filter.modifiedDateEnd = new Date("2024-04-10T12:00:00Z");

    expect(toQueryStrings([], filter)).toBe("modifiedDateEnd=2024-04-10");
  });

  it("should emit repeated owner, id, and operator parameters, preserving order", () => {
    const filter = makeEmptyFilter();
    filter.owners = ["alice", "bob"];
    filter.ids = ["7", "8"];
    filter.operators = ["CSVFileScan", "PythonUDFV2"];

    expect(toQueryStrings([], filter)).toBe(
      "owner=alice&owner=bob&id=7&id=8&operator=CSVFileScan&operator=PythonUDFV2"
    );
  });

  it("should stringify numeric projectIds and keep projectId 0", () => {
    const filter = makeEmptyFilter();
    filter.projectIds = [0, 42];

    expect(toQueryStrings([], filter)).toBe("projectId=0&projectId=42");
  });

  it("should URL-encode filter values", () => {
    const filter = makeEmptyFilter();
    filter.owners = ["a+b@x.com"];

    expect(toQueryStrings([], filter)).toBe("owner=a%2Bb%40x.com");
  });

  it("should silently drop empty-string filter values", () => {
    const filter = makeEmptyFilter();
    filter.owners = ["", "alice"];

    expect(toQueryStrings([], filter)).toBe("owner=alice");
  });

  it("should emit start and count when they are positive", () => {
    expect(toQueryStrings([], makeEmptyFilter(), 10, 20)).toBe("start=10&count=20");
  });

  it("should omit start and count when they are 0", () => {
    expect(toQueryStrings([], makeEmptyFilter(), 0, 0)).toBe("");
  });

  it("should emit resourceType for a non-null type and omit it for null or undefined", () => {
    expect(toQueryStrings([], makeEmptyFilter(), undefined, undefined, "workflow")).toBe("resourceType=workflow");
    expect(toQueryStrings([], makeEmptyFilter(), undefined, undefined, null)).toBe("");
    expect(toQueryStrings([], makeEmptyFilter())).toBe("");
  });

  it("should emit orderBy for SortMethod.NameAsc even though its enum value is 0", () => {
    expect(toQueryStrings([], makeEmptyFilter(), undefined, undefined, undefined, SortMethod.NameAsc)).toBe(
      "orderBy=NameAsc"
    );
  });

  it("should emit the enum member name for other sort methods and omit orderBy when undefined", () => {
    expect(toQueryStrings([], makeEmptyFilter(), undefined, undefined, undefined, SortMethod.EditTimeDesc)).toBe(
      "orderBy=EditTimeDesc"
    );
    expect(toQueryStrings([], makeEmptyFilter(), undefined, undefined, undefined, undefined)).toBe("");
  });

  it("should emit all parameter groups in the documented order when everything is populated", () => {
    const filter: SearchFilterParameters = {
      createDateStart: new Date("2024-01-15T12:00:00Z"),
      createDateEnd: new Date("2024-02-20T12:00:00Z"),
      modifiedDateStart: new Date("2024-03-05T12:00:00Z"),
      modifiedDateEnd: new Date("2024-04-10T12:00:00Z"),
      owners: ["alice"],
      ids: ["7"],
      operators: ["CSVFileScan"],
      projectIds: [42],
    };

    expect(toQueryStrings(["alpha"], filter, 10, 20, "workflow", SortMethod.CreateTimeDesc)).toBe(
      "query=alpha" +
        "&createDateStart=2024-01-15&createDateEnd=2024-02-20" +
        "&modifiedDateStart=2024-03-05&modifiedDateEnd=2024-04-10" +
        "&owner=alice&id=7&operator=CSVFileScan&projectId=42" +
        "&start=10&count=20&resourceType=workflow&orderBy=CreateTimeDesc"
    );
  });

  it("should omit start, count, resourceType, and orderBy when called with only keywords and filters", () => {
    const filter = makeEmptyFilter();
    filter.owners = ["alice"];

    expect(toQueryStrings(["alpha"], filter)).toBe("query=alpha&owner=alice");
  });
});
