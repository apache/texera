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

import { searchTestEntries, SearchFilterParameters, toQueryStrings } from "./search-filter-parameters";
import { SortMethod } from "./sort-method";
import { DashboardEntry } from "./dashboard-entry";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { ExecutionMode } from "../../common/type/workflow";
import { OperatorPredicate } from "../../workspace/types/workflow-common.interface";

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

// Fixtures for searchTestEntries. Dates are built with local-time constructors so
// endOfDay() (which uses local getHours/setHours) is deterministic in every timezone.
interface WorkflowEntryOverrides {
  name?: string;
  wid?: number;
  ownerName?: string;
  creationTime?: number;
  lastModifiedTime?: number;
  operatorTypes?: string[];
  projectIDs?: number[];
}

function makeWorkflowEntry(overrides: WorkflowEntryOverrides = {}): DashboardEntry {
  const operators = (overrides.operatorTypes ?? []).map(
    (operatorType, i) => ({ operatorID: `op${i}`, operatorType }) as unknown as OperatorPredicate
  );
  const workflow: DashboardWorkflow = {
    isOwner: true,
    ownerName: overrides.ownerName ?? "alice",
    workflow: {
      content: {
        operators,
        operatorPositions: {},
        links: [],
        commentBoxes: [],
        settings: {
          dataTransferBatchSize: 400,
          executionMode: ExecutionMode.PIPELINED,
        },
      },
      name: overrides.name ?? "workflow",
      description: "",
      wid: overrides.wid ?? 1,
      creationTime: overrides.creationTime,
      lastModifiedTime: overrides.lastModifiedTime,
      isPublished: 0,
      readonly: false,
    },
    projectIDs: overrides.projectIDs ?? [],
    accessLevel: "WRITE",
    ownerId: 10,
    coverImage: null,
  };
  return new DashboardEntry(workflow);
}

function makeDatasetEntry(name = "dataset"): DashboardEntry {
  const dataset: DashboardDataset = {
    isOwner: false,
    ownerEmail: "dataset-owner@example.com",
    dataset: {
      did: 404,
      ownerUid: 40,
      name,
      isPublic: true,
      isDownloadable: true,
      storagePath: "/datasets/404",
      description: "",
      creationTime: new Date(2024, 0, 15, 12).getTime(),
      coverImage: "",
    },
    accessPrivilege: "READ",
    size: 100,
  };
  return new DashboardEntry(dataset);
}

describe("searchTestEntries", () => {
  it("returns all entries untouched when nothing is constraining", () => {
    const entries = [makeWorkflowEntry({ name: "alpha" }), makeWorkflowEntry({ name: "beta" })];
    expect(searchTestEntries([], makeEmptyFilter(), entries, null)).toEqual(entries);
  });

  it("keeps only entries whose name contains a keyword substring", () => {
    const alpha = makeWorkflowEntry({ name: "alpha-report" });
    const beta = makeWorkflowEntry({ name: "beta-report" });
    const result = searchTestEntries(["alph"], makeEmptyFilter(), [alpha, beta], null);
    expect(result).toEqual([alpha]);
  });

  it("matches when any of several keywords is a substring", () => {
    const alpha = makeWorkflowEntry({ name: "alpha" });
    const beta = makeWorkflowEntry({ name: "beta" });
    const gamma = makeWorkflowEntry({ name: "gamma" });
    const result = searchTestEntries(["alp", "bet"], makeEmptyFilter(), [alpha, beta, gamma], null);
    expect(result).toEqual([alpha, beta]);
  });

  it("filters by createDateStart, keeping entries created on or after the start", () => {
    const before = makeWorkflowEntry({ name: "before", creationTime: new Date(2024, 0, 9, 12).getTime() });
    const onOrAfter = makeWorkflowEntry({ name: "after", creationTime: new Date(2024, 0, 11, 12).getTime() });
    const filter = makeEmptyFilter();
    filter.createDateStart = new Date(2024, 0, 10);
    const result = searchTestEntries([], filter, [before, onOrAfter], null);
    expect(result).toEqual([onOrAfter]);
  });

  it("filters by createDateEnd inclusively through the end of that day", () => {
    // Late on the end day must still be included (endOfDay sets 23:59:59.999).
    const lateSameDay = makeWorkflowEntry({ name: "same", creationTime: new Date(2024, 0, 10, 22, 0).getTime() });
    const nextDay = makeWorkflowEntry({ name: "next", creationTime: new Date(2024, 0, 11, 1, 0).getTime() });
    const filter = makeEmptyFilter();
    filter.createDateEnd = new Date(2024, 0, 10);
    const result = searchTestEntries([], filter, [lateSameDay, nextDay], null);
    expect(result).toEqual([lateSameDay]);
  });

  it("filters by modifiedDateStart and modifiedDateEnd against lastModifiedTime", () => {
    const stale = makeWorkflowEntry({ name: "stale", lastModifiedTime: new Date(2024, 2, 1, 12).getTime() });
    const inRange = makeWorkflowEntry({ name: "inRange", lastModifiedTime: new Date(2024, 2, 15, 12).getTime() });
    const tooNew = makeWorkflowEntry({ name: "tooNew", lastModifiedTime: new Date(2024, 2, 20, 12).getTime() });
    const filter = makeEmptyFilter();
    filter.modifiedDateStart = new Date(2024, 2, 10);
    filter.modifiedDateEnd = new Date(2024, 2, 16);
    const result = searchTestEntries([], filter, [stale, inRange, tooNew], null);
    expect(result).toEqual([inRange]);
  });

  it("filters by owner name, matching any listed owner", () => {
    const alice = makeWorkflowEntry({ name: "a", ownerName: "alice" });
    const bob = makeWorkflowEntry({ name: "b", ownerName: "bob" });
    const carol = makeWorkflowEntry({ name: "c", ownerName: "carol" });
    const filter = makeEmptyFilter();
    filter.owners = ["alice", "carol"];
    const result = searchTestEntries([], filter, [alice, bob, carol], null);
    expect(result).toEqual([alice, carol]);
  });

  it("filters by workflow id, comparing the wid as a string", () => {
    const w7 = makeWorkflowEntry({ name: "w7", wid: 7 });
    const w8 = makeWorkflowEntry({ name: "w8", wid: 8 });
    const filter = makeEmptyFilter();
    filter.ids = ["7"];
    const result = searchTestEntries([], filter, [w7, w8], null);
    expect(result).toEqual([w7]);
  });

  it("filters by operator type present in the workflow content", () => {
    const hasCsv = makeWorkflowEntry({ name: "csv", operatorTypes: ["CSVFileScan", "Filter"] });
    const noCsv = makeWorkflowEntry({ name: "noCsv", operatorTypes: ["PythonUDFV2"] });
    const filter = makeEmptyFilter();
    filter.operators = ["CSVFileScan"];
    const result = searchTestEntries([], filter, [hasCsv, noCsv], null);
    expect(result).toEqual([hasCsv]);
  });

  it("filters by projectId membership", () => {
    const inProject = makeWorkflowEntry({ name: "in", projectIDs: [1, 2] });
    const notInProject = makeWorkflowEntry({ name: "out", projectIDs: [3] });
    const filter = makeEmptyFilter();
    filter.projectIds = [2];
    const result = searchTestEntries([], filter, [inProject, notInProject], null);
    expect(result).toEqual([inProject]);
  });

  it("excludes non-workflow entries when a workflow-only filter is applied", () => {
    // owners/ids/operators/projectIds all gate on e.type === "workflow".
    const workflow = makeWorkflowEntry({ name: "wf", ownerName: "alice" });
    const dataset = makeDatasetEntry("ds");
    const filter = makeEmptyFilter();
    filter.owners = ["alice"];
    const result = searchTestEntries([], filter, [workflow, dataset], null);
    expect(result).toEqual([workflow]);
  });

  it("filters by resource type when a type is given", () => {
    const workflow = makeWorkflowEntry({ name: "wf" });
    const dataset = makeDatasetEntry("ds");
    const result = searchTestEntries([], makeEmptyFilter(), [workflow, dataset], "dataset");
    expect(result).toEqual([dataset]);
  });

  it("applies all filters sequentially, keeping only entries that satisfy every one", () => {
    const match = makeWorkflowEntry({
      name: "alpha-pipeline",
      wid: 7,
      ownerName: "alice",
      creationTime: new Date(2024, 0, 15, 12).getTime(),
      lastModifiedTime: new Date(2024, 0, 16, 12).getTime(),
      operatorTypes: ["CSVFileScan"],
      projectIDs: [42],
    });
    const wrongOwner = makeWorkflowEntry({
      name: "alpha-pipeline",
      wid: 7,
      ownerName: "bob",
      creationTime: new Date(2024, 0, 15, 12).getTime(),
      lastModifiedTime: new Date(2024, 0, 16, 12).getTime(),
      operatorTypes: ["CSVFileScan"],
      projectIDs: [42],
    });
    const wrongName = makeWorkflowEntry({
      name: "beta-pipeline",
      wid: 7,
      ownerName: "alice",
      creationTime: new Date(2024, 0, 15, 12).getTime(),
      lastModifiedTime: new Date(2024, 0, 16, 12).getTime(),
      operatorTypes: ["CSVFileScan"],
      projectIDs: [42],
    });
    const filter: SearchFilterParameters = {
      createDateStart: new Date(2024, 0, 10),
      createDateEnd: new Date(2024, 0, 20),
      modifiedDateStart: new Date(2024, 0, 10),
      modifiedDateEnd: new Date(2024, 0, 20),
      owners: ["alice"],
      ids: ["7"],
      operators: ["CSVFileScan"],
      projectIds: [42],
    };
    const result = searchTestEntries(["alpha"], filter, [match, wrongOwner, wrongName], "workflow");
    expect(result).toEqual([match]);
  });
});
