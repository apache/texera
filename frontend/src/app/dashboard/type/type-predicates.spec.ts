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

import {
  isDashboardDataset,
  isDashboardFile,
  isDashboardProject,
  isDashboardWorkflow,
  isDashboardWorkflowComputingUnit,
} from "./type-predicates";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardFile } from "./dashboard-file.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { DashboardWorkflowComputingUnit } from "../../common/type/workflow-computing-unit";
import { ExecutionMode } from "../../common/type/workflow";

const workflowFixture: DashboardWorkflow = {
  isOwner: true,
  ownerName: "Alice",
  workflow: {
    content: {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: {
        dataTransferBatchSize: 400,
        executionMode: ExecutionMode.PIPELINED,
      },
    },
    name: "My Workflow",
    description: "A sample workflow",
    wid: 1,
    creationTime: 1700000000000,
    lastModifiedTime: 1700000001000,
    isPublished: 0,
    readonly: false,
  },
  projectIDs: [1, 2],
  accessLevel: "WRITE",
  ownerId: 10,
  coverImage: null,
};

const projectFixture: DashboardProject = {
  pid: 5,
  name: "My Project",
  description: "A sample project",
  ownerId: 10,
  creationTime: 1700000000000,
  color: "#ff0000",
  accessLevel: "WRITE",
};

const fileFixture: DashboardFile = {
  ownerEmail: "alice@example.com",
  accessLevel: "READ",
  file: {
    ownerUid: 10,
    fid: 7,
    size: 1024,
    name: "data.csv",
    path: "/files/data.csv",
    description: "A sample file",
    uploadTime: 1700000000000,
  },
};

const datasetFixture: DashboardDataset = {
  isOwner: false,
  ownerEmail: "bob@example.com",
  dataset: {
    did: 3,
    ownerUid: 11,
    name: "My Dataset",
    isPublic: true,
    isDownloadable: true,
    storagePath: "/datasets/3",
    description: "A sample dataset",
    creationTime: 1700000000000,
    coverImage: undefined,
  },
  accessPrivilege: "READ",
  size: 2048,
};

const computingUnitFixture: DashboardWorkflowComputingUnit = {
  computingUnit: {
    cuid: 9,
    uid: 10,
    name: "My Computing Unit",
    creationTime: 1700000000000,
    terminateTime: undefined,
    type: "kubernetes",
    uri: "urn:texera:cu:9",
    resource: {
      cpuLimit: "2",
      memoryLimit: "4Gi",
      gpuLimit: "0",
      jvmMemorySize: "2G",
      shmSize: "64Mi",
      nodeAddresses: ["10.0.0.1"],
    },
  },
  status: "Running",
  metrics: {
    cpuUsage: "0.5",
    memoryUsage: "1Gi",
  },
  isOwner: true,
  accessPrivilege: "WRITE",
  ownerGoogleAvatar: "",
  ownerName: "Alice",
};

describe("isDashboardWorkflow", () => {
  it("should return true for a realistic DashboardWorkflow", () => {
    expect(isDashboardWorkflow(workflowFixture)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDashboardWorkflow(null)).toBe(false);
    expect(isDashboardWorkflow(undefined)).toBe(false);
  });

  it("should return false for an object without a workflow field", () => {
    expect(isDashboardWorkflow({})).toBe(false);
  });

  it("should return false when workflow is not an object", () => {
    expect(isDashboardWorkflow({ workflow: "not an object" })).toBe(false);
  });

  it("should return false when workflow is null", () => {
    // A null payload must be rejected even though typeof null === "object".
    expect(isDashboardWorkflow({ workflow: null })).toBe(false);
  });
});

describe("isDashboardProject", () => {
  it("should return true for a realistic DashboardProject", () => {
    expect(isDashboardProject(projectFixture)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDashboardProject(null)).toBe(false);
    expect(isDashboardProject(undefined)).toBe(false);
  });

  it("should return false for an object without a name field", () => {
    expect(isDashboardProject({})).toBe(false);
  });

  it("should return false when name is not a string", () => {
    expect(isDashboardProject({ name: 42 })).toBe(false);
  });

  it("should return false when a workflow field is also present", () => {
    expect(isDashboardProject({ name: "x", workflow: workflowFixture.workflow })).toBe(false);
  });

  it("should return true when name is a string and workflow is null", () => {
    // Intentional: a null workflow field is treated as "no workflow", so the
    // exclusion branch `!value.workflow` still classifies the object as a project.
    expect(isDashboardProject({ name: "x", workflow: null })).toBe(true);
  });
});

describe("isDashboardFile", () => {
  it("should return true for a realistic DashboardFile", () => {
    expect(isDashboardFile(fileFixture)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDashboardFile(null)).toBe(false);
    expect(isDashboardFile(undefined)).toBe(false);
  });

  it("should return false for an empty object", () => {
    expect(isDashboardFile({})).toBe(false);
  });

  it("should return false when ownerEmail is missing", () => {
    expect(isDashboardFile({ file: fileFixture.file })).toBe(false);
  });

  it("should return false when file is missing", () => {
    expect(isDashboardFile({ ownerEmail: "a@b.com" })).toBe(false);
  });

  it("should return false when ownerEmail is not a string", () => {
    expect(isDashboardFile({ ownerEmail: 42, file: fileFixture.file })).toBe(false);
  });

  it("should return false when file is null", () => {
    // A null payload must be rejected even though typeof null === "object".
    expect(isDashboardFile({ ownerEmail: "a@b.com", file: null })).toBe(false);
  });
});

describe("isDashboardDataset", () => {
  it("should return true for a realistic DashboardDataset", () => {
    expect(isDashboardDataset(datasetFixture)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDashboardDataset(null)).toBe(false);
    expect(isDashboardDataset(undefined)).toBe(false);
  });

  it("should return false for an object without a dataset field", () => {
    expect(isDashboardDataset({})).toBe(false);
  });

  it("should return false when dataset is not an object", () => {
    expect(isDashboardDataset({ dataset: "not an object" })).toBe(false);
  });

  it("should return false when dataset is null", () => {
    // A null payload must be rejected even though typeof null === "object".
    expect(isDashboardDataset({ dataset: null })).toBe(false);
  });
});

describe("isDashboardWorkflowComputingUnit", () => {
  it("should return true for a realistic DashboardWorkflowComputingUnit", () => {
    expect(isDashboardWorkflowComputingUnit(computingUnitFixture)).toBe(true);
  });

  it("should return false for null and undefined", () => {
    expect(isDashboardWorkflowComputingUnit(null)).toBe(false);
    expect(isDashboardWorkflowComputingUnit(undefined)).toBe(false);
  });

  it("should return false for an object without a computingUnit field", () => {
    expect(isDashboardWorkflowComputingUnit({})).toBe(false);
  });

  it("should return false when computingUnit is not an object", () => {
    expect(isDashboardWorkflowComputingUnit({ computingUnit: "not an object" })).toBe(false);
  });

  it("should return false when computingUnit is null", () => {
    // A null payload must be rejected even though typeof null === "object".
    expect(isDashboardWorkflowComputingUnit({ computingUnit: null })).toBe(false);
  });
});

// Every realistic fixture must satisfy exactly one predicate. DashboardEntry relies on
// this mutual exclusivity: its constructor dispatches through an ordered if/else chain,
// so a fixture matching a predicate other than its own would be classified incorrectly.
describe("type predicate cross-classification", () => {
  const fixtures: ReadonlyArray<[string, unknown, string]> = [
    ["DashboardWorkflow fixture", workflowFixture, "isDashboardWorkflow"],
    ["DashboardProject fixture", projectFixture, "isDashboardProject"],
    ["DashboardFile fixture", fileFixture, "isDashboardFile"],
    ["DashboardDataset fixture", datasetFixture, "isDashboardDataset"],
    ["DashboardWorkflowComputingUnit fixture", computingUnitFixture, "isDashboardWorkflowComputingUnit"],
  ];

  const predicates: ReadonlyArray<[string, (value: unknown) => boolean]> = [
    ["isDashboardWorkflow", isDashboardWorkflow],
    ["isDashboardProject", isDashboardProject],
    ["isDashboardFile", isDashboardFile],
    ["isDashboardDataset", isDashboardDataset],
    ["isDashboardWorkflowComputingUnit", isDashboardWorkflowComputingUnit],
  ];

  fixtures.forEach(([fixtureName, fixture, expectedPredicate]) => {
    predicates.forEach(([predicateName, predicate]) => {
      const expected = predicateName === expectedPredicate;
      it(`${predicateName} should return ${expected} for the ${fixtureName}`, () => {
        // The it() title identifies the failing fixture/predicate pair.
        expect(predicate(fixture)).toBe(expected);
      });
    });
  });
});
