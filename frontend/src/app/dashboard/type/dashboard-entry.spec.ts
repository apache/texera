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

import { DashboardEntry } from "./dashboard-entry";
import { EntityType } from "../../hub/service/hub.service";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardFile } from "./dashboard-file.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { DashboardWorkflowComputingUnit } from "../../common/type/workflow-computing-unit";
import { ExecutionMode } from "../../common/type/workflow";

// Each factory returns a fresh fixture that satisfies exactly one type predicate,
// so DashboardEntry's ordered if/else dispatch routes it to the intended branch.
function makeWorkflow(coverImage: string | null = "http://example.com/cover.png"): DashboardWorkflow {
  return {
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
      wid: 101,
      creationTime: 1700000000000,
      lastModifiedTime: 1700000001000,
      isPublished: 0,
      readonly: false,
    },
    projectIDs: [1, 2],
    accessLevel: "WRITE",
    ownerId: 10,
    coverImage,
  };
}

function makeProject(): DashboardProject {
  return {
    pid: 202,
    name: "My Project",
    description: "A sample project",
    ownerId: 20,
    creationTime: 1700000002000,
    color: "#ff0000",
    accessLevel: "READ",
  };
}

function makeFile(): DashboardFile {
  return {
    ownerEmail: "file-owner@example.com",
    accessLevel: "WRITE",
    file: {
      ownerUid: 30,
      fid: 303,
      size: 1234,
      name: "data.csv",
      path: "/files/data.csv",
      description: "A sample file",
      uploadTime: 1700000003000,
    },
  };
}

function makeDataset(): DashboardDataset {
  return {
    isOwner: false,
    ownerEmail: "dataset-owner@example.com",
    dataset: {
      did: 404,
      ownerUid: 40,
      name: "My Dataset",
      isPublic: true,
      isDownloadable: true,
      storagePath: "/datasets/404",
      description: "A sample dataset",
      creationTime: 1700000004000,
      coverImage: "http://example.com/dataset-cover.png",
    },
    accessPrivilege: "READ",
    size: 5678,
  };
}

function makeComputingUnit(): DashboardWorkflowComputingUnit {
  return {
    computingUnit: {
      cuid: 505,
      uid: 50,
      name: "My Computing Unit",
      creationTime: 1700000005000,
      terminateTime: undefined,
      type: "kubernetes",
      uri: "urn:texera:cu:505",
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
    ownerGoogleAvatar: "avatar",
    ownerName: "Bob",
  };
}

describe("DashboardEntry", () => {
  describe("constructor type mapping", () => {
    it("maps a DashboardWorkflow to the Workflow entity and copies workflow fields", () => {
      const value = makeWorkflow();
      const entry = new DashboardEntry(value);

      expect(entry.type).toBe(EntityType.Workflow);
      expect(entry.id).toBe(101);
      expect(entry.name).toBe("My Workflow");
      expect(entry.description).toBe("A sample workflow");
      expect(entry.creationTime).toBe(1700000000000);
      expect(entry.lastModifiedTime).toBe(1700000001000);
      expect(entry.accessLevel).toBe("WRITE");
      expect(entry.ownerName).toBe("Alice");
      expect(entry.ownerId).toBe(10);
      expect(entry.ownerEmail).toBe("");
      expect(entry.ownerGoogleAvatar).toBe("");
      expect(entry.size).toBe(0);
      expect(entry.coverImageUrl).toBe("http://example.com/cover.png");
      expect(entry.value).toBe(value);
    });

    it("leaves coverImageUrl undefined when a workflow has no cover image", () => {
      const entry = new DashboardEntry(makeWorkflow(null));
      expect(entry.coverImageUrl).toBeUndefined();
    });

    it("maps a DashboardProject to the Project entity with empty description and creationTime as both timestamps", () => {
      const entry = new DashboardEntry(makeProject());

      expect(entry.type).toBe(EntityType.Project);
      expect(entry.id).toBe(202);
      expect(entry.name).toBe("My Project");
      expect(entry.description).toBe("");
      expect(entry.creationTime).toBe(1700000002000);
      expect(entry.lastModifiedTime).toBe(1700000002000);
      expect(entry.accessLevel).toBe("READ");
      expect(entry.ownerId).toBe(20);
      expect(entry.coverImageUrl).toBeUndefined();
    });

    it("maps a DashboardFile to the File entity and copies file fields", () => {
      const entry = new DashboardEntry(makeFile());

      expect(entry.type).toBe(EntityType.File);
      expect(entry.id).toBe(303);
      expect(entry.name).toBe("data.csv");
      expect(entry.description).toBe("A sample file");
      expect(entry.creationTime).toBe(1700000003000);
      expect(entry.lastModifiedTime).toBe(1700000003000);
      expect(entry.accessLevel).toBe("WRITE");
      expect(entry.ownerEmail).toBe("file-owner@example.com");
      expect(entry.ownerId).toBe(30);
      expect(entry.size).toBe(1234);
    });

    it("maps a DashboardDataset to the Dataset entity and copies dataset fields", () => {
      const entry = new DashboardEntry(makeDataset());

      expect(entry.type).toBe(EntityType.Dataset);
      expect(entry.id).toBe(404);
      expect(entry.name).toBe("My Dataset");
      expect(entry.description).toBe("A sample dataset");
      expect(entry.creationTime).toBe(1700000004000);
      expect(entry.lastModifiedTime).toBe(1700000004000);
      expect(entry.accessLevel).toBe("READ");
      expect(entry.ownerEmail).toBe("dataset-owner@example.com");
      expect(entry.ownerId).toBe(40);
      expect(entry.size).toBe(5678);
      expect(entry.coverImageUrl).toBe("http://example.com/dataset-cover.png");
    });

    it("maps a DashboardWorkflowComputingUnit to the ComputingUnit entity and copies computing-unit fields", () => {
      const entry = new DashboardEntry(makeComputingUnit());

      expect(entry.type).toBe(EntityType.ComputingUnit);
      expect(entry.id).toBe(505);
      expect(entry.name).toBe("My Computing Unit");
      expect(entry.creationTime).toBe(1700000005000);
      expect(entry.accessLevel).toBe("WRITE");
      expect(entry.ownerId).toBe(50);
      expect(entry.ownerGoogleAvatar).toBe("");
      // The computing-unit branch does not populate these fields.
      expect(entry.description).toBeUndefined();
      expect(entry.lastModifiedTime).toBeUndefined();
      expect(entry.ownerEmail).toBeUndefined();
      expect(entry.size).toBeUndefined();
    });

    it("initializes the shared counters and flags to their defaults", () => {
      const entry = new DashboardEntry(makeWorkflow());

      expect(entry.viewCount).toBe(0);
      expect(entry.cloneCount).toBe(0);
      expect(entry.likeCount).toBe(0);
      expect(entry.isLiked).toBe(false);
      expect(entry.accessibleUserIds).toEqual([]);
      expect(entry.checked).toBe(false);
    });

    it("throws for a value that matches no type predicate", () => {
      expect(() => new DashboardEntry({} as any)).toThrowError("Unexpected type in DashboardEntry.");
    });
  });

  describe("setters", () => {
    it("setOwnerName updates ownerName", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setOwnerName("Carol");
      expect(entry.ownerName).toBe("Carol");
    });

    it("setOwnerGoogleAvatar updates ownerGoogleAvatar", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setOwnerGoogleAvatar("https://example.com/avatar.png");
      expect(entry.ownerGoogleAvatar).toBe("https://example.com/avatar.png");
    });

    it("setCount updates viewCount, cloneCount and likeCount together", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setCount(11, 22, 33);
      expect(entry.viewCount).toBe(11);
      expect(entry.cloneCount).toBe(22);
      expect(entry.likeCount).toBe(33);
    });

    it("setIsLiked updates isLiked", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setIsLiked(true);
      expect(entry.isLiked).toBe(true);
    });

    it("setAccessUsers replaces accessibleUserIds", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setAccessUsers([1, 2, 3]);
      expect(entry.accessibleUserIds).toEqual([1, 2, 3]);
    });

    it("setSize updates size", () => {
      const entry = new DashboardEntry(makeWorkflow());
      entry.setSize(9999);
      expect(entry.size).toBe(9999);
    });
  });

  describe("getters", () => {
    it("workflow getter returns the value for a workflow entry and throws for others", () => {
      const workflowValue = makeWorkflow();
      expect(new DashboardEntry(workflowValue).workflow).toBe(workflowValue);
      expect(() => new DashboardEntry(makeProject()).workflow).toThrowError("Value is not of type DashboardWorkflow.");
    });

    it("project getter returns the value for a project entry and throws for others", () => {
      const projectValue = makeProject();
      expect(new DashboardEntry(projectValue).project).toBe(projectValue);
      expect(() => new DashboardEntry(makeWorkflow()).project).toThrowError("Value is not of type DashboardProject.");
    });

    it("file getter returns the value for a file entry and throws for others", () => {
      const fileValue = makeFile();
      expect(new DashboardEntry(fileValue).file).toBe(fileValue);
      expect(() => new DashboardEntry(makeWorkflow()).file).toThrowError("Value is not of type DashboardFile.");
    });

    it("dataset getter returns the value for a dataset entry and throws for others", () => {
      const datasetValue = makeDataset();
      expect(new DashboardEntry(datasetValue).dataset).toBe(datasetValue);
      expect(() => new DashboardEntry(makeWorkflow()).dataset).toThrowError("Value is not of type DashboardDataset");
    });

    it("computingUnit getter returns the value for a computing-unit entry and throws for others", () => {
      const computingUnitValue = makeComputingUnit();
      expect(new DashboardEntry(computingUnitValue).computingUnit).toBe(computingUnitValue);
      expect(() => new DashboardEntry(makeWorkflow()).computingUnit).toThrowError(
        "Value is not of type DashboardWorkflowComputingUnit"
      );
    });
  });
});
