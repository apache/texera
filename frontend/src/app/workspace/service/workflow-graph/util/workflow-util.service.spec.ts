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

import { StubOperatorMetadataService } from "./../../operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "./../../operator-metadata/operator-metadata.service";
import { inject, TestBed } from "@angular/core/testing";

import { WorkflowUtilService } from "./workflow-util.service";
import { mockMultiInputOutputSchema, mockScanSourceSchema } from "../../operator-metadata/mock-operator-metadata.data";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";

describe("WorkflowUtilService", () => {
  let workflowUtilService: WorkflowUtilService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowUtilService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    });
    workflowUtilService = TestBed.inject(WorkflowUtilService);
  });

  it("should be created", inject([WorkflowUtilService], (service: WorkflowUtilService) => {
    expect(service).toBeTruthy();
  }));

  it("should be able to generate an operator predicate properly given a valid operator type", () => {
    const operatorSchema = mockScanSourceSchema;
    const operatorPredicate = workflowUtilService.getNewOperatorPredicate(operatorSchema.operatorType);

    // assert predicate itself and operator type are correct
    expect(operatorPredicate).toBeTruthy();
    expect(operatorPredicate.operatorType).toEqual(operatorSchema.operatorType);
    // assert num of input ports and output ports are correct
    expect(operatorPredicate.inputPorts.length).toEqual(operatorSchema.additionalMetadata.inputPorts.length);
    expect(operatorPredicate.outputPorts.length).toEqual(operatorSchema.additionalMetadata.outputPorts.length);
    // asssert that the portID of input and output ports are all distinct
    expect(new Set(operatorPredicate.inputPorts).size).toEqual(operatorPredicate.inputPorts.length);
    expect(new Set(operatorPredicate.outputPorts).size).toEqual(operatorPredicate.outputPorts.length);

    // assert that it creates the operator property to be an empty object
    expect(operatorPredicate.operatorProperties).toEqual({});
  });

  it("should throw an error when trying to generate an operator predicate with non exist operator type", () => {
    expect(() => {
      workflowUtilService.getNewOperatorPredicate("non-exist-operator-type");
    }).toThrowError(new RegExp("doesn't exist"));
  });

  it("should be able to generate different operator IDs", () => {
    const idSet = new Set<string>();
    const repeat = 100;
    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getOperatorRandomUUID());
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });

  it("should be able to generate different link IDs", () => {
    const idSet = new Set<string>();
    const repeat = 100;
    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getLinkRandomUUID());
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });

  it("should be able to assign different operator IDs to newly generated operators", () => {
    const operatorSchema = mockScanSourceSchema;
    const idSet = new Set<string>();
    const repeat = 100;

    for (let i = 0; i < repeat; i++) {
      idSet.add(workflowUtilService.getNewOperatorPredicate(operatorSchema.operatorType).operatorID);
    }
    // assert all IDs are distinct
    expect(idSet.size).toEqual(repeat);
  });

  it("should rebuild ports and version from the schema when the operator has no ports", () => {
    const op: OperatorPredicate = {
      operatorID: "op-1",
      operatorType: mockMultiInputOutputSchema.operatorType,
      operatorVersion: "stale-version",
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: false,
      isDisabled: false,
    };

    const updated = workflowUtilService.updateOperatorVersion(op);

    // version is refreshed from the schema
    expect(updated.operatorVersion).toEqual(mockMultiInputOutputSchema.operatorVersion);
    // ports are regenerated from the schema's port metadata
    expect(updated.inputPorts.map(port => port.portID)).toEqual(["input-0", "input-1", "input-2"]);
    expect(updated.outputPorts.map(port => port.portID)).toEqual(["output-0", "output-1", "output-2"]);
  });

  it("should keep the operator's existing ports when they are already present", () => {
    const existingInputPorts = [{ portID: "input-existing", displayName: "keep-me" }];
    const existingOutputPorts = [{ portID: "output-existing", displayName: "keep-me-too" }];
    const op: OperatorPredicate = {
      operatorID: "op-2",
      operatorType: mockMultiInputOutputSchema.operatorType,
      operatorVersion: "stale-version",
      operatorProperties: {},
      inputPorts: existingInputPorts,
      outputPorts: existingOutputPorts,
      showAdvanced: false,
      isDisabled: false,
    };

    const updated = workflowUtilService.updateOperatorVersion(op);

    // existing ports are preserved untouched, only the version is refreshed
    expect(updated.inputPorts).toEqual(existingInputPorts);
    expect(updated.outputPorts).toEqual(existingOutputPorts);
    expect(updated.operatorVersion).toEqual(mockMultiInputOutputSchema.operatorVersion);
  });

  it("should throw an error when updating the version of an operator with an unknown type", () => {
    const op: OperatorPredicate = {
      operatorID: "op-3",
      operatorType: "non-exist-operator-type",
      operatorVersion: "v1",
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [],
      showAdvanced: false,
      isDisabled: false,
    };

    expect(() => workflowUtilService.updateOperatorVersion(op)).toThrowError(new RegExp("doesn't exist"));
  });

  it("should parse the workflow content string into an object", () => {
    const content: WorkflowContent = {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400, executionMode: "BATCH" as any },
    };
    const workflow = {
      name: "test-workflow",
      description: undefined,
      wid: 1,
      creationTime: undefined,
      lastModifiedTime: undefined,
      isPublished: 0,
      readonly: false,
      content: JSON.stringify(content) as any,
    } as unknown as Workflow;

    const parsed = WorkflowUtilService.parseWorkflowInfo(workflow);

    // the string content is replaced with the parsed object
    expect(typeof parsed.content).toEqual("object");
    expect(parsed.content).toEqual(content);
  });

  it("should leave workflow content untouched when it is already an object", () => {
    const content: WorkflowContent = {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400, executionMode: "BATCH" as any },
    };
    const workflow = {
      name: "test-workflow",
      description: undefined,
      wid: 1,
      creationTime: undefined,
      lastModifiedTime: undefined,
      isPublished: 0,
      readonly: false,
      content,
    } as unknown as Workflow;

    const parsed = WorkflowUtilService.parseWorkflowInfo(workflow);

    // an object content is returned as-is
    expect(parsed.content).toBe(content);
  });

  it("should create a fresh comment box at the default position", () => {
    const commentBox = workflowUtilService.getNewCommentBox();

    expect(commentBox.commentBoxID).toMatch(/^commentBox-/);
    expect(commentBox.comments).toEqual([]);
    expect(commentBox.commentBoxPosition).toEqual({ x: 500, y: 20 });
  });

  it("should generate a distinct comment box each time", () => {
    const first = workflowUtilService.getNewCommentBox();
    const second = workflowUtilService.getNewCommentBox();

    expect(first.commentBoxID).not.toEqual(second.commentBoxID);
  });
});
