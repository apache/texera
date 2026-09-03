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
import { JointUIService } from "./../../joint-ui/joint-ui.service";
import { WorkflowGraph } from "./workflow-graph";
import { UndoRedoService } from "./../../undo-redo/undo-redo.service";
import {
  mockCommentBox,
  mockFalseResultSentimentLink,
  mockFalseSentimentScanLink,
  mockMultiInputOutputPredicate,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "./mock-workflow-data";
import { inject, TestBed } from "@angular/core/testing";

import { DEFAULT_WORKFLOW, DEFAULT_WORKFLOW_NAME, WorkflowActionService } from "./workflow-action.service";
import { LogicalPort, OperatorPredicate } from "../../../types/workflow-common.interface";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ExecutionMode, Workflow, WorkflowSettings } from "../../../../common/type/workflow";

describe("WorkflowActionService", () => {
  let service: WorkflowActionService;
  let undoRedo: UndoRedoService;
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
      imports: [],
    });
    service = TestBed.inject(WorkflowActionService);
    undoRedo = TestBed.inject(UndoRedoService);
    texeraGraph = (service as any).texeraGraph;
    jointGraph = (service as any).jointGraph;
  });

  it("should be created", inject([WorkflowActionService], (injectedService: WorkflowActionService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should add an operator to both jointjs and texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
  });

  it("should add commentBox to both jointjs and texera graph correctly", () => {
    service.addCommentBox(mockCommentBox);
    expect(texeraGraph.hasCommentBox(mockCommentBox.commentBoxID)).toBeTruthy();
    expect(jointGraph.getCell(mockCommentBox.commentBoxID)).toBeTruthy();
  });

  it("should throw an error when adding an existed operator", () => {
    service.addOperator(mockScanPredicate, mockPoint);

    expect(() => {
      service.addOperator(mockScanPredicate, mockPoint);
    }).toThrowError(new RegExp("exists"));
  });

  it("should throw an error when adding an operator with invalid operator type", () => {
    const invalidOperator: OperatorPredicate = {
      ...mockScanPredicate,
      operatorType: "invalidOperatorTypeForTesting",
    };

    expect(() => {
      service.addOperator(invalidOperator, mockPoint);
    }).toThrowError(new RegExp("invalid"));
  });

  it("should delete an operator to both jointjs and texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);

    service.deleteOperator(mockScanPredicate.operatorID);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeFalsy();
  });

  it("should throw an error when trying to delete an non-existing operator", () => {
    expect(() => {
      service.deleteOperator(mockScanPredicate.operatorID);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));
  });

  it("should add a link to both jointjs and texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);

    service.addLink(mockScanResultLink);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeTruthy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeTruthy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeTruthy();
  });

  it("should throw appropriate errors when adding various types of incorrect links", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // link already exist
    expect(() => {
      service.addLink(mockScanResultLink);
    }).toThrowError(new RegExp("already exists"));

    const sameLinkDifferentID = {
      ...mockScanResultLink,
      linkID: "link-2",
    };

    // same link but different id already exist
    expect(() => {
      service.addLink(sameLinkDifferentID);
    }).toThrowError(new RegExp("exists"));

    // link's target operator or port doesn't exist
    expect(() => {
      service.addLink(mockScanSentimentLink);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));

    // link's source operator or port doesn't exist
    expect(() => {
      service.addLink(mockSentimentResultLink);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));

    // add another operator for tests below
    service.addOperator(mockSentimentPredicate, mockPoint);

    // link source portID doesn't exist (no output port for source operator)
    expect(() => {
      service.addLink(mockFalseResultSentimentLink);
    }).toThrowError(new RegExp("on output ports of the source operator"));

    // link target portID doesn't exist (no input port for target operator)

    expect(() => {
      service.addLink(mockFalseSentimentScanLink);
    }).toThrowError(new RegExp("on input ports of the target operator"));
  });

  it("should delete a link by link ID from both jointjs and texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // test delete by link ID
    service.deleteLinkWithID(mockScanResultLink.linkID);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeFalsy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeFalsy();
  });

  it("should delete a link by source and target from both jointjs and texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink);

    // test delete by link source and target
    service.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    expect(texeraGraph.hasLink(mockScanResultLink.source, mockScanResultLink.target)).toBeFalsy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeFalsy();
    expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeFalsy();
  });

  it("should throw an error when trying to delete non-existing link", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);

    expect(() => {
      service.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));

    expect(() => {
      service.deleteLinkWithID(mockScanResultLink.linkID);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));
  });

  it("should set operator property to texera graph correctly", () => {
    service.addOperator(mockScanPredicate, mockPoint);

    const newProperty = { table: "test-table" };
    service.setOperatorProperty(mockScanPredicate.operatorID, newProperty);

    const operator = texeraGraph.getOperator(mockScanPredicate.operatorID);
    if (!operator) {
      throw new Error(`operator ${mockScanPredicate.operatorID} doesn't exist`);
    }
    expect(operator.operatorProperties).toEqual(newProperty);
  });

  it("should throw an error when trying to set operator property of an nonexist operator", () => {
    expect(() => {
      const newProperty = { table: "test-table" };
      service.setOperatorProperty(mockScanPredicate.operatorID, newProperty);
    }).toThrowError(new RegExp("does not exist|doesn't exist"));
  });

  it("should handle delete an operator causing connected links to be deleted correctly", () => {
    // add operator scan, sentiment, and result
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockSentimentPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    // add link scan -> result, and sentiment -> result
    service.addLink(mockScanResultLink);
    service.addLink(mockSentimentResultLink);

    // delete result operator, should cause two links to be deleted as well
    service.deleteOperator(mockResultPredicate.operatorID);

    expect(texeraGraph.getAllOperators().length).toEqual(2);
    expect(texeraGraph.getAllLinks().length).toEqual(0);
  });

  it("should reformat the workflow", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockSentimentPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    // add link scan -> result, and sentiment -> result
    service.addLink(mockScanResultLink);
    service.addLink(mockSentimentResultLink);

    service.autoLayoutWorkflow();

    // test it's actually reformated
    let sentimentOpPos = service.getJointGraphWrapper().getElementPosition(mockSentimentPredicate.operatorID);
    let resultOpPos = service.getJointGraphWrapper().getElementPosition(mockResultPredicate.operatorID);

    expect(sentimentOpPos).not.toEqual(mockPoint);
    expect(resultOpPos).not.toEqual(mockPoint);

    // test undo reformat restoring the original positions
    expect(undoRedo.canUndo()).toBeTruthy();
    //
    // undoRedo.undoAction();
    // sentimentOpPos = service.getJointGraphWrapper().getElementPosition(mockSentimentPredicate.operatorID);
    // resultOpPos = service.getJointGraphWrapper().getElementPosition(mockResultPredicate.operatorID);
    //
    // expect(sentimentOpPos).toEqual(mockPoint);
    // expect(resultOpPos).toEqual(mockPoint);
  });

  it("should reload a workflow, repopulating the graph and clearing the undo/redo stacks", () => {
    const settings: WorkflowSettings = {
      dataTransferBatchSize: 250,
      executionMode: ExecutionMode.MATERIALIZED,
    };
    const workflow: Workflow = {
      ...DEFAULT_WORKFLOW,
      name: "Reloaded WF",
      content: {
        operators: [mockScanPredicate, mockResultPredicate],
        operatorPositions: {
          [mockScanPredicate.operatorID]: mockPoint,
          [mockResultPredicate.operatorID]: mockPoint,
        },
        links: [mockScanResultLink],
        commentBoxes: [{ ...mockCommentBox, commentBoxID: "commentBox-1" }],
        settings,
      },
    };
    const clearUndoSpy = vi.spyOn(undoRedo, "clearUndoStack");
    const clearRedoSpy = vi.spyOn(undoRedo, "clearRedoStack");
    const restoreSpy = vi.spyOn(service.getJointGraphWrapper(), "restoreDefaultZoomAndOffset");

    service.reloadWorkflow(workflow);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(texeraGraph.hasOperator(mockResultPredicate.operatorID)).toBeTruthy();
    expect(texeraGraph.hasLinkWithID(mockScanResultLink.linkID)).toBeTruthy();
    expect(texeraGraph.hasCommentBox("commentBox-1")).toBeTruthy();
    expect(service.getWorkflowMetadata().name).toEqual("Reloaded WF");
    expect(service.getWorkflowSettings().dataTransferBatchSize).toEqual(250);
    expect(clearUndoSpy).toHaveBeenCalled();
    expect(clearRedoSpy).toHaveBeenCalled();
    expect(restoreSpy).toHaveBeenCalled();
  });

  it("should throw when a reloaded operator is missing its position", () => {
    const workflow: Workflow = {
      ...DEFAULT_WORKFLOW,
      content: {
        operators: [mockScanPredicate],
        operatorPositions: {},
        links: [],
        commentBoxes: [],
        settings: { dataTransferBatchSize: 100, executionMode: ExecutionMode.PIPELINED },
      },
    };
    expect(() => service.reloadWorkflow(workflow)).toThrowError(
      new RegExp(`position error: ${mockScanPredicate.operatorID}`)
    );
  });

  it("should empty the graph and skip viewport restore when reloading undefined", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    const restoreSpy = vi.spyOn(service.getJointGraphWrapper(), "restoreDefaultZoomAndOffset");

    service.reloadWorkflow(undefined, false, false);

    expect(texeraGraph.getAllOperators().length).toEqual(0);
    expect(restoreSpy).not.toHaveBeenCalled();
  });

  it("should clear the workflow back to its defaults", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.setWorkflowName("Something");
    service.setWorkflowDataTransferBatchSize(999);
    service.setHighlightingEnabled(true);

    service.clearWorkflow();

    expect(service.getWorkflowMetadata()).toEqual(DEFAULT_WORKFLOW);
    expect(service.getWorkflowSettings().dataTransferBatchSize).toEqual(100);
    expect(service.getWorkflowSettings().executionMode).toEqual(ExecutionMode.PIPELINED);
    expect(texeraGraph.getAllOperators().length).toEqual(0);
    expect(service.getHighlightingEnabled()).toBeFalsy();
  });

  it("should emit on workflowChanged when resetting as a new workflow", () => {
    let changed = false;
    service.workflowChanged().subscribe(() => (changed = true));

    service.resetAsNewWorkflow();

    expect(changed).toBeTruthy();
  });

  it("should only emit metadata changes when the reference changes", () => {
    const current = service.getWorkflowMetadata();
    let emitCount = 0;
    service.workflowMetaDataChanged().subscribe(() => (emitCount += 1));

    // identical reference -> no emission
    service.setWorkflowMetadata(current);
    expect(emitCount).toEqual(0);

    // new metadata -> emission
    service.setWorkflowMetadata({ ...DEFAULT_WORKFLOW, name: "renamed" });
    expect(emitCount).toEqual(1);
    expect(service.getWorkflowMetadata().name).toEqual("renamed");
  });

  it("should set the workflow name, defaulting blank names", () => {
    service.setWorkflowName("   ");
    expect(service.getWorkflowMetadata().name).toEqual(DEFAULT_WORKFLOW_NAME);

    service.setWorkflowName("My Workflow");
    expect(service.getWorkflowMetadata().name).toEqual("My Workflow");
  });

  it("should manage workflow settings and publish state", () => {
    expect(service.getWorkflowSettings().dataTransferBatchSize).toEqual(100);
    expect(service.getWorkflowSettings().executionMode).toEqual(ExecutionMode.PIPELINED);

    // non-positive batch sizes are ignored, positive ones are applied
    service.setWorkflowDataTransferBatchSize(0);
    expect(service.getWorkflowSettings().dataTransferBatchSize).toEqual(100);
    service.setWorkflowDataTransferBatchSize(400);
    expect(service.getWorkflowSettings().dataTransferBatchSize).toEqual(400);

    service.updateExecutionMode(ExecutionMode.MATERIALIZED);
    expect(service.getWorkflowSettings().executionMode).toEqual(ExecutionMode.MATERIALIZED);

    service.setWorkflowIsPublished(1);
    expect(service.getWorkflowMetadata().isPublished).toEqual(1);

    const settings: WorkflowSettings = { dataTransferBatchSize: 42, executionMode: ExecutionMode.PIPELINED };
    service.setWorkflowSettings(settings);
    expect(service.getWorkflowSettings()).toEqual(settings);
  });

  it("should toggle the shared-editing connection through temp workflow", () => {
    const wsProvider = texeraGraph.sharedModel.wsProvider;
    // setTempWorkflow only disconnects when the provider is in the "should connect" state,
    // so force it on to make the disconnect assertion deterministic.
    (wsProvider as any).shouldConnect = true;
    const disconnectSpy = vi.spyOn(wsProvider, "disconnect").mockImplementation(() => {});
    const connectSpy = vi.spyOn(wsProvider, "connect").mockImplementation(() => {});
    const tempWorkflow: Workflow = {
      ...DEFAULT_WORKFLOW,
      content: {
        operators: [],
        operatorPositions: {},
        links: [],
        commentBoxes: [],
        settings: { dataTransferBatchSize: 100, executionMode: ExecutionMode.PIPELINED },
      },
    };

    service.setTempWorkflow(tempWorkflow);
    expect(disconnectSpy).toHaveBeenCalled();
    expect(service.getTempWorkflow()).toBe(tempWorkflow);

    service.resetTempWorkflow();
    expect(connectSpy).toHaveBeenCalled();
    expect(service.getTempWorkflow()).toBeUndefined();
  });

  it("should add a dynamic input port to both the texera and joint graphs", () => {
    const dynamicOp: OperatorPredicate = {
      ...mockMultiInputOutputPredicate,
      dynamicInputPorts: true,
      dynamicOutputPorts: true,
    };
    service.addOperator(dynamicOp, mockPoint);
    const element = jointGraph.getCell(dynamicOp.operatorID) as joint.dia.Element;
    const beforeInputs = texeraGraph.getOperator(dynamicOp.operatorID).inputPorts.length;
    const beforePorts = element.getPorts().length;

    service.addPort(dynamicOp.operatorID, true);

    expect(texeraGraph.getOperator(dynamicOp.operatorID).inputPorts.length).toEqual(beforeInputs + 1);
    expect(element.getPorts().length).toEqual(beforePorts + 1);
  });

  it("should honor disallowMultiInputs when adding an input port", () => {
    const dynamicOp: OperatorPredicate = { ...mockMultiInputOutputPredicate, dynamicInputPorts: true };
    service.addOperator(dynamicOp, mockPoint);

    service.addPort(dynamicOp.operatorID, true, true);

    const inputPorts = texeraGraph.getOperator(dynamicOp.operatorID).inputPorts;
    expect(inputPorts[inputPorts.length - 1].disallowMultiInputs).toBe(true);
  });

  it("should remove the last dynamic port from both the texera and joint graphs", () => {
    const dynamicOp: OperatorPredicate = { ...mockMultiInputOutputPredicate, dynamicInputPorts: true };
    service.addOperator(dynamicOp, mockPoint);
    const element = jointGraph.getCell(dynamicOp.operatorID) as joint.dia.Element;
    // Add a dynamic input port first, then remove it — so this exercises the dynamic-port
    // removal path (round-tripping back to the original counts) rather than just deleting a
    // pre-existing port.
    const baseInputs = texeraGraph.getOperator(dynamicOp.operatorID).inputPorts.length;
    const basePorts = element.getPorts().length;
    service.addPort(dynamicOp.operatorID, true);
    expect(texeraGraph.getOperator(dynamicOp.operatorID).inputPorts.length).toEqual(baseInputs + 1);

    service.removePort(dynamicOp.operatorID, true);

    expect(texeraGraph.getOperator(dynamicOp.operatorID).inputPorts.length).toEqual(baseInputs);
    expect(element.getPorts().length).toEqual(basePorts);
  });

  it("should throw when adding a port to an operator without dynamic ports", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    expect(() => service.addPort(mockScanPredicate.operatorID, true)).toThrowError(
      new RegExp("does not have dynamic input ports")
    );
  });

  it("should disable and enable operators, reflecting in the graph and change stream", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    let event: { newDisabled: readonly string[]; newEnabled: readonly string[] } | undefined;
    texeraGraph.getDisabledOperatorsChangedStream().subscribe(e => (event = e));

    service.disableOperators([mockScanPredicate.operatorID]);
    expect(texeraGraph.getDisabledOperators().has(mockScanPredicate.operatorID)).toBeTruthy();
    expect(event?.newDisabled).toContain(mockScanPredicate.operatorID);

    service.enableOperators([mockScanPredicate.operatorID]);
    expect(texeraGraph.getDisabledOperators().has(mockScanPredicate.operatorID)).toBeFalsy();
    expect(event?.newEnabled).toContain(mockScanPredicate.operatorID);
  });

  it("should mark and unmark operators for result reuse", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    let event: { newReuseCacheOps: readonly string[]; newUnreuseCacheOps: readonly string[] } | undefined;
    texeraGraph.getReuseCacheOperatorsChangedStream().subscribe(e => (event = e));

    service.markReuseResults([mockScanPredicate.operatorID]);
    expect(texeraGraph.getOperatorsMarkedForReuseResult().has(mockScanPredicate.operatorID)).toBeTruthy();
    expect(event?.newReuseCacheOps).toContain(mockScanPredicate.operatorID);

    service.removeMarkReuseResults([mockScanPredicate.operatorID]);
    expect(texeraGraph.getOperatorsMarkedForReuseResult().has(mockScanPredicate.operatorID)).toBeFalsy();
    expect(event?.newUnreuseCacheOps).toContain(mockScanPredicate.operatorID);
  });

  it("should set and unset operators for viewing results", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    let event: { newViewResultOps: readonly string[]; newUnviewResultOps: readonly string[] } | undefined;
    texeraGraph.getViewResultOperatorsChangedStream().subscribe(e => (event = e));

    service.setViewOperatorResults([mockScanPredicate.operatorID]);
    expect(texeraGraph.getOperatorsToViewResult().has(mockScanPredicate.operatorID)).toBeTruthy();
    expect(event?.newViewResultOps).toContain(mockScanPredicate.operatorID);

    service.unsetViewOperatorResults([mockScanPredicate.operatorID]);
    expect(texeraGraph.getOperatorsToViewResult().has(mockScanPredicate.operatorID)).toBeFalsy();
    expect(event?.newUnviewResultOps).toContain(mockScanPredicate.operatorID);
  });

  it("should highlight and unhighlight ports honoring multiselect mode", () => {
    const jointGraphWrapper = service.getJointGraphWrapper();
    const portA: LogicalPort = { operatorID: mockScanPredicate.operatorID, portID: "output-0" };
    const portB: LogicalPort = { operatorID: mockSentimentPredicate.operatorID, portID: "input-0" };

    // multiselect on -> both ports stay highlighted
    service.highlightPorts(true, portA);
    service.highlightPorts(true, portB);
    expect(jointGraphWrapper.getCurrentHighlightedPortIDs().length).toEqual(2);

    service.unhighlightPorts(portA, portB);
    expect(jointGraphWrapper.getCurrentHighlightedPortIDs().length).toEqual(0);

    // multiselect off -> highlighting a new port replaces the previous one
    service.highlightPorts(false, portA);
    service.highlightPorts(false, portB);
    expect(jointGraphWrapper.getCurrentHighlightedPortIDs()).toEqual([portB]);
  });

  it("should highlight mixed operator and comment-box elements with multiselect", () => {
    const jointGraphWrapper = service.getJointGraphWrapper();
    service.addOperator(mockScanPredicate, mockPoint);
    const commentBox = { ...mockCommentBox, commentBoxID: "commentBox-hl" };
    service.addCommentBox(commentBox);

    service.highlightElements(true, mockScanPredicate.operatorID, commentBox.commentBoxID);

    expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toContain(mockScanPredicate.operatorID);
    expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toContain(commentBox.commentBoxID);
  });

  it("should update an operator's version in the graph", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.setOperatorVersion(mockScanPredicate.operatorID, "scan-v2");
    expect(texeraGraph.getOperator(mockScanPredicate.operatorID).operatorVersion).toEqual("scan-v2");
  });

  it("should set a port property on an existing port and throw on a missing one", () => {
    service.addOperator(mockSentimentPredicate, mockPoint);
    const port: LogicalPort = { operatorID: mockSentimentPredicate.operatorID, portID: "input-0" };
    const portProperty = { partitionInfo: { type: "none" }, dependencies: [] };

    service.setPortProperty(port, portProperty);
    expect(texeraGraph.getPortDescription(port)?.partitionRequirement).toEqual({ type: "none" });

    const missingPort: LogicalPort = { operatorID: mockSentimentPredicate.operatorID, portID: "input-99" };
    expect(() => service.setPortProperty(missingPort, portProperty)).toThrowError(new RegExp("does not exist"));
  });

  it("should compute the top-left position across all operators", () => {
    service.addOperator(mockScanPredicate, { x: 300, y: 400 });
    service.addOperator(mockResultPredicate, { x: 100, y: 250 });

    service.calculateTopLeftOperatorPosition();

    expect(service.getCenterPoint()).toEqual({ x: 100, y: 250 });
  });

  it("should leave the center point at its default when there are no operators", () => {
    const before = service.getCenterPoint();

    service.calculateTopLeftOperatorPosition();

    expect(service.getCenterPoint()).toEqual(before);
    expect(service.getCenterPoint()).toEqual({ x: 0, y: 0 });
  });

  it("should toggle the workflow-modification lock and emit each state on the stream", () => {
    const emitted: boolean[] = [];
    service.getWorkflowModificationEnabledStream().subscribe(v => emitted.push(v));

    // BehaviorSubject replays the initial "enabled" state on subscription
    expect(service.checkWorkflowModificationEnabled()).toBeTruthy();

    service.disableWorkflowModification();
    expect(service.checkWorkflowModificationEnabled()).toBeFalsy();

    // enable only takes effect when previously disabled (and workflow not readonly)
    service.enableWorkflowModification();
    expect(service.checkWorkflowModificationEnabled()).toBeTruthy();

    expect(emitted).toEqual([true, false, true]);
  });

  it("should expose the underlying joint graph instance", () => {
    expect(service.getJointGraph()).toBe(jointGraph);
  });

  it("should resolve a conflicting port ID when adding a dynamic input port", () => {
    // inputPorts already contains "input-1" but has length 1, so the first candidate
    // portID ("input-" + 1) collides and the loop must bump the suffix to "input-2".
    const gapOp: OperatorPredicate = {
      ...mockMultiInputOutputPredicate,
      operatorID: "gap-op",
      dynamicInputPorts: true,
      inputPorts: [{ portID: "input-1" }],
    };
    service.addOperator(gapOp, mockPoint);

    service.addPort(gapOp.operatorID, true);

    const portIDs = texeraGraph.getOperator(gapOp.operatorID).inputPorts.map(p => p.portID);
    expect(portIDs).toContain("input-2");
    expect(portIDs).not.toContain("input-1input-1");
  });

  it("should add a dynamic output port to an operator that allows them", () => {
    const dynOutOp: OperatorPredicate = {
      ...mockMultiInputOutputPredicate,
      operatorID: "dyn-out",
      dynamicOutputPorts: true,
    };
    service.addOperator(dynOutOp, mockPoint);
    const beforeOutputs = texeraGraph.getOperator(dynOutOp.operatorID).outputPorts.length;

    service.addPort(dynOutOp.operatorID, false);

    expect(texeraGraph.getOperator(dynOutOp.operatorID).outputPorts.length).toEqual(beforeOutputs + 1);
  });

  it("should throw when adding an output port to an operator without dynamic output ports", () => {
    const noOutOp: OperatorPredicate = { ...mockMultiInputOutputPredicate, operatorID: "no-out" };
    service.addOperator(noOutOp, mockPoint);

    expect(() => service.addPort(noOutOp.operatorID, false)).toThrowError(
      new RegExp("does not have dynamic output ports")
    );
  });

  it("should throw when specifying disallowMultiInputs on an output port", () => {
    const dynOutOp: OperatorPredicate = {
      ...mockMultiInputOutputPredicate,
      operatorID: "dyn-out-2",
      dynamicOutputPorts: true,
    };
    service.addOperator(dynOutOp, mockPoint);

    expect(() => service.addPort(dynOutOp.operatorID, false, true)).toThrowError(
      new RegExp("disallowMultiInputs property of an output port should not be specified")
    );
  });

  it("should add a comment box together with its initial comments", () => {
    const comment = { content: "hello", creationTime: "2020-01-01", creatorName: "me", creatorID: 1 };
    const box = { ...mockCommentBox, commentBoxID: "commentBox-with-comment", comments: [comment] };

    service.addCommentBox(box);

    expect(texeraGraph.hasCommentBox("commentBox-with-comment")).toBeTruthy();
    expect(texeraGraph.getCommentBox("commentBox-with-comment").comments.length).toEqual(1);
    expect(texeraGraph.getCommentBox("commentBox-with-comment").comments[0].content).toEqual("hello");
  });

  it("should delete a comment box and throw when deleting a non-existing one", () => {
    const box = { ...mockCommentBox, commentBoxID: "commentBox-del" };
    service.addCommentBox(box);
    expect(texeraGraph.hasCommentBox("commentBox-del")).toBeTruthy();

    service.deleteCommentBox("commentBox-del");
    expect(texeraGraph.hasCommentBox("commentBox-del")).toBeFalsy();

    expect(() => service.deleteCommentBox("commentBox-missing")).toThrowError(new RegExp("does not exist"));
  });

  it("should add, edit, and delete a comment inside a comment box", () => {
    const box = { ...mockCommentBox, commentBoxID: "commentBox-cmt" };
    service.addCommentBox(box);
    const comment = { content: "first", creationTime: "2020-02-02", creatorName: "author", creatorID: 7 };

    service.addComment(comment, "commentBox-cmt");
    expect(texeraGraph.getCommentBox("commentBox-cmt").comments.length).toEqual(1);

    service.editComment(7, "2020-02-02", "commentBox-cmt", "edited");
    expect(texeraGraph.getCommentBox("commentBox-cmt").comments[0].content).toEqual("edited");

    service.deleteComment(7, "2020-02-02", "commentBox-cmt");
    expect(texeraGraph.getCommentBox("commentBox-cmt").comments.length).toEqual(0);
  });

  it("should delete multiple operators along with their connecting links", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockSentimentPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanSentimentLink);
    service.addLink(mockSentimentResultLink);

    // duplicate IDs verify the internal Set de-duplication path too
    service.deleteOperatorsAndLinks([
      mockScanPredicate.operatorID,
      mockSentimentPredicate.operatorID,
      mockScanPredicate.operatorID,
    ]);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    expect(texeraGraph.hasOperator(mockSentimentPredicate.operatorID)).toBeFalsy();
    expect(texeraGraph.hasOperator(mockResultPredicate.operatorID)).toBeTruthy();
    expect(texeraGraph.getAllLinks().length).toEqual(0);
  });

  it("should delete a downstream operator and drop links matched by their target", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    service.addOperator(mockResultPredicate, mockPoint);
    service.addLink(mockScanResultLink); // scan -> result

    // deleting only the target operator: the link matches on its target, not its source,
    // exercising the right-hand operand of the link filter's OR condition
    service.deleteOperatorsAndLinks([mockResultPredicate.operatorID]);

    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(texeraGraph.hasOperator(mockResultPredicate.operatorID)).toBeFalsy();
    expect(texeraGraph.getAllLinks().length).toEqual(0);
  });

  it("should record comment-box positions during auto layout", () => {
    service.addOperator(mockScanPredicate, mockPoint);
    const box = { ...mockCommentBox, commentBoxID: "commentBox-layout" };
    service.addCommentBox(box);

    service.autoLayoutWorkflow();

    expect(texeraGraph.sharedModel.elementPositionMap.get("commentBox-layout")).toBeDefined();
  });

  it("should emit open and close events on the result-panel stream", () => {
    const events: boolean[] = [];
    service.resultPanelOpen$.subscribe(v => events.push(v));

    service.openResultPanel();
    service.closeResultPanel();

    expect(events).toEqual([true, false]);
  });

  it("should not replace the workflow settings object when given the same reference", () => {
    const current = service.getWorkflowSettings();

    service.setWorkflowSettings(current);

    expect(service.getWorkflowSettings()).toBe(current);
  });

  it("should assemble workflow content and the full workflow from graph state", () => {
    service.addOperator(mockScanPredicate, { x: 10, y: 20 });
    service.addOperator(mockResultPredicate, { x: 30, y: 40 });
    service.addLink(mockScanResultLink);
    service.setWorkflowName("Content WF");

    const content = service.getWorkflowContent();
    expect(content.operators.map(o => o.operatorID).sort()).toEqual([
      mockScanPredicate.operatorID,
      mockResultPredicate.operatorID,
    ]);
    expect(content.links.length).toEqual(1);
    expect(content.operatorPositions[mockScanPredicate.operatorID]).toEqual({ x: 10, y: 20 });
    expect(content.operatorPositions[mockResultPredicate.operatorID]).toEqual({ x: 30, y: 40 });
    expect(content.settings).toEqual(service.getWorkflowSettings());

    const workflow = service.getWorkflow();
    expect(workflow.name).toEqual("Content WF");
    expect(workflow.content.operators.length).toEqual(2);
    expect(workflow.content.links.length).toEqual(1);
  });

  describe("formBinding (Form View definition)", () => {
    const config = {
      instruction: { title: "How to use this", body: "Pick a file, then Run." },
      fields: [
        {
          id: "f1",
          operatorID: mockScanPredicate.operatorID,
          propertyKey: "tableName",
          displayName: "Input table",
          helpText: "Which table to read.",
        },
      ],
      resultOperatorIds: [mockResultPredicate.operatorID],
    };

    it("should start empty for a workflow that was never set up", () => {
      expect(service.getFormBinding()).toEqual({ fields: [], resultOperatorIds: [] });
    });

    it("should round-trip through workflow content", () => {
      service.addOperator(mockScanPredicate, { x: 10, y: 20 });
      service.setFormBinding(config);

      expect(service.getWorkflowContent().formBinding).toEqual(config);
    });

    // The definition must survive being saved and opened again, which is what makes it
    // travel with clone, version and publish for free.
    it("should be restored when a workflow carrying one is reloaded", () => {
      const workflow: Workflow = {
        ...DEFAULT_WORKFLOW,
        content: {
          operators: [mockScanPredicate],
          operatorPositions: { [mockScanPredicate.operatorID]: mockPoint },
          links: [],
          commentBoxes: [],
          settings: undefined as any,
          formBinding: config,
        },
      };

      service.reloadWorkflow(workflow, false, false);

      expect(service.getFormBinding()).toEqual(config);
    });

    it("should fall back to an empty definition for a workflow without one", () => {
      service.setFormBinding(config);

      service.reloadWorkflow(
        {
          ...DEFAULT_WORKFLOW,
          content: {
            operators: [],
            operatorPositions: {},
            links: [],
            commentBoxes: [],
            settings: undefined as any,
          },
        },
        false,
        false
      );

      expect(service.getFormBinding()).toEqual({ fields: [], resultOperatorIds: [] });
    });

    // Starting a blank workflow must not carry the previous one's definition, or it would
    // leak into the new workflow's first save.
    it("should clear a leftover definition when reset to a blank workflow", () => {
      service.setFormBinding(config);
      expect(service.getFormBinding()).toEqual(config);

      service.reloadWorkflow(undefined, false, false);

      expect(service.getFormBinding()).toEqual({ fields: [], resultOperatorIds: [] });
    });

    // A plain workflow must not stamp an empty formBinding into its content, or every existing
    // workflow's first save would diff and cut a needless version. Mirrors the agent-service rule.
    it("should omit formBinding from content for a workflow that never had one", () => {
      service.reloadWorkflow(undefined, false, false);

      expect("formBinding" in service.getWorkflowContent()).toBe(false);
    });

    // Editing the form has to reach the same autosave that canvas edits use.
    it("should report an edit through workflowChanged", () => {
      const seen: unknown[] = [];
      const sub = service.workflowChanged().subscribe(v => seen.push(v));

      service.setFormBinding(config);

      expect(seen.length).toEqual(1);
      sub.unsubscribe();
    });

    // Opening a workflow is not an edit; announcing it would save on every open.
    it("should stay silent while a workflow is being opened", () => {
      const seen: unknown[] = [];
      const sub = service.formBindingChanged$.subscribe(v => seen.push(v));

      service.hydrateFormBinding(config);

      expect(seen.length).toEqual(0);
      expect(service.getFormBinding()).toEqual(config);
      sub.unsubscribe();
    });
  });

  it("should clear pre-existing comment boxes and fall back to default settings when reloading", () => {
    service.addCommentBox({ ...mockCommentBox, commentBoxID: "commentBox-old" });
    expect(texeraGraph.hasCommentBox("commentBox-old")).toBeTruthy();

    // Make the assertion meaningful by starting from a non-default value.
    service.setWorkflowSettings({ dataTransferBatchSize: 1, executionMode: ExecutionMode.MATERIALIZED });
    const env = (service as any).config.env;

    const workflow: Workflow = {
      ...DEFAULT_WORKFLOW,
      content: {
        operators: [mockScanPredicate],
        operatorPositions: { [mockScanPredicate.operatorID]: mockPoint },
        links: [],
        commentBoxes: [],
        settings: undefined as any,
      },
    };

    service.reloadWorkflow(workflow, false, false);

    expect(texeraGraph.hasCommentBox("commentBox-old")).toBeFalsy();
    expect(texeraGraph.hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    // settings was undefined in the reloaded content, so defaults are applied
    expect(service.getWorkflowSettings()).toEqual({
      dataTransferBatchSize: env.defaultDataTransferBatchSize,
      executionMode: env.defaultExecutionMode,
    });
  });

  it("should drag all highlighted elements together when one highlighted operator is moved", () => {
    const wrapper = service.getJointGraphWrapper();
    service.addOperator(mockScanPredicate, { x: 100, y: 100 });
    service.addOperator(mockSentimentPredicate, { x: 300, y: 300 });
    // comment-box ID must contain "commentBox" to exercise the comment-box persistence branch
    const commentBoxID = "commentBox-drag";
    service.addCommentBox({ ...mockCommentBox, commentBoxID });

    service.highlightElements(true, mockScanPredicate.operatorID, mockSentimentPredicate.operatorID, commentBoxID);

    const scanBefore = wrapper.getElementPosition(mockScanPredicate.operatorID);
    const sentimentBefore = wrapper.getElementPosition(mockSentimentPredicate.operatorID);
    const commentBefore = wrapper.getElementPosition(commentBoxID);
    const offset = { x: 50, y: 30 };

    // moving the highlighted scan operator triggers the drag handler, which moves the
    // other highlighted elements by the same offset
    wrapper.setAbsolutePosition(mockScanPredicate.operatorID, scanBefore.x + offset.x, scanBefore.y + offset.y);

    expect(wrapper.getElementPosition(mockSentimentPredicate.operatorID)).toEqual({
      x: sentimentBefore.x + offset.x,
      y: sentimentBefore.y + offset.y,
    });
    expect(wrapper.getElementPosition(commentBoxID)).toEqual({
      x: commentBefore.x + offset.x,
      y: commentBefore.y + offset.y,
    });
    expect(texeraGraph.sharedModel.elementPositionMap.get(mockScanPredicate.operatorID)).toEqual({
      x: scanBefore.x + offset.x,
      y: scanBefore.y + offset.y,
    });
    expect(texeraGraph.sharedModel.elementPositionMap.get(mockSentimentPredicate.operatorID)).toEqual({
      x: sentimentBefore.x + offset.x,
      y: sentimentBefore.y + offset.y,
    });
  });

  /*
   * The remaining half-taken arms on this service: an optional argument left out, a guard whose
   * "already in that state" side never ran, and the comparisons in the top-left calculation.
   */
  it("adds operators without links or comment boxes when neither is supplied", () => {
    service.addOperatorsAndLinks([
      { op: mockScanPredicate, pos: { x: 10, y: 20 } },
      { op: mockResultPredicate, pos: { x: 30, y: 40 } },
    ]);

    expect(
      texeraGraph
        .getAllOperators()
        .map(o => o.operatorID)
        .sort()
    ).toEqual([mockScanPredicate.operatorID, mockResultPredicate.operatorID].sort());
    expect(texeraGraph.getAllLinks()).toEqual([]);
    expect(texeraGraph.getAllCommentBoxes()).toEqual([]);
  });

  it("does nothing when modification is enabled again while already enabled", () => {
    const emitted: boolean[] = [];
    service.getWorkflowModificationEnabledStream().subscribe(v => emitted.push(v));
    // Starts enabled, so this call has nothing to do and must not re-announce it.
    service.enableWorkflowModification();

    expect(service.checkWorkflowModificationEnabled()).toBeTruthy();
    expect(emitted).toEqual([true]);
  });

  it("takes the smaller of each coordinate independently when computing the top-left", () => {
    // The x guard is false for the second operator and the y guard is true, so each
    // comparison decides the result once in both directions.
    service.addOperator(mockScanPredicate, { x: 100, y: 400 });
    service.addOperator(mockResultPredicate, { x: 300, y: 250 });
    service.addOperator(mockSentimentPredicate, { x: 50, y: 500 });

    service.calculateTopLeftOperatorPosition();

    // x comes from the third operator and y from the second, so neither comparison
    // decides both coordinates.
    expect(service.getCenterPoint()).toEqual({ x: 50, y: 250 });
  });

  it("disconnects the shared-editing provider only when it is meant to be connected", () => {
    const disconnect = vi.spyOn(texeraGraph.sharedModel.wsProvider, "disconnect").mockImplementation(() => {});
    const workflow = service.getWorkflow();

    (texeraGraph.sharedModel.wsProvider as any).shouldConnect = false;
    service.setTempWorkflow(workflow);
    expect(disconnect).not.toHaveBeenCalled();

    (texeraGraph.sharedModel.wsProvider as any).shouldConnect = true;
    service.setTempWorkflow(workflow);
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
