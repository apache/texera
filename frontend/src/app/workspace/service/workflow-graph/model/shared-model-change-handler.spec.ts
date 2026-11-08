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

import { TestBed } from "@angular/core/testing";
import * as joint from "jointjs";
import * as Y from "yjs";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { WorkflowActionService } from "./workflow-action.service";
import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockSentimentPredicate,
} from "./mock-workflow-data";
import { Comment, OperatorLink, OperatorPredicate, Point } from "../../../types/workflow-common.interface";
import { createYTypeFromObject } from "../../../types/shared-editing.interface";

/**
 * SharedModelChangeHandler is constructed internally by WorkflowActionService. It wires yjs deep-observers on the
 * TexeraGraph's shared model (operatorIDMap, operatorLinkMap, elementPositionMap, commentBoxMap) and triggers the
 * TexeraGraph's change subjects while mirroring the change onto the JointJS graph.
 *
 * These tests build the real service (so the handler is wired to real collaborators: WorkflowGraph, joint.dia.Graph,
 * JointGraphWrapper, JointUIService) and drive the shared model through the graph's mutation API, asserting the
 * corresponding subjects fire with the right payloads and that JointJS is kept in sync.
 */
describe("SharedModelChangeHandler", () => {
  let service: WorkflowActionService;
  let texeraGraph: WorkflowGraph;
  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;

  beforeEach(() => {
    // silence the informational console.log calls emitted by the graph/handler on some mutations
    vi.spyOn(console, "log").mockImplementation(() => {});

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
    });
    service = TestBed.inject(WorkflowActionService);
    texeraGraph = (service as any).texeraGraph;
    jointGraph = service.getJointGraph();
    jointGraphWrapper = service.getJointGraphWrapper();
  });

  afterEach(() => {
    // Tear down the underlying shared model (awareness / websocket provider) to avoid leaks across tests.
    // If this throws, the test should fail so we don't silently leak shared-editing resources.
    texeraGraph.destroyYModel();
    vi.restoreAllMocks();
  });

  /**
   * The handler's operator-add observer reads the operator's position out of elementPositionMap, so a real add must
   * write both maps inside a single transaction (this mirrors WorkflowActionService.addOperator).
   */
  function addOperatorWithPosition(operator: OperatorPredicate, point: Point = mockPoint): void {
    texeraGraph.bundleActions(() => {
      texeraGraph.addOperator(operator);
      texeraGraph.sharedModel.elementPositionMap.set(operator.operatorID, point);
    });
  }

  describe("operator add and delete", () => {
    it("should emit operatorAddSubject and add a joint element when an operator is added", () => {
      let added: OperatorPredicate | undefined;
      const sub = texeraGraph.getOperatorAddStream().subscribe(op => (added = op));

      addOperatorWithPosition(mockScanPredicate);

      expect(added).toBeDefined();
      expect(added!.operatorID).toEqual(mockScanPredicate.operatorID);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      // a single local add highlights the new operator without multi-select
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);
      sub.unsubscribe();
    });

    it("should throw when adding an operator that has no entry in the position map", () => {
      // adding to operatorIDMap alone (no elementPositionMap entry) trips the handler's position guard
      expect(() => texeraGraph.addOperator(mockScanPredicate)).toThrowError(
        new RegExp("does not exist in position map")
      );
    });

    it("should highlight all newly added operators in multi-select mode when several are added together", () => {
      const added: string[] = [];
      const sub = texeraGraph.getOperatorAddStream().subscribe(op => added.push(op.operatorID));

      texeraGraph.bundleActions(() => {
        texeraGraph.addOperator(mockScanPredicate);
        texeraGraph.sharedModel.elementPositionMap.set(mockScanPredicate.operatorID, mockPoint);
        texeraGraph.addOperator(mockResultPredicate);
        texeraGraph.sharedModel.elementPositionMap.set(mockResultPredicate.operatorID, mockPoint);
      });

      expect(added).toEqual([mockScanPredicate.operatorID, mockResultPredicate.operatorID]);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      expect(jointGraph.getCell(mockResultPredicate.operatorID)).toBeTruthy();
      const highlighted = jointGraphWrapper.getCurrentHighlightedOperatorIDs();
      expect(highlighted.length).toEqual(2);
      expect(highlighted).toContain(mockScanPredicate.operatorID);
      expect(highlighted).toContain(mockResultPredicate.operatorID);
      sub.unsubscribe();
    });

    it("should emit operatorDeleteSubject and remove the joint element (unhighlighting first) when an operator is deleted", () => {
      addOperatorWithPosition(mockScanPredicate);
      // the add above left the operator highlighted, so deletion must exercise the unhighlight branch
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

      let deleted: { deletedOperatorID: string } | undefined;
      const sub = texeraGraph.getOperatorDeleteStream().subscribe(v => (deleted = v));

      texeraGraph.deleteOperator(mockScanPredicate.operatorID);

      expect(deleted).toEqual({ deletedOperatorID: mockScanPredicate.operatorID });
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeFalsy();
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
      sub.unsubscribe();
    });
  });

  describe("link add and delete", () => {
    beforeEach(() => {
      addOperatorWithPosition(mockScanPredicate);
      addOperatorWithPosition(mockResultPredicate);
    });

    it("should emit linkAddSubject and add a joint link cell when a valid link is added", () => {
      let addedLink: OperatorLink | undefined;
      const sub = texeraGraph.getLinkAddStream().subscribe(l => (addedLink = l));

      texeraGraph.addLink(mockScanResultLink);

      expect(addedLink).toEqual(mockScanResultLink);
      expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeTruthy();
      sub.unsubscribe();
    });

    it("should emit linkDeleteSubject and remove the joint link cell when a link is deleted", () => {
      texeraGraph.addLink(mockScanResultLink);
      expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeTruthy();

      let deletedLink: { deletedLink: OperatorLink } | undefined;
      const sub = texeraGraph.getLinkDeleteStream().subscribe(v => (deletedLink = v));

      texeraGraph.deleteLinkWithID(mockScanResultLink.linkID);

      expect(deletedLink).toEqual({ deletedLink: mockScanResultLink });
      expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeFalsy();
      sub.unsubscribe();
    });

    it("should repair the shared model (and not emit linkAddSubject) when an invalid link is added directly", () => {
      let addFired = false;
      const sub = texeraGraph.getLinkAddStream().subscribe(() => (addFired = true));

      // a link whose source operator does not exist fails validateAndRepairNewLink and is deleted from the model
      const invalidLink: OperatorLink = {
        linkID: "invalid-link",
        source: { operatorID: "nonexist", portID: "output-0" },
        target: { operatorID: mockResultPredicate.operatorID, portID: "input-0" },
      };
      texeraGraph.sharedModel.operatorLinkMap.set(invalidLink.linkID, invalidLink);

      expect(addFired).toBe(false);
      expect(texeraGraph.hasLinkWithID(invalidLink.linkID)).toBe(false);
      expect(jointGraph.getCell(invalidLink.linkID)).toBeFalsy();
      sub.unsubscribe();
    });
  });

  describe("element position change", () => {
    it("should reposition the joint element when the element position map is updated", () => {
      addOperatorWithPosition(mockScanPredicate, { x: 10, y: 20 });

      texeraGraph.sharedModel.elementPositionMap.set(mockScanPredicate.operatorID, { x: 99, y: 88 });

      const element = jointGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element;
      expect(element.position()).toEqual({ x: 99, y: 88 });
    });
  });

  describe("element position change with no value", () => {
    it("leaves the joint element alone when the update carries no position", () => {
      addOperatorWithPosition(mockScanPredicate);
      const setAbsolute = vi.spyOn(jointGraphWrapper, "setAbsolutePosition");

      // An update whose value is empty: the handler must not try to place the element.
      texeraGraph.sharedModel.elementPositionMap.set(mockScanPredicate.operatorID, null as never);

      expect(setAbsolute).not.toHaveBeenCalled();
    });
  });

  describe("comment box add and delete", () => {
    it("should emit commentBoxAddSubject and add a joint comment element when a comment box is added", () => {
      let addedCommentBox: any;
      const sub = texeraGraph.getCommentBoxAddStream().subscribe(cb => (addedCommentBox = cb));

      texeraGraph.addCommentBox(mockCommentBox);

      expect(addedCommentBox).toBeDefined();
      expect(addedCommentBox.commentBoxID).toEqual(mockCommentBox.commentBoxID);
      expect(jointGraph.getCell(mockCommentBox.commentBoxID)).toBeTruthy();
      sub.unsubscribe();
    });

    it("should remove the joint comment element when a comment box is deleted", () => {
      texeraGraph.addCommentBox(mockCommentBox);
      expect(jointGraph.getCell(mockCommentBox.commentBoxID)).toBeTruthy();

      texeraGraph.deleteCommentBox(mockCommentBox.commentBoxID);

      expect(jointGraph.getCell(mockCommentBox.commentBoxID)).toBeFalsy();
    });
  });

  describe("deep operator changes", () => {
    beforeEach(() => {
      addOperatorWithPosition(mockScanPredicate);
    });

    it("should emit viewResultOperatorChangedSubject when the view-result flag is toggled on and off", () => {
      let change: { newViewResultOps: readonly string[]; newUnviewResultOps: readonly string[] } | undefined;
      const sub = texeraGraph.getViewResultOperatorsChangedStream().subscribe(v => (change = v));

      texeraGraph.setViewOperatorResult(mockScanPredicate.operatorID);
      expect(change).toEqual({ newViewResultOps: [mockScanPredicate.operatorID], newUnviewResultOps: [] });

      texeraGraph.unsetViewOperatorResult(mockScanPredicate.operatorID);
      expect(change).toEqual({ newViewResultOps: [], newUnviewResultOps: [mockScanPredicate.operatorID] });
      sub.unsubscribe();
    });

    it("should emit reuseOperatorChangedSubject when the reuse-cache flag is toggled on and off", () => {
      let change: { newReuseCacheOps: readonly string[]; newUnreuseCacheOps: readonly string[] } | undefined;
      const sub = texeraGraph.getReuseCacheOperatorsChangedStream().subscribe(v => (change = v));

      texeraGraph.markReuseResult(mockScanPredicate.operatorID);
      expect(change).toEqual({ newReuseCacheOps: [mockScanPredicate.operatorID], newUnreuseCacheOps: [] });

      texeraGraph.removeMarkReuseResult(mockScanPredicate.operatorID);
      expect(change).toEqual({ newReuseCacheOps: [], newUnreuseCacheOps: [mockScanPredicate.operatorID] });
      sub.unsubscribe();
    });

    it("should emit disabledOperatorChangedSubject when the operator is disabled and enabled", () => {
      let change: { newDisabled: readonly string[]; newEnabled: readonly string[] } | undefined;
      const sub = texeraGraph.getDisabledOperatorsChangedStream().subscribe(v => (change = v));

      texeraGraph.disableOperator(mockScanPredicate.operatorID);
      expect(change).toEqual({ newDisabled: [mockScanPredicate.operatorID], newEnabled: [] });

      texeraGraph.enableOperator(mockScanPredicate.operatorID);
      expect(change).toEqual({ newDisabled: [], newEnabled: [mockScanPredicate.operatorID] });
      sub.unsubscribe();
    });

    it("should emit operatorPropertyChangeSubject when the operator properties change", () => {
      let change: { operator: OperatorPredicate } | undefined;
      const sub = texeraGraph.getOperatorPropertyChangeStream().subscribe(v => (change = v));

      texeraGraph.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "some-table" });

      expect(change).toBeDefined();
      expect(change!.operator.operatorID).toEqual(mockScanPredicate.operatorID);
      expect(change!.operator.operatorProperties).toEqual({ tableName: "some-table" });
      sub.unsubscribe();
    });

    it("should update local awareness when the currently-edited operator's property changes locally", () => {
      // making the operator the locally-edited one exercises the awareness-update branch in onOperatorPropertyChanged
      texeraGraph.sharedModel.awareness.setLocalState({ currentlyEditing: mockScanPredicate.operatorID } as any);
      const awarenessSpy = vi.spyOn(texeraGraph, "updateSharedModelAwareness");

      let changed = false;
      const sub = texeraGraph.getOperatorPropertyChangeStream().subscribe(() => (changed = true));

      texeraGraph.setOperatorProperty(mockScanPredicate.operatorID, { tableName: "edited" });

      expect(changed).toBe(true);
      expect(awarenessSpy).toHaveBeenCalledWith("changed", mockScanPredicate.operatorID);
      expect(awarenessSpy).toHaveBeenCalledWith("changed", undefined);
      sub.unsubscribe();
    });

    it("should emit operatorPropertyChangeSubject when the whole operatorProperties type is replaced", () => {
      // A wholesale replacement of the operatorProperties key (e.g. from a remote update / undo) is handled by the
      // path-length-1 branch, distinct from the in-place nested update that setOperatorProperty performs.
      let change: { operator: OperatorPredicate } | undefined;
      const sub = texeraGraph.getOperatorPropertyChangeStream().subscribe(v => (change = v));

      texeraGraph
        .getSharedOperatorType(mockScanPredicate.operatorID)
        .set("operatorProperties", createYTypeFromObject({ replaced: "yes" }) as any);

      expect(change).toBeDefined();
      expect(change!.operator.operatorID).toEqual(mockScanPredicate.operatorID);
      expect(change!.operator.operatorProperties).toEqual({ replaced: "yes" });
      sub.unsubscribe();
    });

    it("should throw on an unrecognized nested operator change", () => {
      // Mutating a nested field the handler does not know how to sync (operatorVersion text) hits the defensive guard.
      expect(() =>
        (texeraGraph.getSharedOperatorType(mockScanPredicate.operatorID).get("operatorVersion") as any).insert(0, "v2-")
      ).toThrowError(new RegExp("undefined operation on shared type"));
    });

    it("should emit operatorDisplayNameChangedSubject when the custom display name text changes", () => {
      // rebuild the operator with a customDisplayName so the shared model holds a Y.Text we can mutate
      texeraGraph.deleteOperator(mockScanPredicate.operatorID);
      const namedOperator: OperatorPredicate = { ...mockScanPredicate, customDisplayName: "Original" };
      addOperatorWithPosition(namedOperator);

      let change: { operatorID: string; newDisplayName: string } | undefined;
      const sub = texeraGraph.getOperatorDisplayNameChangedStream().subscribe(v => (change = v));

      (texeraGraph.getSharedOperatorType(namedOperator.operatorID).get("customDisplayName") as any).insert(0, "New-");

      expect(change).toBeDefined();
      expect(change!.operatorID).toEqual(namedOperator.operatorID);
      expect(change!.newDisplayName).toEqual("New-Original");
      sub.unsubscribe();
    });
  });

  describe("port events", () => {
    it("should emit portAddedOrDeletedSubject and add a joint port when an output port is added", () => {
      addOperatorWithPosition(mockScanPredicate); // has one existing output port

      let event: { newOperator: OperatorPredicate } | undefined;
      const sub = texeraGraph.getPortAddedOrDeletedStream().subscribe(v => (event = v));

      texeraGraph.addPort(mockScanPredicate.operatorID, { portID: "output-1" }, false);

      expect(event).toBeDefined();
      expect(event!.newOperator.operatorID).toEqual(mockScanPredicate.operatorID);
      expect(event!.newOperator.outputPorts.map(p => p.portID)).toEqual(["output-0", "output-1"]);
      const element = jointGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element;
      expect(element.getPorts().some(p => p.id === "output-1")).toBe(true);
      sub.unsubscribe();
    });

    it("should emit portAddedOrDeletedSubject and add a joint port when an input port is added", () => {
      addOperatorWithPosition(mockSentimentPredicate); // has one existing input port

      let event: { newOperator: OperatorPredicate } | undefined;
      const sub = texeraGraph.getPortAddedOrDeletedStream().subscribe(v => (event = v));

      texeraGraph.addPort(mockSentimentPredicate.operatorID, { portID: "input-1" }, true);

      expect(event).toBeDefined();
      expect(event!.newOperator.inputPorts.map(p => p.portID)).toEqual(["input-0", "input-1"]);
      const element = jointGraph.getCell(mockSentimentPredicate.operatorID) as joint.dia.Element;
      expect(element.getPorts().some(p => p.id === "input-1")).toBe(true);
      sub.unsubscribe();
    });

    it("should emit portAddedOrDeletedSubject and remove a joint port when input and output ports are removed", () => {
      addOperatorWithPosition(mockSentimentPredicate); // input-0, output-0
      texeraGraph.addPort(mockSentimentPredicate.operatorID, { portID: "input-1" }, true);

      const events: OperatorPredicate[] = [];
      const sub = texeraGraph.getPortAddedOrDeletedStream().subscribe(v => events.push(v.newOperator));

      // removing when >1 port yields a retain+delete delta; removing the last port yields a delete-at-0 delta
      texeraGraph.removePort(mockSentimentPredicate.operatorID, true); // remove input-1 (delta[1].delete)
      texeraGraph.removePort(mockSentimentPredicate.operatorID, false); // remove output-0 (delta[0].delete)

      expect(events.length).toEqual(2);
      const element = jointGraph.getCell(mockSentimentPredicate.operatorID) as joint.dia.Element;
      expect(element.getPorts().some(p => p.id === "input-1")).toBe(false);
      expect(element.getPorts().some(p => p.id === "output-0")).toBe(false);
      sub.unsubscribe();
    });

    it("should emit portDisplayNameChangedSubject when a port's display name text changes", () => {
      const operatorWithNamedPort: OperatorPredicate = {
        ...mockSentimentPredicate,
        inputPorts: [{ portID: "input-0", displayName: "orig" }],
      };
      addOperatorWithPosition(operatorWithNamedPort);

      let change: { operatorID: string; portID: string; newDisplayName: string } | undefined;
      const sub = texeraGraph.getPortDisplayNameChangedSubject().subscribe(v => (change = v));

      const portType = texeraGraph.getSharedPortDescriptionType({
        operatorID: operatorWithNamedPort.operatorID,
        portID: "input-0",
      });
      (portType!.get("displayName") as any).insert(0, "new-");

      expect(change).toBeDefined();
      expect(change!.operatorID).toEqual(operatorWithNamedPort.operatorID);
      expect(change!.portID).toEqual("input-0");
      expect(change!.newDisplayName).toEqual("new-orig");
      sub.unsubscribe();
    });

    it("should emit portPropertyChangedSubject when a port's partition/dependency properties change", () => {
      addOperatorWithPosition(mockSentimentPredicate);

      let change: any;
      const sub = texeraGraph.getPortPropertyChangedStream().subscribe(v => (change = v));

      texeraGraph.setPortProperty(
        { operatorID: mockSentimentPredicate.operatorID, portID: "input-0" },
        { partitionInfo: { type: "hash", hashAttributeNames: ["col"] }, dependencies: [{ id: 0, internal: false }] }
      );

      expect(change).toBeDefined();
      expect(change.operatorPortID).toEqual({ operatorID: mockSentimentPredicate.operatorID, portID: "input-0" });
      expect(change.newProperty.partitionInfo).toEqual({ type: "hash", hashAttributeNames: ["col"] });
      expect(change.newProperty.dependencies).toEqual([{ id: 0, internal: false }]);
      sub.unsubscribe();
    });

    it("reports a display-name change on an output port too", () => {
      // The input side is covered above; the handler picks the port list from an `isInput`
      // ternary, so the output side is a separate arm.
      const operatorWithNamedOutput: OperatorPredicate = {
        ...mockScanPredicate,
        outputPorts: [{ portID: "output-0", displayName: "orig" }],
      };
      addOperatorWithPosition(operatorWithNamedOutput);

      let change: { operatorID: string; portID: string; newDisplayName: string } | undefined;
      const sub = texeraGraph.getPortDisplayNameChangedSubject().subscribe(v => (change = v));

      const portType = texeraGraph.getSharedPortDescriptionType({
        operatorID: operatorWithNamedOutput.operatorID,
        portID: "output-0",
      });
      (portType!.get("displayName") as any).insert(0, "new-");

      expect(change).toBeDefined();
      expect(change!.portID).toEqual("output-0");
      expect(change!.newDisplayName).toEqual("new-orig");
      sub.unsubscribe();
    });

    it("reports a property change on an output port too", () => {
      addOperatorWithPosition(mockScanPredicate);

      let change: any;
      const sub = texeraGraph.getPortPropertyChangedStream().subscribe(v => (change = v));

      texeraGraph.setPortProperty(
        { operatorID: mockScanPredicate.operatorID, portID: "output-0" },
        { partitionInfo: { type: "none" }, dependencies: [] }
      );

      expect(change).toBeDefined();
      expect(change.operatorPortID.portID).toEqual("output-0");
      sub.unsubscribe();
    });

    it("stays quiet for a port property change that carries no partition or dependencies", () => {
      addOperatorWithPosition(mockSentimentPredicate);

      let emitted = false;
      const sub = texeraGraph.getPortPropertyChangedStream().subscribe(() => (emitted = true));

      // Only one half of the pair the handler requires before announcing a change.
      texeraGraph.setPortProperty(
        { operatorID: mockSentimentPredicate.operatorID, portID: "input-0" },
        { partitionInfo: { type: "hash", hashAttributeNames: ["col"] } }
      );

      expect(emitted).toBe(false);
      sub.unsubscribe();
    });

    it("removes nothing from the joint element when that side has no ports", () => {
      // mockScanPredicate has output ports only; the input side has nothing to strip, which
      // is the arm the port-removal helper guards against.
      addOperatorWithPosition(mockScanPredicate);
      const handler = (texeraGraph as any).sharedModelChangeHandler ?? (service as any).sharedModelChangeHandler;
      const element = jointGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element;
      const portsBefore = element.getPorts().length;

      (handler as any).onPortRemoved(mockScanPredicate.operatorID, true);

      expect(element.getPorts().length).toEqual(portsBefore);
    });

    it("refuses a port event that is neither a display name nor a property change", () => {
      addOperatorWithPosition(mockSentimentPredicate);
      const handler = (texeraGraph as any).sharedModelChangeHandler ?? (service as any).sharedModelChangeHandler;

      // A path of length 1 reaches neither the add/delete arm (length 2) nor either
      // property arm, which is the guard's whole purpose. Yjs cannot produce such an event
      // through the graph API, so it is handed to the handler directly.
      expect(() =>
        (handler as any).handlePortEvent(
          { path: ["ports"], delta: [], target: { toJSON: () => "" } },
          mockSentimentPredicate.operatorID,
          true
        )
      ).toThrow(/undefined port operation/);
    });
  });

  describe("deep comment box changes", () => {
    beforeEach(() => {
      texeraGraph.addCommentBox(mockCommentBox);
    });

    it("should emit commentBoxAddCommentSubject when a comment is added", () => {
      let event: { addedComment: Comment; commentBox: any } | undefined;
      const sub = texeraGraph.getCommentBoxAddCommentStream().subscribe(v => (event = v));

      const comment: Comment = { content: "hello", creationTime: "t1", creatorName: "alice", creatorID: 1 };
      texeraGraph.addCommentToCommentBox(comment, mockCommentBox.commentBoxID);

      expect(event).toBeDefined();
      expect(event!.addedComment.content).toEqual("hello");
      expect(event!.commentBox.commentBoxID).toEqual(mockCommentBox.commentBoxID);
      sub.unsubscribe();
    });

    it("should emit commentBoxDeleteCommentSubject when a comment is deleted", () => {
      const comment: Comment = { content: "hello", creationTime: "t1", creatorName: "alice", creatorID: 1 };
      texeraGraph.addCommentToCommentBox(comment, mockCommentBox.commentBoxID);

      let event: { commentBox: any } | undefined;
      const sub = texeraGraph.getCommentBoxDeleteCommentStream().subscribe(v => (event = v));

      texeraGraph.deleteCommentFromCommentBox(1, "t1", mockCommentBox.commentBoxID);

      expect(event).toBeDefined();
      expect(event!.commentBox.commentBoxID).toEqual(mockCommentBox.commentBoxID);
      sub.unsubscribe();
    });

    it("should emit commentBoxEditCommentSubject when a comment is edited in place", () => {
      const comment: Comment = { content: "hello", creationTime: "t1", creatorName: "alice", creatorID: 1 };
      texeraGraph.addCommentToCommentBox(comment, mockCommentBox.commentBoxID);

      let event: { commentBox: any } | undefined;
      const sub = texeraGraph.getCommentBoxEditCommentStream().subscribe(v => (event = v));

      texeraGraph.editCommentInCommentBox(1, "t1", mockCommentBox.commentBoxID, "edited");

      expect(event).toBeDefined();
      expect(event!.commentBox.commentBoxID).toEqual(mockCommentBox.commentBoxID);
      sub.unsubscribe();
    });
  });

  describe("new Y doc reload", () => {
    it("should re-register the observers on the freshly loaded shared model", () => {
      // Load a new model; the handler subscribes to newYDocLoadedSubject and must re-attach every observer.
      texeraGraph.loadNewYModel();

      let added: OperatorPredicate | undefined;
      const sub = texeraGraph.getOperatorAddStream().subscribe(op => (added = op));

      addOperatorWithPosition(mockScanPredicate);

      expect(added).toBeDefined();
      expect(added!.operatorID).toEqual(mockScanPredicate.operatorID);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      sub.unsubscribe();
    });
  });

  describe("async rendering path", () => {
    beforeEach(() => {
      (TestBed.inject(GuiConfigService) as any).setConfig({ asyncRenderingEnabled: true });
    });

    it("should batch-add operators onto the joint graph when async rendering is enabled", () => {
      const added: string[] = [];
      const sub = texeraGraph.getOperatorAddStream().subscribe(op => added.push(op.operatorID));

      texeraGraph.bundleActions(() => {
        texeraGraph.addOperator(mockScanPredicate);
        texeraGraph.sharedModel.elementPositionMap.set(mockScanPredicate.operatorID, mockPoint);
        texeraGraph.addOperator(mockResultPredicate);
        texeraGraph.sharedModel.elementPositionMap.set(mockResultPredicate.operatorID, mockPoint);
      });

      expect(added).toEqual([mockScanPredicate.operatorID, mockResultPredicate.operatorID]);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      expect(jointGraph.getCell(mockResultPredicate.operatorID)).toBeTruthy();
      sub.unsubscribe();
    });

    it("should batch-add links onto the joint graph when async rendering is enabled", () => {
      addOperatorWithPosition(mockScanPredicate);
      addOperatorWithPosition(mockResultPredicate);

      let addedLink: OperatorLink | undefined;
      const sub = texeraGraph.getLinkAddStream().subscribe(l => (addedLink = l));

      texeraGraph.addLink(mockScanResultLink);

      expect(addedLink).toEqual(mockScanResultLink);
      expect(jointGraph.getCell(mockScanResultLink.linkID)).toBeTruthy();
      sub.unsubscribe();
    });
  });
  // Every change so far originated locally. A change that arrives from a peer runs the same
  // observers with `transaction.local === false` — the arm that decides whether local awareness
  // and undo state are touched. A second Y.Doc stands in for the peer: mutating it and syncing
  // the diff back produces a genuinely remote transaction, which `yDoc.transact` cannot fake.
  describe("remote changes", () => {
    /** Runs `mutate` on a peer document and syncs the result in as a remote update. */
    function applyAsRemote(mutate: (peerDoc: Y.Doc) => void): void {
      const localDoc = texeraGraph.sharedModel.yDoc;
      const peerDoc = new Y.Doc();
      try {
        Y.applyUpdate(peerDoc, Y.encodeStateAsUpdate(localDoc));
        mutate(peerDoc);
        // Send back only what the local doc is missing, as a real peer would.
        Y.applyUpdate(localDoc, Y.encodeStateAsUpdate(peerDoc, Y.encodeStateVector(localDoc)));
      } finally {
        peerDoc.destroy();
      }
    }

    it("adds an operator that arrives from another client", () => {
      let added: OperatorPredicate | undefined;
      const sub = texeraGraph.getOperatorAddStream().subscribe(o => (added = o));

      applyAsRemote(peerDoc =>
        peerDoc.transact(() => {
          peerDoc.getMap("operatorIDMap").set(mockScanPredicate.operatorID, createYTypeFromObject(mockScanPredicate));
          peerDoc.getMap("elementPositionMap").set(mockScanPredicate.operatorID, mockPoint);
        })
      );

      expect(added?.operatorID).toBe(mockScanPredicate.operatorID);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeTruthy();
      sub.unsubscribe();
    });

    it("deletes an operator that another client removed", () => {
      addOperatorWithPosition(mockScanPredicate);
      const deleted: string[] = [];
      const sub = texeraGraph.getOperatorDeleteStream().subscribe(e => deleted.push(e.deletedOperatorID));

      applyAsRemote(peerDoc =>
        peerDoc.transact(() => {
          peerDoc.getMap("operatorIDMap").delete(mockScanPredicate.operatorID);
          peerDoc.getMap("elementPositionMap").delete(mockScanPredicate.operatorID);
        })
      );

      // The local-only unhighlight is skipped for a remote transaction; the removal itself is not.
      expect(deleted).toEqual([mockScanPredicate.operatorID]);
      expect(jointGraph.getCell(mockScanPredicate.operatorID)).toBeFalsy();
      sub.unsubscribe();
    });

    it("applies a remote property change without updating local awareness", () => {
      addOperatorWithPosition(mockScanPredicate);
      // The awareness update needs both a local transaction and this operator being the one
      // under edit. Setting the field directly on the awareness the handler reads leaves
      // `transaction.local` as the only thing that can keep the call away, so the assertion
      // below cannot pass vacuously. (updateSharedModelAwareness does not populate it here:
      // with no provider connected its local state stays empty.)
      texeraGraph.sharedModel.awareness.setLocalStateField("currentlyEditing", mockScanPredicate.operatorID);
      const awarenessSpy = vi.spyOn(texeraGraph, "updateSharedModelAwareness");
      let changed = false;
      const sub = texeraGraph.getOperatorPropertyChangeStream().subscribe(() => (changed = true));

      applyAsRemote(peerDoc =>
        peerDoc.transact(() => {
          const operator = peerDoc.getMap("operatorIDMap").get(mockScanPredicate.operatorID) as Y.Map<unknown>;
          (operator.get("operatorProperties") as Y.Map<unknown>).set("tableName", "from-peer");
        })
      );

      expect(changed).toBe(true);
      expect(texeraGraph.getOperator(mockScanPredicate.operatorID).operatorProperties).toEqual({
        tableName: "from-peer",
      });
      // the awareness update is the local-only arm of onOperatorPropertyChanged
      expect(awarenessSpy).not.toHaveBeenCalled();
      sub.unsubscribe();
    });
  });
});
