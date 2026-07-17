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
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";

import { OperatorMenuService } from "./operator-menu.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "../workflow-graph/model/mock-workflow-data";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { Subscription } from "rxjs";

describe("OperatorMenuService", () => {
  let service: OperatorMenuService;
  let workflowActionService: WorkflowActionService;
  let opsLatest: readonly string[] = [];
  let boxesLatest: readonly string[] = [];
  let subs: Subscription;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        // Stub the two collaborators OperatorMenuService only calls into (executeWorkflow,
        // error/info toasts) so the spec doesn't boot the real execution/notification stacks
        // (websocket heartbeats, NgZorro notification overlays).
        { provide: ExecuteWorkflowService, useValue: { executeWorkflow: vi.fn() } },
        {
          provide: NotificationService,
          useValue: { error: vi.fn(), info: vi.fn(), success: vi.fn(), warning: vi.fn() },
        },
        ...commonTestProviders,
      ],
      imports: [HttpClientTestingModule],
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    service = TestBed.inject(OperatorMenuService);

    subs = new Subscription();
    subs.add(service.highlightedOperators$.subscribe(ids => (opsLatest = ids)));
    subs.add(service.highlightedCommentBoxes$.subscribe(ids => (boxesLatest = ids)));
  });

  afterEach(() => subs.unsubscribe());

  // drains both the microtask queue (Promise .then/.catch) and the macrotask queue so the
  // clipboard-driven flows (writeText/readText promises) settle before assertions run.
  const flushAsync = () => new Promise<void>(resolve => setTimeout(resolve, 0));

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("starts with empty highlighted snapshots", () => {
    expect(opsLatest).toEqual([]);
    expect(boxesLatest).toEqual([]);
  });

  it("does not expose mutable BehaviorSubjects on the public API", () => {
    // service must not let outside code call .next() on its internal state.
    expect((service as any).highlightedOperators).toBeUndefined();
    expect((service as any).highlightedCommentBoxes).toBeUndefined();
  });

  it("emits the new highlighted operator IDs on highlightedOperators$", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

    expect(opsLatest).toEqual([mockScanPredicate.operatorID]);
  });

  it("emits the new highlighted comment box IDs on highlightedCommentBoxes$", () => {
    workflowActionService.addCommentBox(mockCommentBox);
    workflowActionService.getJointGraphWrapper().highlightCommentBoxes(mockCommentBox.commentBoxID);

    expect(boxesLatest).toEqual([mockCommentBox.commentBoxID]);
  });

  it("emits exactly once on highlightedOperators$ per highlight change (no fan-out)", () => {
    const emissions: string[][] = [];
    const sub = service.highlightedOperators$.subscribe(ids => emissions.push([...ids]));
    // BehaviorSubject seed
    expect(emissions.length).toBe(1);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

    // a single highlight event must produce a single emission, not 4 (one per dependent handler).
    expect(emissions.length).toBe(2);
    expect(emissions[1]).toEqual([mockScanPredicate.operatorID]);

    workflowActionService.getJointGraphWrapper().unhighlightOperators(mockScanPredicate.operatorID);
    expect(emissions.length).toBe(3);
    expect(emissions[2]).toEqual([]);

    sub.unsubscribe();
  });

  describe("button state recomputation", () => {
    it("makes disable button clickable when an operator is highlighted and modification is enabled", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

      expect(service.isDisableOperatorClickable).toBe(true);
      expect(service.isDisableOperator).toBe(true);
    });

    it("flips isDisableOperator to enable after the operator is disabled", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);
      workflowActionService.disableOperators([mockScanPredicate.operatorID]);

      // all highlighted operators are now disabled, so clicking should re-enable them.
      expect(service.isDisableOperator).toBe(false);
    });

    it("excludes sinks from view-result targets", () => {
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      const wrapper = workflowActionService.getJointGraphWrapper();
      // start from a clean highlight state — addOperator may auto-highlight new operators.
      wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());

      // highlighting only a sink: view-result should not be clickable.
      wrapper.highlightOperators(mockResultPredicate.operatorID);
      expect(service.isToViewResultClickable).toBe(false);
      expect(service.isReuseResultClickable).toBe(false);

      // highlighting only a non-sink: view-result becomes clickable.
      wrapper.unhighlightOperators(mockResultPredicate.operatorID);
      wrapper.highlightOperators(mockScanPredicate.operatorID);
      expect(service.isToViewResultClickable).toBe(true);
      expect(service.isReuseResultClickable).toBe(true);
    });

    it("recomputes when modification-enabled stream fires without a highlight change", () => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);
      expect(service.isDisableOperatorClickable).toBe(true);

      workflowActionService.disableWorkflowModification();
      expect(service.isDisableOperatorClickable).toBe(false);

      workflowActionService.enableWorkflowModification();
      expect(service.isDisableOperatorClickable).toBe(true);
    });

    it("recomputes when view-result state of a highlighted non-sink operator changes", () => {
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockSentimentPredicate, pos: mockPoint },
        ],
        []
      );
      workflowActionService
        .getJointGraphWrapper()
        .highlightOperators(mockScanPredicate.operatorID, mockSentimentPredicate.operatorID);

      expect(service.isToViewResult).toBe(true);

      workflowActionService.setViewOperatorResults([mockScanPredicate.operatorID, mockSentimentPredicate.operatorID]);
      // both highlighted non-sinks are now viewing results → next click should toggle off.
      expect(service.isToViewResult).toBe(false);
    });
  });

  describe("operator action callbacks", () => {
    beforeEach(() => {
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);
    });

    it("disables then re-enables the highlighted operators based on isDisableOperator", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      // fresh non-disabled operator → the button is in "disable" mode.
      expect(service.isDisableOperator).toBe(true);

      service.disableHighlightedOperators();
      expect(texeraGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBe(true);
      // recompute flips the toggle so the next click re-enables.
      expect(service.isDisableOperator).toBe(false);

      service.disableHighlightedOperators();
      expect(texeraGraph.isOperatorDisabled(mockScanPredicate.operatorID)).toBe(false);
    });

    it("sets then unsets view-result on the highlighted non-sink operators", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      expect(service.isToViewResult).toBe(true);

      service.viewResultHighlightedOperators();
      expect(texeraGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(true);
      expect(service.isToViewResult).toBe(false);

      service.viewResultHighlightedOperators();
      expect(texeraGraph.isViewingResult(mockScanPredicate.operatorID)).toBe(false);
    });

    it("marks then unmarks the highlighted non-sink operators for reuse", () => {
      const texeraGraph = workflowActionService.getTexeraGraph();
      expect(service.isMarkForReuse).toBe(true);

      service.reuseResultHighlightedOperator();
      expect(texeraGraph.isMarkedForReuseResult(mockScanPredicate.operatorID)).toBe(true);
      expect(service.isMarkForReuse).toBe(false);

      service.reuseResultHighlightedOperator();
      expect(texeraGraph.isMarkedForReuseResult(mockScanPredicate.operatorID)).toBe(false);
    });
  });

  describe("executeUpToOperator", () => {
    it("errors and does not execute when zero operators are highlighted", () => {
      const notificationService = TestBed.inject(NotificationService);
      const executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflow").mockImplementation(() => undefined);

      service.executeUpToOperator();

      expect(errorSpy).toHaveBeenCalledWith("Can only execute to exactly one target operator.");
      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("errors and does not execute when more than one operator is highlighted", () => {
      const notificationService = TestBed.inject(NotificationService);
      const executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflow").mockImplementation(() => undefined);
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockSentimentPredicate, pos: mockPoint },
        ],
        []
      );
      const wrapper = workflowActionService.getJointGraphWrapper();
      wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
      wrapper.setMultiSelectMode(true);
      wrapper.highlightOperators(mockScanPredicate.operatorID, mockSentimentPredicate.operatorID);

      service.executeUpToOperator();

      expect(errorSpy).toHaveBeenCalledWith("Can only execute to exactly one target operator.");
      expect(executeSpy).not.toHaveBeenCalled();
    });

    it("executes the workflow up to the single highlighted operator", () => {
      const executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
      const executeSpy = vi.spyOn(executeWorkflowService, "executeWorkflow").mockImplementation(() => undefined);
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

      service.executeUpToOperator();

      expect(executeSpy).toHaveBeenCalledWith("", mockScanPredicate.operatorID);
    });
  });

  describe("saveHighlightedElements", () => {
    let originalClipboard: PropertyDescriptor | undefined;
    let writeText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      originalClipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
      writeText = vi.fn().mockResolvedValue(undefined);
      Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    });

    afterEach(() => {
      if (originalClipboard) {
        Object.defineProperty(navigator, "clipboard", originalClipboard);
      } else {
        delete (navigator as any).clipboard;
      }
    });

    it("serializes the highlighted operators, links, and comment boxes to the clipboard", () => {
      // give the comment box a distinct ID: operators and comment boxes share the joint-graph
      // cell namespace, and mockCommentBox reuses operator ID "1".
      const commentBox = { ...mockCommentBox, commentBoxID: "comment-box-a" };
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockSentimentPredicate, pos: mockPoint },
        ],
        [mockScanSentimentLink]
      );
      workflowActionService.addCommentBox(commentBox);
      const wrapper = workflowActionService.getJointGraphWrapper();
      // start from a clean highlight state, then multi-select every element we want copied.
      wrapper.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
      wrapper.setMultiSelectMode(true);
      wrapper.highlightOperators(mockScanPredicate.operatorID, mockSentimentPredicate.operatorID);
      wrapper.highlightLinks(mockScanSentimentLink.linkID);
      wrapper.highlightCommentBoxes(commentBox.commentBoxID);

      service.saveHighlightedElements();

      expect(writeText).toHaveBeenCalledTimes(1);
      const serialized = JSON.parse(writeText.mock.calls[0][0]);
      const copiedIds = [mockScanPredicate.operatorID, mockSentimentPredicate.operatorID].sort();
      expect(serialized.operators.map((op: any) => op.operatorID).sort()).toEqual(copiedIds);
      expect(Object.keys(serialized.operatorPositions).sort()).toEqual(copiedIds);
      // the serialized position mirrors what the graph reports for that operator.
      expect(serialized.operatorPositions[mockScanPredicate.operatorID]).toEqual(
        wrapper.getElementPosition(mockScanPredicate.operatorID)
      );
      expect(serialized.links.map((link: any) => link.linkID)).toEqual([mockScanSentimentLink.linkID]);
      expect(serialized.commentBoxes.map((box: any) => box.commentBoxID)).toEqual([commentBox.commentBoxID]);
    });

    it("notifies the user when writing to the clipboard is rejected", async () => {
      writeText.mockRejectedValue(new Error("no permission"));
      const notificationService = TestBed.inject(NotificationService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

      service.saveHighlightedElements();
      await flushAsync();

      expect(errorSpy).toHaveBeenCalledWith("Copy failed. You don't have the permission to write to the clipboard.");
    });
  });

  describe("performPasteOperation", () => {
    let originalClipboard: PropertyDescriptor | undefined;
    let readText: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      originalClipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
      readText = vi.fn();
      Object.defineProperty(navigator, "clipboard", { value: { readText }, configurable: true });
    });

    afterEach(() => {
      if (originalClipboard) {
        Object.defineProperty(navigator, "clipboard", originalClipboard);
      } else {
        delete (navigator as any).clipboard;
      }
    });

    it("pastes operators, links, and comment boxes with fresh IDs and non-overlapping positions", async () => {
      // both operators share the same clipboard position, so pasting the second one must be
      // shifted by the getNonOverlappingPosition loop to avoid landing on the first.
      const clipboard = {
        operators: [mockScanPredicate, mockSentimentPredicate],
        operatorPositions: {
          [mockScanPredicate.operatorID]: { x: 100, y: 100 },
          [mockSentimentPredicate.operatorID]: { x: 100, y: 100 },
        },
        links: [mockScanSentimentLink],
        commentBoxes: [mockCommentBox],
      };
      readText.mockResolvedValue(JSON.stringify(clipboard));
      const texeraGraph = workflowActionService.getTexeraGraph();

      service.performPasteOperation();
      await flushAsync();

      const pastedOperators = texeraGraph.getAllOperators();
      expect(pastedOperators.length).toBe(2);
      // fresh IDs are assigned; none of the originals are reused.
      expect(pastedOperators.map(op => op.operatorID)).not.toContain(mockScanPredicate.operatorID);
      expect(pastedOperators.map(op => op.operatorID)).not.toContain(mockSentimentPredicate.operatorID);
      // both clipboard entries carried the same position, so getNonOverlappingPosition must have
      // shifted the second paste off the first: the two pasted elements end up at distinct spots.
      const wrapper = workflowActionService.getJointGraphWrapper();
      const [posA, posB] = pastedOperators.map(op => wrapper.getElementPosition(op.operatorID));
      expect(posA).not.toEqual(posB);
      expect(texeraGraph.getAllLinks().length).toBe(1);
      expect(texeraGraph.getAllCommentBoxes().length).toBe(1);
      // the pasted comment box is a distinct copy, not the original.
      expect(texeraGraph.getAllCommentBoxes()[0].commentBoxID).not.toBe(mockCommentBox.commentBoxID);
    });

    it("notifies the user when the clipboard holds no pasteable elements", async () => {
      readText.mockResolvedValue("{}");
      const notificationService = TestBed.inject(NotificationService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      service.performPasteOperation();
      await flushAsync();

      expect(errorSpy).toHaveBeenCalledWith("You haven't copied any element yet.");
      expect(workflowActionService.getTexeraGraph().getAllOperators().length).toBe(0);
    });

    it("notifies the user when reading the clipboard is rejected", async () => {
      readText.mockRejectedValue(new Error("blocked"));
      const notificationService = TestBed.inject(NotificationService);
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      service.performPasteOperation();
      await flushAsync();

      expect(errorSpy).toHaveBeenCalledWith("Paste failed. This site has been blocked from reading the clipboard.");
    });

    it("warns when adding the pasted operators and links fails", async () => {
      const clipboard = {
        operators: [mockScanPredicate],
        operatorPositions: { [mockScanPredicate.operatorID]: { x: 100, y: 100 } },
        links: [],
        commentBoxes: [],
      };
      readText.mockResolvedValue(JSON.stringify(clipboard));
      const notificationService = TestBed.inject(NotificationService);
      const infoSpy = vi.spyOn(notificationService, "info").mockImplementation(() => {});
      vi.spyOn(workflowActionService, "addOperatorsAndLinks").mockImplementation(() => {
        throw new Error("dangling link");
      });

      service.performPasteOperation();
      await flushAsync();

      expect(infoSpy).toHaveBeenCalledWith(
        "Some of the links that you selected don't have operators attached to both ends of them. " +
          "These links won't be pasted, since links can't exist without operators."
      );
    });
  });
});
