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

import { WorkflowActionService } from "./workflow-action.service";
import { UndoRedoService } from "../../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";
import {
  JointUIService,
  operatorCoeditorChangedPropertyClass,
  operatorCoeditorEditingClass,
} from "../../joint-ui/joint-ui.service";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { TestBed } from "@angular/core/testing";
import { marbles } from "rxjs-marbles";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "./mock-workflow-data";
import { Coeditor, Role } from "../../../../common/type/user";
import * as joint from "jointjs";
import { StubOperatorMetadataService } from "../../operator-metadata/stub-operator-metadata.service";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { map, share, tap } from "rxjs/operators";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../../common/service/gui-config.service.mock";

describe("JointGraphWrapperService", () => {
  let jointGraph: joint.dia.Graph;
  let jointGraphWrapper: JointGraphWrapper;
  let jointUIService: JointUIService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    });
    jointGraph = new joint.dia.Graph();
    jointGraphWrapper = new JointGraphWrapper(jointGraph);
    jointUIService = TestBed.inject(JointUIService);
  });

  it(
    "should emit operator delete event correctly when operator is deleted by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));

      m.hot("-e-")
        .pipe(tap(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const expectedStream = m.hot("-e-");

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should emit link add event correctly when a link is connected by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);

      m.hot("-e-")
        .pipe(tap(() => jointGraph.addCell(mockScanResultLinkCell)))
        .subscribe();

      const jointLinkAddStream = jointGraphWrapper.getJointLinkCellAddStream().pipe(map(() => "e"));
      const expectedStream = m.hot("-e-");

      m.expect(jointLinkAddStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should emit link delete event correctly when a link is deleted by JointJS",
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
      jointGraph.addCell(mockScanResultLinkCell);

      m.hot("---e-")
        .pipe(tap(() => jointGraph.getCell(mockScanResultLink.linkID).remove()))
        .subscribe();

      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));
      const expectedStream = m.hot("---e-");

      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);
    })
  );

  /**
   * When the user deletes an operator in the UI, jointJS will delete the connected links automatically.
   *
   * This test verifies that when an operator is deleted, causing the one connected link to be deleted,
   *   the JointJS event Observable streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(
    `should emit operator delete event and link delete event correctly
          when an operator along with one connected link are deleted by JointJS`,
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanResultLinkCell = JointUIService.getJointLinkCell(mockScanResultLink);
      jointGraph.addCell(mockScanResultLinkCell);

      m.hot("-e-")
        .pipe(tap(() => jointGraph.getCell(mockScanPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));

      const expectedStream = "-e-";

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedStream);
    })
  );

  /**
   *
   * This test verifies that when an operator is deleted, causing *multiple* connected links to be deleted,
   *   the JointJS event Observalbe streams are emitted correctly.
   * It should emit one operator delete event and one link delete event at the same time.
   */
  it(
    `should emit operator delete event and link delete event correctly when
        an operator along with multiple links are deleted by JointJS`,
    marbles(m => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockSentimentPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));

      const mockScanSentimentLinkCell = JointUIService.getJointLinkCell(mockScanSentimentLink);
      const mockSentimentResultLinkCell = JointUIService.getJointLinkCell(mockSentimentResultLink);
      jointGraph.addCell(mockScanSentimentLinkCell);
      jointGraph.addCell(mockSentimentResultLinkCell);

      m.hot("-e--")
        .pipe(tap(() => jointGraph.getCell(mockSentimentPredicate.operatorID).remove()))
        .subscribe();

      const jointOperatorDeleteStream = jointGraphWrapper.getJointElementCellDeleteStream().pipe(map(() => "e"));
      const jointLinkDeleteStream = jointGraphWrapper.getJointLinkCellDeleteStream().pipe(map(() => "e"));

      const expectedStream = "-e--";
      const expectedMultiStream = "-(ee)--";

      m.expect(jointOperatorDeleteStream).toBeObservable(expectedStream);
      m.expect(jointLinkDeleteStream).toBeObservable(expectedMultiStream);
    })
  );

  it(
    "should emit a highlight event correctly when an operator is highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add one operator, it should be automatically highlighted
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([
        mockScanPredicate.operatorID,
      ]);
      // unhighlight the current operator
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockScanPredicate.operatorID);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([]);

      // prepare marble operation for highlighting an operator
      const highlightActionMarbleEvent = m.hot("-a-|", { a: mockScanPredicate.operatorID }).pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operator is correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);
        },
      });
    })
  );

  it(
    "should emit a highlight event correctly when multiple operators are highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add two operators, they should be automatically highlighted
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([
        mockScanPredicate.operatorID,
        mockResultPredicate.operatorID,
      ]);

      // unhighlight current operators
      workflowActionService
        .getJointGraphWrapper()
        .unhighlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);
      expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual([]);

      // prepare marble operation for highlighting two operators
      const highlightActionMarbleEvent = m
        .hot("-a-|", {
          a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
        })
        .pipe(share());

      // highlight those operators at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(...value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operators are correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([
            mockScanPredicate.operatorID,
            mockResultPredicate.operatorID,
          ]);
        },
      });
    })
  );

  it(
    "should emit an unhighlight event correctly when an operator is unhighlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight an operator
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.getJointGraphWrapper().highlightOperators(mockScanPredicate.operatorID);

      // prepare marble operation for unhighlighting an operator
      const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

      // unhighlight that operator at events
      unhighlightActionMarbleEvent.subscribe(() =>
        localJointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID)
      );

      // prepare expected output unhighlight event stream
      const expectedUnhighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(
        expectedUnhighlightEventStream
      );

      // expect no operator is currently highlighted
      unhighlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
        },
      });
    })
  );

  it(
    "should emit an unhighlight event correctly when multiple operators are unhighlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight two operators
      workflowActionService.addOperatorsAndLinks(
        [
          { op: mockScanPredicate, pos: mockPoint },
          { op: mockResultPredicate, pos: mockPoint },
        ],
        []
      );
      workflowActionService
        .getJointGraphWrapper()
        .highlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);

      // prepare marble operation for unhighlighting two operators
      const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

      // unhighlight those operators at events
      unhighlightActionMarbleEvent.subscribe(() =>
        localJointGraphWrapper.unhighlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID)
      );

      // prepare expected output unhighlight event stream
      const expectedUnhighlightEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID, mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(
        expectedUnhighlightEventStream
      );

      // expect no operator is currently highlighted
      unhighlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
        },
      });
    })
  );

  it(
    "should unhighlight previous highlighted operator if a new operator is highlighted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      workflowActionService.addOperator(mockResultPredicate, mockPoint);

      // unhighlight the last operator in case of automatic highlight
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockResultPredicate.operatorID);

      // prepare marble operation for highlighting one operator, then highlight another
      const highlightActionMarbleEvent = m
        .hot("-a-b-|", {
          a: mockScanPredicate.operatorID,
          b: mockResultPredicate.operatorID,
        })
        .pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream
      const expectedHighlightEventStream = m.hot("-a-b-", {
        a: [mockScanPredicate.operatorID],
        b: [mockResultPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);

      // expect the current highlighted operator is correct
      highlightActionMarbleEvent.subscribe({
        complete: () => {
          expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockResultPredicate.operatorID]);
        },
      });
    })
  );

  it(
    "should ignore the action if trying to highlight the same currently highlighted operator",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add an operator, it should be automatically highlighted
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      // unhighlight it
      workflowActionService.getJointGraphWrapper().unhighlightOperators(mockScanPredicate.operatorID);

      // prepare marble operation for highlighting the same operator twice
      const highlightActionMarbleEvent = m
        .hot("-a-b-|", {
          a: mockScanPredicate.operatorID,
          b: mockScanPredicate.operatorID,
        })
        .pipe(share());

      // highlight that operator at events
      highlightActionMarbleEvent.subscribe(value => localJointGraphWrapper.highlightOperators(value));

      // prepare expected output highlight event stream: the second highlight is ignored
      const expectedHighlightEventStream = m.hot("-a---", {
        a: [mockScanPredicate.operatorID],
      });

      // expect the output event stream is correct
      m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(expectedHighlightEventStream);
    })
  );

  it(
    "should unhighlight the currently highlighted operator if it is deleted",
    marbles(m => {
      const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
      const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

      // add and highlight the operator
      workflowActionService.addOperator(mockScanPredicate, mockPoint);
      localJointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);

      expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

      // prepare the delete operator action marble test
      const deleteOperatorActionMarble = m.hot("-a-").pipe(share());
      deleteOperatorActionMarble.subscribe(() => workflowActionService.deleteOperator(mockScanPredicate.operatorID));

      // expect that the unhighlight event stream is triggered
      const expectedEventStream = m.hot("-a-", {
        a: [mockScanPredicate.operatorID],
      });
      m.expect(localJointGraphWrapper.getJointOperatorUnhighlightStream()).toBeObservable(expectedEventStream);

      // expect that the current highlighted operator is undefined
      deleteOperatorActionMarble.subscribe({
        complete: () => expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]),
      });
    })
  );

  it("should get operator position successfully if the operator exists in the paper", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(mockPoint);
  });

  it("should throw an error if operator does not exist in the paper when calling 'getElementPosition()'", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    expect(function () {
      localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID);
    }).toThrowError(`element with ID ${mockScanPredicate.operatorID} doesn't exist`);
  });

  it("should throw an error if the id we are using is linkID when calling 'getElementPosition()'", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    workflowActionService.addLink(mockScanResultLink);

    expect(function () {
      localJointGraphWrapper.getElementPosition(mockScanResultLink.linkID);
    }).toThrowError(`${mockScanResultLink.linkID} is not an element`);
  });

  it("should repositions the operator successfully if the operator exists in the paper", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    // changes the operator's position
    localJointGraphWrapper.setElementPosition(mockScanPredicate.operatorID, 10, 10);

    const expectedPosition = { x: mockPoint.x + 10, y: mockPoint.y + 10 };
    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(expectedPosition);
  });

  it("should successfully set a new zoom property", () => {
    const mockNewZoomProperty = 0.5;

    let currentZoomRatio = jointGraphWrapper.getZoomRatio();
    expect(currentZoomRatio).toEqual(1);

    jointGraphWrapper.setZoomProperty(mockNewZoomProperty);
    currentZoomRatio = jointGraphWrapper.getZoomRatio();
    expect(currentZoomRatio).toEqual(mockNewZoomProperty);
  });

  it(
    "should triggle getWorkflowEditorZoomStream when new zoom ratio is set",
    marbles(m => {
      const mockNewZoomProperty = 0.5;

      m.hot("-e-")
        .pipe(tap(event => jointGraphWrapper.setZoomProperty(mockNewZoomProperty)))
        .subscribe();
      const zoomStream = jointGraphWrapper.getWorkflowEditorZoomStream().pipe(map(value => "e"));
      const expectedStream = "-e-";

      m.expect(zoomStream).toBeObservable(expectedStream);
    })
  );

  it(
    "should trigger getRestorePaperOffsetStream when resumeDefaultZoomAndOffset is called",
    marbles(m => {
      m.hot("-e-")
        .pipe(tap(() => jointGraphWrapper.restoreDefaultZoomAndOffset()))
        .subscribe();
      const restoreStream = jointGraphWrapper.getRestorePaperOffsetStream().pipe(map(value => "e"));
      const expectedStream = "-e-";

      m.expect(restoreStream).toBeObservable(expectedStream);
    })
  );

  it("should move all highlighted operators together when any one of them is moved", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

    // add and highlight two operators
    workflowActionService.addOperatorsAndLinks(
      [
        { op: mockScanPredicate, pos: mockPoint },
        { op: mockResultPredicate, pos: mockPoint },
      ],
      []
    );
    localJointGraphWrapper.highlightOperators(...mockScanPredicate.operatorID, mockResultPredicate.operatorID);

    // change one operator's position
    localJointGraphWrapper.setElementPosition(mockScanPredicate.operatorID, 10, 10);

    const expectedPosition = { x: mockPoint.x + 10, y: mockPoint.y + 10 };

    // expect both operators to be in the new position
    expect(localJointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual(expectedPosition);
    expect(localJointGraphWrapper.getElementPosition(mockResultPredicate.operatorID)).toEqual(expectedPosition);
  });

  describe("when linkBreakpoint is enabled", () => {
    let mockConfigService: MockGuiConfigService;

    beforeEach(() => {
      // Get the mock service and enable linkBreakpoint for each test in this describe block
      mockConfigService = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      mockConfigService.setConfig({ linkBreakpointEnabled: true });
    });

    afterEach(() => {
      // Reset to default after each test
      if (mockConfigService) {
        mockConfigService.setConfig({ linkBreakpointEnabled: false });
      }
    });

    it(
      "should emit link highlight event correctly when a link is selected",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m.hot("-a-|", { a: mockScanResultLink.linkID }).pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          localJointGraphWrapper.highlightLink(value);
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);

        // expect the current highlighted link to be correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanResultLink.linkID]);
          },
        });
      })
    );

    it(
      "should emit an unhighlight event correctly when an link is unhighlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        // add one operator
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        // highlight the operator
        localJointGraphWrapper.highlightLink(mockScanResultLink.linkID);

        // prepare marble operation for unhighlighting an operator
        const unhighlightActionMarbleEvent = m.hot("-a-|").pipe(share());

        // unhighlight that operator at events
        unhighlightActionMarbleEvent.subscribe(() => localJointGraphWrapper.unhighlightLink(mockScanResultLink.linkID));

        // prepare expected output highlight event stream
        const expectedUnhighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedUnhighlightEventStream);

        // expect the current highlighted operator is correct
        unhighlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
          },
        });
      })
    );

    it(
      "should emit an unhighlight event correctly when an highlighted link is deleted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        // add one operator
        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);
        // highlight the operator
        localJointGraphWrapper.highlightLink(mockScanResultLink.linkID);

        // prepare marble operation for unhighlighting an operator
        const deleteActionMarbleEvent = m.hot("-a-|").pipe(share());

        // unhighlight that operator at events
        deleteActionMarbleEvent.subscribe(() => workflowActionService.deleteLinkWithID(mockScanResultLink.linkID));

        // prepare expected output highlight event stream
        const expectedUnhighlightEventStream = m.hot("-a-", {
          a: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedUnhighlightEventStream);

        // expect the current highlighted operator is correct
        deleteActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
          },
        });
      })
    );

    it(
      "should unhighlight previous highlighted link if another link is selected/highlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanSentimentLink);
        workflowActionService.addLink(mockSentimentResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m
          .hot("-a-b-|", {
            a: mockScanSentimentLink.linkID,
            b: mockSentimentResultLink.linkID,
          })
          .pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          localJointGraphWrapper.highlightLink(value);
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a-b-", {
          a: [mockScanSentimentLink.linkID],
          b: [mockSentimentResultLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);

        // prepare expected output unhighlight event stream
        const expectedLinUnhighlightEventStream = m.hot("---a-", {
          a: [mockScanSentimentLink.linkID],
        });
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedLinUnhighlightEventStream);

        // expect the current highlighted link to be correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockSentimentResultLink.linkID]);
          },
        });
      })
    );

    it(
      "should unhighlight previous highlighted links if an operator is highlighted",
      marbles(m => {
        const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
        const localJointGraphWrapper = workflowActionService.getJointGraphWrapper();

        workflowActionService.addOperator(mockScanPredicate, mockPoint);
        workflowActionService.addOperator(mockResultPredicate, mockPoint);
        workflowActionService.addLink(mockScanResultLink);

        // prepare marble operation for highlighting one link, then highlight an operator
        const highlightActionMarbleEvent = m
          .hot("-a-b-|", {
            a: mockScanResultLink.linkID,
            b: mockResultPredicate.operatorID,
          })
          .pipe(share());

        // highlight at events
        highlightActionMarbleEvent.subscribe(value => {
          if (value === mockResultPredicate.operatorID) {
            localJointGraphWrapper.highlightOperators(value);
          } else {
            localJointGraphWrapper.highlightLink(value);
          }
        });

        // prepare expected output highlight event stream
        const expectedLinkHighlightEventStream = m.hot("-a---", {
          a: [mockScanResultLink.linkID],
        });

        const expectedOperatorHighlightEventStream = m.hot("---b-", {
          b: [mockResultPredicate.operatorID],
        });

        // prepare expected output highlight event stream
        const expectedLinkUnhighlightEventStream = m.hot("---c-", {
          c: [mockScanResultLink.linkID],
        });

        // expect the output event stream is correct
        m.expect(localJointGraphWrapper.getLinkHighlightStream()).toBeObservable(expectedLinkHighlightEventStream);
        m.expect(localJointGraphWrapper.getJointOperatorHighlightStream()).toBeObservable(
          expectedOperatorHighlightEventStream
        );
        m.expect(localJointGraphWrapper.getLinkUnhighlightStream()).toBeObservable(expectedLinkUnhighlightEventStream);

        // expect the current highlighted operator is correct
        highlightActionMarbleEvent.subscribe({
          complete: () => {
            expect(localJointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockResultPredicate.operatorID]);
          },
        });
      })
    );
  });

  describe("regions displayed flag", () => {
    it("defaults to not displayed", () => {
      expect(jointGraphWrapper.getRegionsDisplayed()).toBe(false);
    });

    it("updates the value when set", () => {
      jointGraphWrapper.setRegionsDisplayed(true);
      expect(jointGraphWrapper.getRegionsDisplayed()).toBe(true);

      jointGraphWrapper.setRegionsDisplayed(false);
      expect(jointGraphWrapper.getRegionsDisplayed()).toBe(false);
    });

    it("emits the current value to new subscribers and on every change", () => {
      const emitted: boolean[] = [];
      jointGraphWrapper.getRegionsDisplayedStream().subscribe(displayed => emitted.push(displayed));

      jointGraphWrapper.setRegionsDisplayed(true);
      jointGraphWrapper.setRegionsDisplayed(false);

      // BehaviorSubject replays the initial false, then each subsequent change
      expect(emitted).toEqual([false, true, false]);
    });
  });

  describe("highlighting links, comment boxes and ports", () => {
    // highlightElement() throws unless the cell is actually in the graph, so seed
    // the cells first. Ports are tracked purely in the wrapper and need no cell.
    const addOperators = (): void => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));
    };
    const addLink = (): void => {
      jointGraph.addCell(JointUIService.getJointLinkCell(mockScanResultLink));
    };
    const addCommentBox = (): void => {
      jointGraph.addCell(jointUIService.getCommentElement(mockCommentBox));
    };
    const port = (operatorID: string, portID: string) => ({ operatorID, portID });

    it("highlightLinks and unhighlightLinks track the current link ids and emit", () => {
      addOperators();
      addLink();
      const highlighted: string[][] = [];
      const unhighlighted: string[][] = [];
      const onHighlight = jointGraphWrapper.getLinkHighlightStream().subscribe(ids => highlighted.push([...ids]));
      const onUnhighlight = jointGraphWrapper.getLinkUnhighlightStream().subscribe(ids => unhighlighted.push([...ids]));

      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
      expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanResultLink.linkID]);
      expect(highlighted).toEqual([[mockScanResultLink.linkID]]);

      // re-highlighting an already-highlighted link is a no-op (no second emission)
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
      expect(highlighted).toHaveLength(1);

      jointGraphWrapper.unhighlightLinks(mockScanResultLink.linkID);
      expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
      expect(unhighlighted).toEqual([[mockScanResultLink.linkID]]);

      onHighlight.unsubscribe();
      onUnhighlight.unsubscribe();
    });

    it("highlightCommentBoxes and unhighlightCommentBoxes track the ids and emit", () => {
      addCommentBox();
      const highlighted: string[][] = [];
      const unhighlighted: string[][] = [];
      const onHighlight = jointGraphWrapper
        .getJointCommentBoxHighlightStream()
        .subscribe(ids => highlighted.push([...ids]));
      const onUnhighlight = jointGraphWrapper
        .getJointCommentBoxUnhighlightStream()
        .subscribe(ids => unhighlighted.push([...ids]));

      jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);
      expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([mockCommentBox.commentBoxID]);
      expect(highlighted).toEqual([[mockCommentBox.commentBoxID]]);

      jointGraphWrapper.unhighlightCommentBoxes(mockCommentBox.commentBoxID);
      expect(jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()).toEqual([]);
      expect(unhighlighted).toEqual([[mockCommentBox.commentBoxID]]);

      onHighlight.unsubscribe();
      onUnhighlight.unsubscribe();
    });

    it("highlightPorts and unhighlightPorts track the current ports and emit", () => {
      const highlighted: unknown[][] = [];
      const unhighlighted: unknown[][] = [];
      const onHighlight = jointGraphWrapper.getJointPortHighlightStream().subscribe(ids => highlighted.push([...ids]));
      const onUnhighlight = jointGraphWrapper
        .getJointPortUnhighlightStream()
        .subscribe(ids => unhighlighted.push([...ids]));

      const portA = port(mockScanPredicate.operatorID, "output-0");
      jointGraphWrapper.highlightPorts(portA);
      expect(jointGraphWrapper.getCurrentHighlightedPortIDs()).toEqual([portA]);
      expect(highlighted).toEqual([[portA]]);
      // With multi-select off, highlightPorts clears the previous ports first. The
      // port streams emit unconditionally (unlike the operator/link ones), so that
      // pre-clear surfaces as an empty batch even though nothing was highlighted.
      expect(unhighlighted).toEqual([[]]);

      jointGraphWrapper.unhighlightPorts(portA);
      expect(jointGraphWrapper.getCurrentHighlightedPortIDs()).toEqual([]);
      expect(unhighlighted).toEqual([[], [portA]]);

      onHighlight.unsubscribe();
      onUnhighlight.unsubscribe();
    });

    it("single-select mode (the default) unhighlights the previous element", () => {
      addOperators();
      addLink();

      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

      // highlighting a link with multi-select off drops the operator highlight
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
      expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanResultLink.linkID]);
    });

    it("multi-select mode keeps previously highlighted elements", () => {
      addOperators();
      addLink();
      jointGraphWrapper.setMultiSelectMode(true);

      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);

      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);
      expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([mockScanResultLink.linkID]);
    });

    it("getCurrentHighlights and getCurrentHighlightedIDs aggregate every family", () => {
      addOperators();
      addLink();
      addCommentBox();
      jointGraphWrapper.setMultiSelectMode(true);

      const portA = port(mockScanPredicate.operatorID, "output-0");
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
      jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);
      jointGraphWrapper.highlightPorts(portA);

      expect(jointGraphWrapper.getCurrentHighlights()).toEqual({
        operators: [mockScanPredicate.operatorID],
        links: [mockScanResultLink.linkID],
        commentBoxes: [mockCommentBox.commentBoxID],
        ports: [portA],
      });
      // getCurrentHighlightedIDs concatenates the id-based families only (ports excluded)
      expect(jointGraphWrapper.getCurrentHighlightedIDs()).toEqual([
        mockScanPredicate.operatorID,
        mockScanResultLink.linkID,
        mockCommentBox.commentBoxID,
      ]);
    });

    it("unhighlightElements clears operators, links, comment boxes and ports together", () => {
      addOperators();
      addLink();
      addCommentBox();
      jointGraphWrapper.setMultiSelectMode(true);

      const portA = port(mockScanPredicate.operatorID, "output-0");
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);
      jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);
      jointGraphWrapper.highlightPorts(portA);

      // copy the lists so the call does not iterate the arrays it is mutating
      jointGraphWrapper.unhighlightElements({
        operators: [...jointGraphWrapper.getCurrentHighlightedOperatorIDs()],
        links: [...jointGraphWrapper.getCurrentHighlightedLinkIDs()],
        commentBoxes: [...jointGraphWrapper.getCurrentHighlightedCommentBoxIDs()],
        ports: [...jointGraphWrapper.getCurrentHighlightedPortIDs()],
      });

      expect(jointGraphWrapper.getCurrentHighlights()).toEqual({
        operators: [],
        links: [],
        commentBoxes: [],
        ports: [],
      });
    });

    it("exposes the group highlight streams, which operator highlighting does not touch", () => {
      addOperators();
      const emitted: unknown[] = [];
      const onHighlight = jointGraphWrapper.getJointGroupHighlightStream().subscribe(ids => emitted.push(ids));
      const onUnhighlight = jointGraphWrapper.getJointGroupUnhighlightStream().subscribe(ids => emitted.push(ids));

      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);

      expect(emitted).toEqual([]);
      onHighlight.unsubscribe();
      onUnhighlight.unsubscribe();
    });
  });

  describe("workflow flags", () => {
    it("setReloadingWorkflow round-trips through getReloadingWorkflow", () => {
      expect(jointGraphWrapper.getReloadingWorkflow()).toBe(false);

      jointGraphWrapper.setReloadingWorkflow(true);
      expect(jointGraphWrapper.getReloadingWorkflow()).toBe(true);

      jointGraphWrapper.setReloadingWorkflow(false);
      expect(jointGraphWrapper.getReloadingWorkflow()).toBe(false);
    });

    it("setListenPositionChange round-trips through getListenPositionChange", () => {
      expect(jointGraphWrapper.getListenPositionChange()).toBe(true);

      jointGraphWrapper.setListenPositionChange(false);
      expect(jointGraphWrapper.getListenPositionChange()).toBe(false);

      jointGraphWrapper.setListenPositionChange(true);
      expect(jointGraphWrapper.getListenPositionChange()).toBe(true);
    });
  });

  describe("zoom ratio bounds", () => {
    it("setZoomProperty updates the ratio and emits it on the zoom stream", () => {
      const emitted: number[] = [];
      const subscription = jointGraphWrapper.getWorkflowEditorZoomStream().subscribe(ratio => emitted.push(ratio));

      jointGraphWrapper.setZoomProperty(1.2);

      expect(jointGraphWrapper.getZoomRatio()).toBe(1.2);
      expect(emitted).toEqual([1.2]);
      subscription.unsubscribe();
    });

    it("isZoomRatioMin and isZoomRatioMax report the configured bounds", () => {
      // the wrapper itself does not clamp; it only reports where the ratio sits
      jointGraphWrapper.setZoomProperty(JointGraphWrapper.ZOOM_MINIMUM);
      expect(jointGraphWrapper.isZoomRatioMin()).toBe(true);
      expect(jointGraphWrapper.isZoomRatioMax()).toBe(false);

      jointGraphWrapper.setZoomProperty(JointGraphWrapper.ZOOM_MAXIMUM);
      expect(jointGraphWrapper.isZoomRatioMax()).toBe(true);
      expect(jointGraphWrapper.isZoomRatioMin()).toBe(false);

      jointGraphWrapper.setZoomProperty(JointGraphWrapper.INIT_ZOOM_VALUE);
      expect(jointGraphWrapper.isZoomRatioMin()).toBe(false);
      expect(jointGraphWrapper.isZoomRatioMax()).toBe(false);
    });

    it("restoreDefaultZoomAndOffset resets the ratio and signals a paper-offset restore", () => {
      const restores: number[] = [];
      const subscription = jointGraphWrapper.getRestorePaperOffsetStream().subscribe(() => restores.push(1));
      jointGraphWrapper.setZoomProperty(1.25);

      jointGraphWrapper.restoreDefaultZoomAndOffset();

      expect(jointGraphWrapper.getZoomRatio()).toBe(JointGraphWrapper.INIT_ZOOM_VALUE);
      expect(restores).toHaveLength(1);
      subscription.unsubscribe();
    });
  });

  describe("cell layer, breakpoints and position changes", () => {
    it("getCellLayer returns the cell's z index and throws for an unknown cell", () => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));

      expect(jointGraphWrapper.getCellLayer(mockScanPredicate.operatorID)).toBeGreaterThanOrEqual(0);
      expect(() => jointGraphWrapper.getCellLayer("no-such-cell")).toThrowError(
        "cell with ID no-such-cell doesn't exist"
      );
    });

    it("getLinkIDsWithBreakpoint starts empty", () => {
      expect(jointGraphWrapper.getLinkIDsWithBreakpoint()).toEqual([]);
    });

    it("getElementPositionChangeEvent reports the old and new position of a moved element", () => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));

      const moves: { elementID: string; newPosition: { x: number; y: number } }[] = [];
      const subscription = jointGraphWrapper
        .getElementPositionChangeEvent()
        .subscribe(event => moves.push({ elementID: event.elementID, newPosition: event.newPosition }));

      (jointGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element).position(100, 200);

      expect(moves).toEqual([{ elementID: mockScanPredicate.operatorID, newPosition: { x: 100, y: 200 } }]);
      subscription.unsubscribe();
    });
  });

  describe("coeditor presence without an attached paper", () => {
    // Every coeditor method reaches the canvas through `getMainJointPaper()?.`,
    // so with no paper attached they must degrade to a safe no-op.
    const coeditor: Coeditor = {
      clientId: "client-1",
      uid: 1,
      name: "Alice",
      email: "alice@x.io",
      role: Role.REGULAR,
      comment: "",
      joiningReason: "",
      color: "#ff0000",
    };

    it("add/deleteCoeditorOperatorHighlight are no-ops when no paper is attached", () => {
      expect(() =>
        jointGraphWrapper.addCoeditorOperatorHighlight(coeditor, mockScanPredicate.operatorID)
      ).not.toThrow();
      expect(() =>
        jointGraphWrapper.deleteCoeditorOperatorHighlight(coeditor, mockScanPredicate.operatorID)
      ).not.toThrow();
    });

    it("setCurrentEditing returns an interval that removeCurrentEditing clears", () => {
      // Deliberately NOT using fake timers: zone.js patches setInterval/clearInterval
      // too, and driving vitest's fake clock through that patched pair behaves
      // differently across Node versions. This body is fully synchronous, so the
      // 300ms animation callback can never be reached before it is cleared.
      const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");
      const intervalId = jointGraphWrapper.setCurrentEditing(coeditor, mockScanPredicate.operatorID);
      try {
        expect(intervalId).toBeDefined();

        jointGraphWrapper.removeCurrentEditing(coeditor, mockScanPredicate.operatorID, intervalId);
        expect(clearIntervalSpy).toHaveBeenCalledWith(intervalId);
      } finally {
        clearInterval(intervalId);
        clearIntervalSpy.mockRestore();
      }
    });

    it("set/removePropertyChanged are no-ops when no paper is attached", () => {
      expect(() => jointGraphWrapper.setPropertyChanged(coeditor, mockScanPredicate.operatorID)).not.toThrow();
      expect(() => jointGraphWrapper.removePropertyChanged(coeditor, mockScanPredicate.operatorID)).not.toThrow();
    });
  });

  describe("guard clauses for missing, wrong-typed and unhighlighted cells", () => {
    const addScanAndResult = (): void => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));
    };
    const addScanResultLink = (): void => {
      jointGraph.addCell(JointUIService.getJointLinkCell(mockScanResultLink));
    };

    it("setElementPosition rejects an unknown id and a link id", () => {
      addScanAndResult();
      addScanResultLink();

      expect(() => jointGraphWrapper.setElementPosition("no-such-cell", 10, 10)).toThrowError(
        "element with ID no-such-cell doesn't exist"
      );
      expect(() => jointGraphWrapper.setElementPosition(mockScanResultLink.linkID, 10, 10)).toThrowError(
        `${mockScanResultLink.linkID} is not an element`
      );
    });

    it("setAbsolutePosition moves an element and rejects an unknown id and a link id", () => {
      addScanAndResult();
      addScanResultLink();

      jointGraphWrapper.setAbsolutePosition(mockScanPredicate.operatorID, 42, 43);
      expect(jointGraphWrapper.getElementPosition(mockScanPredicate.operatorID)).toEqual({ x: 42, y: 43 });

      expect(() => jointGraphWrapper.setAbsolutePosition("no-such-cell", 1, 1)).toThrowError(
        "element with ID no-such-cell doesn't exist"
      );
      expect(() => jointGraphWrapper.setAbsolutePosition(mockScanResultLink.linkID, 1, 1)).toThrowError(
        `${mockScanResultLink.linkID} is not an element`
      );
    });

    it("highlightOperators rejects an id that is not in the graph", () => {
      expect(() => jointGraphWrapper.highlightOperators("no-such-operator")).toThrowError(
        "element with ID no-such-operator doesn't exist"
      );
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
    });

    it("highlightLink rejects an unknown id and ignores an already highlighted link", () => {
      addScanAndResult();
      addScanResultLink();
      const highlighted: string[][] = [];
      const subscription = jointGraphWrapper.getLinkHighlightStream().subscribe(ids => highlighted.push([...ids]));

      expect(() => jointGraphWrapper.highlightLink("no-such-link")).toThrowError(
        "link with ID no-such-link doesn't exist"
      );

      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      jointGraphWrapper.highlightLink(mockScanResultLink.linkID);
      expect(highlighted).toEqual([[mockScanResultLink.linkID]]);

      subscription.unsubscribe();
    });

    it("unhighlightLink and unhighlightOperators stay silent for cells that are not highlighted", () => {
      addScanAndResult();
      addScanResultLink();
      const unhighlightedLinks: string[][] = [];
      const unhighlightedOperators: string[][] = [];
      const onLink = jointGraphWrapper.getLinkUnhighlightStream().subscribe(ids => unhighlightedLinks.push([...ids]));
      const onOperator = jointGraphWrapper
        .getJointOperatorUnhighlightStream()
        .subscribe(ids => unhighlightedOperators.push([...ids]));

      jointGraphWrapper.unhighlightLink(mockScanResultLink.linkID);
      jointGraphWrapper.unhighlightOperators(mockScanPredicate.operatorID);

      expect(unhighlightedLinks).toEqual([]);
      expect(unhighlightedOperators).toEqual([]);

      onLink.unsubscribe();
      onOperator.unsubscribe();
    });

    it("highlightCommentBoxes stays silent when every requested box is already highlighted", () => {
      jointGraph.addCell(jointUIService.getCommentElement(mockCommentBox));
      const highlighted: string[][] = [];
      const subscription = jointGraphWrapper
        .getJointCommentBoxHighlightStream()
        .subscribe(ids => highlighted.push([...ids]));

      jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);
      jointGraphWrapper.highlightCommentBoxes(mockCommentBox.commentBoxID);

      expect(highlighted).toEqual([[mockCommentBox.commentBoxID]]);
      subscription.unsubscribe();
    });

    it("getCellLayer reports the layer JointUIService assigned", () => {
      addScanAndResult();
      addScanResultLink();

      // JointUIService assigns z=1 to operators and z=0 to links. The `|| 0` fallback in
      // getCellLayer is unreachable and is deliberately not pinned: joint's Graph.addCell sets
      // `z = maxZIndex() + 1` on any cell that arrives without one, and getCellLayer throws for a
      // cell that is not in the graph — so `attributes.z` is never undefined by the time it reads
      // it. The link's zero is an explicitly assigned zero, not the fallback.
      expect(jointGraphWrapper.getCellLayer(mockScanPredicate.operatorID)).toBe(1);
      expect(jointGraphWrapper.getCellLayer(mockScanResultLink.linkID)).toBe(0);
    });

    it("removing a highlighted cell straight from the graph unhighlights it", () => {
      addScanAndResult();
      addScanResultLink();
      jointGraphWrapper.setMultiSelectMode(true);
      jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
      jointGraphWrapper.highlightLinks(mockScanResultLink.linkID);

      jointGraph.getCell(mockScanResultLink.linkID).remove();
      expect(jointGraphWrapper.getCurrentHighlightedLinkIDs()).toEqual([]);
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([mockScanPredicate.operatorID]);

      jointGraph.getCell(mockScanPredicate.operatorID).remove();
      expect(jointGraphWrapper.getCurrentHighlightedOperatorIDs()).toEqual([]);
    });

    it("getElementPositionChangeEvent carries the previous position across consecutive moves", () => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      const moves: { oldPosition: { x: number; y: number }; newPosition: { x: number; y: number } }[] = [];
      const subscription = jointGraphWrapper
        .getElementPositionChangeEvent()
        .subscribe(event => moves.push({ oldPosition: event.oldPosition, newPosition: event.newPosition }));

      const element = jointGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element;
      element.position(100, 200);
      element.position(150, 200); // only x moves
      element.position(150, 250); // only y moves

      expect(moves).toEqual([
        { oldPosition: mockPoint, newPosition: { x: 100, y: 200 } },
        { oldPosition: { x: 100, y: 200 }, newPosition: { x: 150, y: 200 } },
        { oldPosition: { x: 150, y: 200 }, newPosition: { x: 150, y: 250 } },
      ]);
      subscription.unsubscribe();
    });

    it("getElementPositionChangeEvent errors for an element the wrapper never saw being added", () => {
      // the position map is only filled from the cell-add stream, so a cell that predates
      // the wrapper has no recorded position and the stream must fail loudly instead of
      // reporting a bogus old position.
      const preexistingGraph = new joint.dia.Graph();
      preexistingGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      const lateWrapper = new JointGraphWrapper(preexistingGraph);

      let error: unknown;
      const subscription = lateWrapper
        .getElementPositionChangeEvent()
        .subscribe({ error: (e: unknown) => (error = e) });
      (preexistingGraph.getCell(mockScanPredicate.operatorID) as joint.dia.Element).position(1, 2);

      expect(error).toEqual(
        new Error(`internal error: cannot find element position for ${mockScanPredicate.operatorID}`)
      );
      subscription.unsubscribe();
    });
  });

  describe("link endpoint changes", () => {
    it("getJointLinkCellChangeStream emits the link whose endpoint was re-attached", () => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockSentimentPredicate, mockPoint));
      jointGraph.addCell(JointUIService.getJointLinkCell(mockScanResultLink));

      const changed: string[] = [];
      const subscription = jointGraphWrapper
        .getJointLinkCellChangeStream()
        .subscribe(link => changed.push(link.id.toString()));

      const link = jointGraph.getCell(mockScanResultLink.linkID) as joint.dia.Link;
      link.set("target", { id: mockSentimentPredicate.operatorID, port: "input-0" });
      link.set("source", { x: 10, y: 20 }); // detaching an end is a change too

      expect(changed).toEqual([mockScanResultLink.linkID, mockScanResultLink.linkID]);
      subscription.unsubscribe();
    });
  });

  describe("auto layout", () => {
    it("lays operators out left to right and leaves region elements where they are", () => {
      // regions are decorations drawn around operators; feeding them to dagre would move
      // them independently of the operators they wrap.
      const Region = joint.dia.Element.define("region", {}, { markup: [{ tagName: "path", selector: "body" }] });
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));
      jointGraph.addCell(JointUIService.getJointLinkCell(mockScanResultLink));
      const region = new Region({ position: { x: 500, y: 500 }, size: { width: 10, height: 10 } });
      jointGraph.addCell(region);

      jointGraphWrapper.autoLayoutJoint();

      const scan = jointGraphWrapper.getElementPosition(mockScanPredicate.operatorID);
      const result = jointGraphWrapper.getElementPosition(mockResultPredicate.operatorID);
      expect(scan.x).toBeLessThan(result.x);
      expect(region.position()).toEqual({ x: 500, y: 500 });
    });
  });

  describe("with a joint paper attached", () => {
    const alice: Coeditor = {
      clientId: "client-alice",
      uid: 1,
      name: "Alice",
      email: "alice@x.io",
      role: Role.REGULAR,
      comment: "",
      joiningReason: "",
      color: "#ff0000",
    };
    const bob: Coeditor = { ...alice, clientId: "client-bob", name: "Bob", color: "#00ff00" };

    let paper: joint.dia.Paper;
    let paperHost: HTMLDivElement;

    beforeEach(() => {
      paperHost = document.createElement("div");
      document.body.appendChild(paperHost);
      paper = jointGraphWrapper.attachMainJointPaper({ el: paperHost, width: 600, height: 400 });
    });

    afterEach(() => {
      paper.remove();
      paperHost.remove();
    });

    const addScanOperator = (): void => {
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
    };
    const editingBanner = (): { text?: string; fill?: string; visibility?: string } =>
      (
        paper.getModelById(mockScanPredicate.operatorID).attributes.attrs as Record<
          string,
          { text?: string; fill?: string; visibility?: string }
        >
      )[`.${operatorCoeditorEditingClass}`];

    it("attachMainJointPaper binds the wrapper's own graph and publishes the paper", () => {
      expect(paper.model).toBe(jointGraph);
      expect(jointGraphWrapper.getMainJointPaper()).toBe(paper);

      // the stream is a ReplaySubject, so a subscriber that arrives after the attach still gets it
      const attached: joint.dia.Paper[] = [];
      const subscription = jointGraphWrapper.getMainJointPaperAttachedStream().subscribe(p => attached.push(p));
      expect(attached).toHaveLength(1);
      expect(attached[0]).toBe(paper);
      subscription.unsubscribe();
    });

    it("an async context defers highlight events and switches the paper to async rendering", () => {
      addScanOperator();
      jointGraph.addCell(jointUIService.getJointOperatorElement(mockResultPredicate, mockPoint));
      jointGraphWrapper.setMultiSelectMode(true);
      const highlighted: string[][] = [];
      const subscription = jointGraphWrapper
        .getJointOperatorHighlightStream()
        .subscribe(ids => highlighted.push([...ids]));
      const updateViews = vi.spyOn(paper, "updateViews");

      expect(paper.options.async).toBe(false);
      jointGraphWrapper.jointGraphContext.withContext({ async: true }, () => {
        expect(paper.options.async).toBe(true);
        jointGraphWrapper.highlightOperators(mockScanPredicate.operatorID);
        expect(highlighted).toEqual([]);
      });

      // leaving the context restores synchronous rendering and forces the deferred views out
      expect(paper.options.async).toBe(false);
      expect(updateViews).toHaveBeenCalled();

      // the buffered event is only drained by the next event emitted outside the context
      jointGraphWrapper.highlightOperators(mockResultPredicate.operatorID);
      expect(highlighted).toEqual([[mockScanPredicate.operatorID], [mockResultPredicate.operatorID]]);

      // a synchronous context never deferred anything, so leaving it must not force a repaint
      updateViews.mockClear();
      jointGraphWrapper.jointGraphContext.withContext({ async: false }, () => {
        expect(paper.options.async).toBe(false);
      });
      expect(updateViews).not.toHaveBeenCalled();

      updateViews.mockRestore();
      subscription.unsubscribe();
    });

    it("an async context still buffers when no paper was ever attached", () => {
      const bareGraph = new joint.dia.Graph();
      const bareWrapper = new JointGraphWrapper(bareGraph);
      bareGraph.addCell(jointUIService.getJointOperatorElement(mockScanPredicate, mockPoint));
      const highlighted: string[][] = [];
      const subscription = bareWrapper.getJointOperatorHighlightStream().subscribe(ids => highlighted.push([...ids]));

      expect(() =>
        bareWrapper.jointGraphContext.withContext({ async: true }, () =>
          bareWrapper.highlightOperators(mockScanPredicate.operatorID)
        )
      ).not.toThrow();
      expect(highlighted).toEqual([]);

      subscription.unsubscribe();
    });

    it("addCoeditorOperatorHighlight stacks one ring per coeditor and ignores duplicates", () => {
      addScanOperator();
      const view = paper.findViewByModel(mockScanPredicate.operatorID);
      const rings = () =>
        joint.highlighters.mask.get(view).map(stroke => ({ id: stroke.id, padding: stroke.options.padding }));

      jointGraphWrapper.addCoeditorOperatorHighlight(alice, mockScanPredicate.operatorID);
      jointGraphWrapper.addCoeditorOperatorHighlight(alice, mockScanPredicate.operatorID);
      jointGraphWrapper.addCoeditorOperatorHighlight(bob, mockScanPredicate.operatorID);

      // each additional coeditor gets a wider ring so all of them stay visible
      expect(rings()).toEqual([
        { id: `coeditorHighlight_${alice.clientId}_${mockScanPredicate.operatorID}`, padding: 5 },
        { id: `coeditorHighlight_${bob.clientId}_${mockScanPredicate.operatorID}`, padding: 10 },
      ]);
    });

    it("deleteCoeditorOperatorHighlight removes one ring and re-pads the ones behind it", () => {
      addScanOperator();
      const view = paper.findViewByModel(mockScanPredicate.operatorID);
      const rings = () =>
        joint.highlighters.mask.get(view).map(stroke => ({ id: stroke.id, padding: stroke.options.padding }));
      jointGraphWrapper.addCoeditorOperatorHighlight(alice, mockScanPredicate.operatorID);
      jointGraphWrapper.addCoeditorOperatorHighlight(bob, mockScanPredicate.operatorID);

      // deleting a coeditor that never highlighted this operator must leave the existing
      // rings alone: the re-pad loop below removes and re-adds rings, which reorders them.
      jointGraphWrapper.deleteCoeditorOperatorHighlight(
        { ...alice, clientId: "client-carol" },
        mockScanPredicate.operatorID
      );
      expect(rings()).toEqual([
        { id: `coeditorHighlight_${alice.clientId}_${mockScanPredicate.operatorID}`, padding: 5 },
        { id: `coeditorHighlight_${bob.clientId}_${mockScanPredicate.operatorID}`, padding: 10 },
      ]);

      jointGraphWrapper.deleteCoeditorOperatorHighlight(alice, mockScanPredicate.operatorID);

      // Bob's ring moves inwards to close the gap Alice left behind
      expect(rings()).toEqual([
        { id: `coeditorHighlight_${bob.clientId}_${mockScanPredicate.operatorID}`, padding: 5 },
      ]);
    });

    it("setCurrentEditing writes the coeditor banner and animates its trailing dots", () => {
      addScanOperator();
      vi.useFakeTimers();
      const intervalId = jointGraphWrapper.setCurrentEditing(alice, mockScanPredicate.operatorID);
      try {
        expect(editingBanner()).toMatchObject({
          text: "Alice is viewing/editing...",
          fill: alice.color,
          visibility: "visible",
        });

        // the trailing dots cycle ... -> . -> .. -> ... on every tick
        vi.advanceTimersByTime(300);
        expect(editingBanner().text).toBe("Alice is viewing/editing.");
        vi.advanceTimersByTime(300);
        expect(editingBanner().text).toBe("Alice is viewing/editing..");
        vi.advanceTimersByTime(300);
        expect(editingBanner().text).toBe("Alice is viewing/editing...");

        // a banner that belongs to somebody else must not be overwritten by Alice's animation
        paper
          .getModelById(mockScanPredicate.operatorID)
          .attr({ [`.${operatorCoeditorEditingClass}`]: { text: "Bob is viewing/editing..." } });
        vi.advanceTimersByTime(300);
        expect(editingBanner().text).toBe("Bob is viewing/editing...");
      } finally {
        clearInterval(intervalId);
        vi.useRealTimers();
      }
    });

    it("removeCurrentEditing hides the banner", () => {
      addScanOperator();
      vi.useFakeTimers();
      const intervalId = jointGraphWrapper.setCurrentEditing(alice, mockScanPredicate.operatorID);
      try {
        jointGraphWrapper.removeCurrentEditing(alice, mockScanPredicate.operatorID, intervalId);

        expect(editingBanner()).toMatchObject({ text: "", visibility: "hidden" });
      } finally {
        clearInterval(intervalId);
        vi.useRealTimers();
      }
    });

    it("set/removePropertyChanged show and hide the property-changed banner", () => {
      addScanOperator();
      const banner = (): { text?: string; fill?: string; visibility?: string } =>
        (
          paper.getModelById(mockScanPredicate.operatorID).attributes.attrs as Record<
            string,
            { text?: string; fill?: string; visibility?: string }
          >
        )[`.${operatorCoeditorChangedPropertyClass}`];

      jointGraphWrapper.setPropertyChanged(alice, mockScanPredicate.operatorID);
      expect(banner()).toMatchObject({
        text: "Alice changed property!",
        fill: alice.color,
        visibility: "visible",
      });

      jointGraphWrapper.removePropertyChanged(alice, mockScanPredicate.operatorID);
      expect(banner()).toMatchObject({ text: "", visibility: "hidden" });
    });
  });
});
