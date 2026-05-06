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

import { JointUIService } from "../joint-ui/joint-ui.service";
import { inject, TestBed } from "@angular/core/testing";
import { DragDropService } from "./drag-drop.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { marbles } from "rxjs-marbles";
import {
  mockMultiInputOutputPredicate,
  mockResultPredicate,
  mockScanPredicate,
  mockScanResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { VIEW_RESULT_OP_TYPE } from "../workflow-graph/model/workflow-graph";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("DragDropService", () => {
  let dragDropService: DragDropService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        JointUIService,
        WorkflowActionService,
        UndoRedoService,
        WorkflowUtilService,
        DragDropService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        ...commonTestProviders,
      ],
    });

    dragDropService = TestBed.inject(DragDropService);

    // custom equality disregards link ID (since I use DragDropService.getNew)
    /* TODO(vitest): no equivalent — port via expect.extend */ ((..._args: unknown[]) => {})(
      (link1: OperatorLink, link2: OperatorLink) => {
        if (typeof link1 === "object" && typeof link2 === "object") {
          return link1.source === link2.source && link1.target === link2.target;
        }
      }
    );
  });

  it("should be created", inject([DragDropService], (injectedService: DragDropService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should successfully create a new operator link given 2 operator predicates", () => {
    const createdLink: OperatorLink = (dragDropService as any).getNewOperatorLink(
      mockScanPredicate,
      mockResultPredicate
    );

    expect(createdLink.source).toEqual(mockScanResultLink.source);
    expect(createdLink.target).toEqual(mockScanResultLink.target);
  });

  it("should find 3 input operatorPredicates and 3 output operatorPredicates for an operatorPredicate with 3 input / 3 output ports", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);

    const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);

    workflowActionService.addOperator(input1, { x: 0, y: 0 });
    workflowActionService.addOperator(input2, { x: 0, y: 10 });
    workflowActionService.addOperator(input3, { x: 0, y: 20 });
    workflowActionService.addOperator(output1, { x: 100, y: 0 });
    workflowActionService.addOperator(output2, { x: 100, y: 10 });
    workflowActionService.addOperator(output3, { x: 100, y: 20 });

    // Probe at the centroid between the input and output columns. With the
    // SUGGESTION_DISTANCE_THRESHOLD = 300, all 6 operators are in range; the
    // 3 to the left are ranked as inputs, the 3 to the right as outputs.
    // Order within each list is heap-internal and not guaranteed by the
    // implementation — assert membership only.
    const [inputOps, outputOps] = (dragDropService as any).findClosestOperators(
      { x: 50, y: 0 },
      mockMultiInputOutputPredicate
    );

    expect(inputOps).toHaveLength(3);
    expect(inputOps).toEqual(expect.arrayContaining([input1, input2, input3]));
    expect(outputOps).toHaveLength(3);
    expect(outputOps).toEqual(expect.arrayContaining([output1, output2, output3]));
  });

  it('should publish operatorPredicates to highlight streams when calling "updateHighlighting(prevHighlights,newHighlights)"', async () => {
    TestBed.inject(WorkflowActionService);
    const highlights: string[] = [];
    const unhighlights: string[] = [];
    const expectedHighlights = [mockScanPredicate.operatorID, mockScanPredicate.operatorID];
    const expectedUnhighlights = [mockScanPredicate.operatorID, mockResultPredicate.operatorID];
    // allow test to run for 10ms before checking, since observables are async
    const timeout = new Promise(resolve => setTimeout(resolve, 10));

    dragDropService.getOperatorSuggestionHighlightStream().subscribe(operatorID => {
      highlights.push(operatorID);
    });
    dragDropService.getOperatorSuggestionUnhighlightStream().subscribe(operatorID => {
      unhighlights.push(operatorID);
    });

    // highlighting update situations
    (dragDropService as any).updateHighlighting([mockScanPredicate], [mockScanPredicate]); // no change
    (dragDropService as any).updateHighlighting([], [mockScanPredicate]); // new highlight
    (dragDropService as any).updateHighlighting([mockScanPredicate], []); // new unhighlight
    (dragDropService as any).updateHighlighting([mockResultPredicate], [mockScanPredicate]); // new highlight and unhighlight

    // allow test to run for up to 500ms before checking, since observables are async
    await timeout;
    expect(highlights).toEqual(expectedHighlights);
    expect(unhighlights).toEqual(expectedUnhighlights);
  });

  it("should not find any operator when the mouse coordinate is greater than the threshold defined", () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);

    workflowActionService.addOperator(mockScanPredicate, { x: 0, y: 0 });

    const [inputOps] = (dragDropService as any).findClosestOperators(
      {
        x: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
        y: DragDropService.SUGGESTION_DISTANCE_THRESHOLD + 10,
      },
      mockResultPredicate
    );

    expect(inputOps).toEqual([]);
  });

  it("should add the dropped operator with links to suggested neighbors and unhighlight prior suggestions", async () => {
    const workflowActionService: WorkflowActionService = TestBed.inject(WorkflowActionService);
    const workflowUtilService: WorkflowUtilService = TestBed.inject(WorkflowUtilService);
    const input1 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input2 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const input3 = workflowUtilService.getNewOperatorPredicate("ScanSource");
    const output1 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output2 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);
    const output3 = workflowUtilService.getNewOperatorPredicate(VIEW_RESULT_OP_TYPE);

    // Real main jointjs paper attached to a hidden DOM host so coordinate
    // transforms in `dragStarted` / mousemove / `dragDropped` resolve
    // without stubs. jsdom doesn't compute layout, so the SVG polyfill's
    // identity matrices collapse `pageToLocalPoint(x, y)` to (0, 0)
    // regardless of input — that's why operators are placed at x=±100
    // around the origin below.
    const paperHost = document.createElement("div");
    const flyingOpHost = document.createElement("div");
    flyingOpHost.id = "flyingOP";
    document.body.appendChild(paperHost);
    document.body.appendChild(flyingOpHost);
    try {
      workflowActionService.getJointGraphWrapper().attachMainJointPaper({ el: paperHost });

      // Inputs at negative x and outputs at positive x so the (0, 0) drop
      // point classifies them correctly via `findClosestOperators` (which
      // compares operator x against mouse x).
      workflowActionService.addOperator(input1, { x: -100, y: 10 });
      workflowActionService.addOperator(input2, { x: -100, y: 20 });
      workflowActionService.addOperator(input3, { x: -100, y: 30 });
      workflowActionService.addOperator(output1, { x: 100, y: 10 });
      workflowActionService.addOperator(output2, { x: 100, y: 20 });
      workflowActionService.addOperator(output3, { x: 100, y: 30 });

      const unhighlights: string[] = [];
      dragDropService.getOperatorSuggestionUnhighlightStream().subscribe(id => unhighlights.push(id));
      const links: OperatorLink[] = [];
      workflowActionService
        .getTexeraGraph()
        .getLinkAddStream()
        .subscribe(link => links.push(link));

      // dragStarted creates a fresh `op` of the given type and subscribes
      // to window mousemove to populate suggestionInputs / suggestionOutputs.
      dragDropService.dragStarted("MultiInputOutput");
      const droppedOp = (dragDropService as any).op as OperatorPredicate;

      // Drive the suggestion pipeline. Any mousemove will do — jsdom's
      // `pageToLocalPoint` collapses to (0, 0) regardless of the
      // dispatched coordinates.
      window.dispatchEvent(new MouseEvent("mousemove", { clientX: 0, clientY: 0 }));
      await new Promise(resolve => setTimeout(resolve, 0));

      dragDropService.dragDropped({ x: 0, y: 0 });
      // Tear down the window-level mousemove subscriptions installed by
      // `dragStarted`. Without this the `first()` mouseup observer stays
      // armed and a stray mousemove from a later spec re-enters this
      // service's suggestion pipeline.
      window.dispatchEvent(new MouseEvent("mouseup"));
      await new Promise(resolve => setTimeout(resolve, 0));

      // Each suggested operator should have been unhighlighted at drop time.
      expect(unhighlights).toEqual(
        expect.arrayContaining([
          input1.operatorID,
          input2.operatorID,
          input3.operatorID,
          output1.operatorID,
          output2.operatorID,
          output3.operatorID,
        ])
      );
      expect(unhighlights).toHaveLength(6);

      // 3 input→droppedOp links and 3 droppedOp→output links.
      expect(links).toHaveLength(6);
      const inputLinks = links.filter(l => l.target.operatorID === droppedOp.operatorID);
      const outputLinks = links.filter(l => l.source.operatorID === droppedOp.operatorID);
      expect(inputLinks.map(l => l.source.operatorID).sort()).toEqual(
        [input1.operatorID, input2.operatorID, input3.operatorID].sort()
      );
      expect(outputLinks.map(l => l.target.operatorID).sort()).toEqual(
        [output1.operatorID, output2.operatorID, output3.operatorID].sort()
      );
    } finally {
      // Always clean up the DOM hosts even if an assertion above threw,
      // so the JointJS papers don't leak into later specs.
      document.body.removeChild(paperHost);
      document.body.removeChild(flyingOpHost);
    }
  });
});
