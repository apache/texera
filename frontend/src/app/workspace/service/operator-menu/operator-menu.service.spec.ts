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
import { HttpClientModule } from "@angular/common/http";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  mockCommentBox,
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockSentimentPredicate,
} from "../workflow-graph/model/mock-workflow-data";
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
        ...commonTestProviders,
      ],
      imports: [HttpClientModule],
    });
    workflowActionService = TestBed.inject(WorkflowActionService);
    service = TestBed.inject(OperatorMenuService);

    subs = new Subscription();
    subs.add(service.highlightedOperators$.subscribe(ids => (opsLatest = ids)));
    subs.add(service.highlightedCommentBoxes$.subscribe(ids => (boxesLatest = ids)));
  });

  afterEach(() => subs.unsubscribe());

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
});
