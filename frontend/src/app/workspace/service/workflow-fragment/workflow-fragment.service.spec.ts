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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  mockPoint,
  mockResultPredicate,
  mockScanPredicate,
  mockScanSentimentLink,
  mockSentimentPredicate,
  mockSentimentResultLink,
} from "../workflow-graph/model/mock-workflow-data";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { WorkflowFragmentService } from "./workflow-fragment.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";

describe("WorkflowFragmentService", () => {
  let service: WorkflowFragmentService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowFragmentService);
    workflowActionService = TestBed.inject(WorkflowActionService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, { x: 200, y: 100 });
    workflowActionService.addOperator(mockResultPredicate, { x: 400, y: 100 });
    workflowActionService.addLink(mockScanSentimentLink);
    workflowActionService.addLink(mockSentimentResultLink);
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-capture",
      comments: [],
      commentBoxPosition: { x: 100, y: 100 },
      overbox: { name: "Capture", color: "#1677ff", width: 275, height: 180 },
    });
  });

  it("rejects a missing section box", () => {
    expect(() => service.captureSectionBox("missing")).toThrowError("Choose a section box to save.");
  });

  it("captures only links whose two operators are selected", () => {
    const fragment = service.captureSectionBox("commentBox-capture");

    expect(fragment.operators.map(operator => operator.operatorID)).toEqual([
      mockScanPredicate.operatorID,
      mockSentimentPredicate.operatorID,
    ]);
    expect(fragment.links).toEqual([mockScanSentimentLink]);
    expect(fragment.operatorPositions[mockScanPredicate.operatorID]).toEqual({ x: 0, y: 0 });
    expect(fragment.operatorPositions[mockSentimentPredicate.operatorID]).toEqual({ x: 100, y: 0 });
  });

  it("captures only operators geometrically contained by a section box", () => {
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-section",
      comments: [],
      commentBoxPosition: { x: 75, y: 70 },
      overbox: { name: "Text cleaning", color: "#1677ff", width: 300, height: 180 },
    });

    const fragment = service.captureSectionBox("commentBox-section");

    expect(fragment.operators.map(operator => operator.operatorID)).toEqual([
      mockScanPredicate.operatorID,
      mockSentimentPredicate.operatorID,
    ]);
    expect(fragment.links).toEqual([mockScanSentimentLink]);
    expect(fragment.operatorPositions[mockScanPredicate.operatorID]).toEqual({ x: 25, y: 30 });
    expect(fragment.sectionBox).toEqual({
      name: "Text cleaning",
      color: "#1677ff",
      width: 300,
      height: 180,
    });
  });

  it("rejects an empty section box instead of saving an unusable definition", () => {
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-empty-section",
      comments: [],
      commentBoxPosition: { x: 800, y: 700 },
      overbox: { name: "Empty", color: "#1677ff", width: 300, height: 180 },
    });

    expect(() => service.captureSectionBox("commentBox-empty-section")).toThrowError(
      "The section box does not contain any operators."
    );
  });

  it("deep-copies operator properties, including UDF code", () => {
    const udf = {
      ...mockSentimentPredicate,
      operatorID: "SentimentAnalysis-udf-code-test",
      operatorProperties: { code: "yield tuple_", nested: { value: 1 } },
    };
    workflowActionService.addOperator(udf, { x: 600, y: 100 });

    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-udf",
      comments: [],
      commentBoxPosition: { x: 600, y: 100 },
      overbox: { name: "Code", color: "#1677ff", width: 300, height: 180 },
    });
    const fragment = service.captureSectionBox("commentBox-udf");
    (udf.operatorProperties.nested as { value: number }).value = 2;

    expect(fragment.operators[0].operatorProperties["code"]).toBe("yield tuple_");
    expect(fragment.operators[0].operatorProperties["nested"]).toEqual({ value: 1 });
  });

  it("inserts independent operators and remaps their internal links", () => {
    const fragment = service.captureSectionBox("commentBox-capture");
    const inserted = service.insert(fragment, { x: 800, y: 300 });

    expect(inserted.operatorIDs).toHaveLength(2);
    expect(inserted.operatorIDs).not.toContain(mockScanPredicate.operatorID);
    expect(inserted.linkIDs).toHaveLength(1);

    const insertedLink = workflowActionService.getTexeraGraph().getLinkWithID(inserted.linkIDs[0]);
    expect(inserted.operatorIDs).toContain(insertedLink.source.operatorID);
    expect(inserted.operatorIDs).toContain(insertedLink.target.operatorID);
    expect(insertedLink.source.operatorID).not.toBe(mockScanSentimentLink.source.operatorID);
  });

  it("inserts an independent section box with the captured graph", () => {
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-section",
      comments: [],
      commentBoxPosition: { x: 75, y: 70 },
      overbox: { name: "Text cleaning", color: "#1677ff", width: 300, height: 180 },
    });
    const fragment = service.captureSectionBox("commentBox-section");

    const inserted = service.insert(fragment, { x: 700, y: 400 });

    expect(inserted.sectionBoxID).toBeDefined();
    const sectionBox = workflowActionService.getTexeraGraph().getCommentBox(inserted.sectionBoxID!);
    expect(sectionBox.commentBoxPosition).toEqual({ x: 700, y: 400 });
    expect(sectionBox.overbox).toEqual(fragment.sectionBox);
    expect(workflowActionService.getJointGraphWrapper().getElementPosition(inserted.operatorIDs[0])).toEqual({
      x: 725,
      y: 430,
    });
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()).toEqual(
      inserted.operatorIDs
    );
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedLinkIDs()).toEqual(inserted.linkIDs);
    expect(workflowActionService.getJointGraphWrapper().getCurrentHighlightedCommentBoxIDs()).toEqual([
      inserted.sectionBoxID,
    ]);
  });
});
