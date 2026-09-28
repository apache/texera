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
import { mockPoint, mockScanPredicate } from "../workflow-graph/model/mock-workflow-data";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { WorkflowSnippetService } from "./workflow-snippet.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowFragmentService } from "../workflow-fragment/workflow-fragment.service";

describe("WorkflowSnippetService", () => {
  let service: WorkflowSnippetService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [{ provide: OperatorMetadataService, useClass: StubOperatorMetadataService }, ...commonTestProviders],
    });
    service = TestBed.inject(WorkflowSnippetService);
    workflowActionService = TestBed.inject(WorkflowActionService);
    workflowActionService.addOperator(mockScanPredicate, mockPoint);
  });

  afterEach(() => localStorage.clear());

  const addSectionBox = () =>
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-reusable",
      comments: [],
      commentBoxPosition: { x: mockPoint.x - 30, y: mockPoint.y - 30 },
      overbox: { name: "Input block", color: "#1677ff", width: 300, height: 180 },
    });

  it("creates and persists a reusable section from a section box", () => {
    addSectionBox();
    const snippet = service.createFromSectionBox("commentBox-reusable", "Reusable input");

    expect(snippet.name).toBe("Input block");
    expect(service.getSnippets()).toEqual([snippet]);
    expect(localStorage.getItem(WorkflowSnippetService.STORAGE_KEY)).toContain("Input block");
    expect(snippet.fragment.sectionBox?.name).toBe(snippet.name);
  });

  it("trims the description and rejects a regular comment box", () => {
    workflowActionService.addCommentBox({
      commentBoxID: "commentBox-regular",
      comments: [],
      commentBoxPosition: { x: 0, y: 0 },
    });
    expect(() => service.createFromSectionBox("commentBox-regular", "")).toThrowError("Choose a section box to save.");
    addSectionBox();
    const snippet = service.createFromSectionBox("commentBox-reusable", "  Reusable input  ");
    expect(snippet.description).toBe("Reusable input");
  });

  it("ignores malformed stored data without preventing startup", () => {
    localStorage.setItem(WorkflowSnippetService.STORAGE_KEY, "not-json");

    const freshService = new WorkflowSnippetService(TestBed.inject(WorkflowFragmentService));

    expect(freshService.getSnippets()).toEqual([]);
  });

  it("ignores legacy snippets that do not contain a section box", () => {
    localStorage.setItem(
      WorkflowSnippetService.STORAGE_KEY,
      JSON.stringify([
        {
          id: "legacy",
          schemaVersion: 1,
          name: "Old snippet",
          description: "",
          fragment: { operators: [mockScanPredicate], operatorPositions: {}, links: [] },
        },
      ])
    );

    const freshService = new WorkflowSnippetService(TestBed.inject(WorkflowFragmentService));

    expect(freshService.getSnippets()).toEqual([]);
  });

  it("deletes the saved definition without touching inserted operators", () => {
    addSectionBox();
    const snippet = service.createFromSectionBox("commentBox-reusable", "");
    const inserted = service.insert(snippet.id, { x: 500, y: 300 });

    service.delete(snippet.id);

    expect(service.getSnippets()).toEqual([]);
    expect(workflowActionService.getTexeraGraph().hasOperator(inserted.operatorIDs[0])).toBe(true);
    expect(workflowActionService.getTexeraGraph().hasCommentBox(inserted.sectionBoxID!)).toBe(true);
  });
});
