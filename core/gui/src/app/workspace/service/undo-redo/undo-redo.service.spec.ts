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

import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { mockScanPredicate, mockResultPredicate, mockPoint } from "../workflow-graph/model/mock-workflow-data";
import { TestBed, inject } from "@angular/core/testing";

import { UndoRedoService } from "./undo-redo.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";

describe("UndoRedoService", () => {
  let service: UndoRedoService;
  let workflowActionService: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UndoRedoService,
        WorkflowActionService,
        WorkflowUtilService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
      ],
    });
    service = TestBed.get(UndoRedoService);
    workflowActionService = TestBed.get(WorkflowActionService);
  });

  it("should be created", inject([UndoRedoService], (injectedService: UndoRedoService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("executing command should append to stack", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(service.getUndoLength()).toEqual(1);
    expect(service.getRedoLength()).toEqual(0);
  });

  it("redoing command should move from undo to redo stack and vice versa", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    service.undoAction();
    expect(service.getUndoLength()).toEqual(0);
    expect(service.getRedoLength()).toEqual(1);

    service.redoAction();
    expect(service.getUndoLength()).toEqual(1);
    expect(service.getRedoLength()).toEqual(0);
  });

  it("executing new action clears redo stack", () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    service.undoAction();
    expect(service.getUndoLength()).toEqual(0);
    expect(service.getRedoLength()).toEqual(1);

    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    expect(service.getUndoLength()).toEqual(1);
    expect(service.getRedoLength()).toEqual(0);
  });
});
