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
import { mockPoint, mockResultPredicate, mockScanPredicate } from "../workflow-graph/model/mock-workflow-data";
import { inject, TestBed } from "@angular/core/testing";

import { UndoRedoService } from "./undo-redo.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import * as Y from "yjs";

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
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(UndoRedoService);
    workflowActionService = TestBed.inject(WorkflowActionService);
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

  describe("with a stubbed undo manager", () => {
    let undoRedo: UndoRedoService;
    let manager: {
      canUndo: ReturnType<typeof vi.fn>;
      canRedo: ReturnType<typeof vi.fn>;
      undo: ReturnType<typeof vi.fn>;
      redo: ReturnType<typeof vi.fn>;
      clear: ReturnType<typeof vi.fn>;
      undoStack: unknown[];
      redoStack: unknown[];
    };

    beforeEach(() => {
      undoRedo = new UndoRedoService();
      manager = {
        canUndo: vi.fn().mockReturnValue(true),
        canRedo: vi.fn().mockReturnValue(true),
        undo: vi.fn(),
        redo: vi.fn(),
        clear: vi.fn(),
        undoStack: [1, 2, 3],
        redoStack: [1],
      };
      undoRedo.setUndoManager(manager as unknown as Y.UndoManager);
    });

    afterEach(() => vi.restoreAllMocks()); // guarantees any console spy is restored even if an assertion throws

    it("undoAction / redoAction delegate to the manager and toggle the joint-command guard back on", () => {
      undoRedo.undoAction();
      undoRedo.redoAction();

      expect(manager.undo).toHaveBeenCalledTimes(1);
      expect(manager.redo).toHaveBeenCalledTimes(1);
      expect(undoRedo.listenJointCommand).toBe(true);
    });

    it("undoAction / redoAction are no-ops when the manager cannot undo/redo", () => {
      manager.canUndo.mockReturnValue(false);
      manager.canRedo.mockReturnValue(false);

      undoRedo.undoAction();
      undoRedo.redoAction();

      expect(manager.undo).not.toHaveBeenCalled();
      expect(manager.redo).not.toHaveBeenCalled();
    });

    it("undoAction / redoAction are no-ops while workflow modification is disabled", () => {
      vi.spyOn(console, "error").mockImplementation(() => {}); // restored by afterEach
      undoRedo.disableWorkFlowModification();

      undoRedo.undoAction();
      undoRedo.redoAction();

      expect(manager.undo).not.toHaveBeenCalled();
      expect(manager.redo).not.toHaveBeenCalled();
    });

    it("canUndo / canRedo require both the modification guard and the manager", () => {
      expect(undoRedo.canUndo()).toBe(true);
      expect(undoRedo.canRedo()).toBe(true);

      undoRedo.disableWorkFlowModification();
      expect(undoRedo.canUndo()).toBe(false);
      expect(undoRedo.canRedo()).toBe(false);

      undoRedo.enableWorkFlowModification();
      manager.canUndo.mockReturnValue(false);
      expect(undoRedo.canUndo()).toBe(false);
    });

    it("getUndoLength / getRedoLength reflect the manager stacks", () => {
      expect(undoRedo.getUndoLength()).toBe(3);
      expect(undoRedo.getRedoLength()).toBe(1);
    });

    it("clearUndoStack / clearRedoStack clear the correct side of the manager", () => {
      undoRedo.clearUndoStack();
      expect(manager.clear).toHaveBeenCalledWith(true, false);

      undoRedo.clearRedoStack();
      expect(manager.clear).toHaveBeenCalledWith(false, true);
    });
  });

  describe("without an undo manager", () => {
    it("canUndo / canRedo are false", () => {
      const undoRedo = new UndoRedoService();
      expect(undoRedo.canUndo()).toBe(false);
      expect(undoRedo.canRedo()).toBe(false);
    });
  });
});
