import { StubOperatorMetadataService } from './../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../operator-metadata/operator-metadata.service';
import { Command, WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import {
  mockScanPredicate, mockResultPredicate, mockPoint
} from './../workflow-graph/model/mock-workflow-data';
import { TestBed, inject } from '@angular/core/testing';

import { UndoRedoService } from './undo-redo.service';
// make some mock data maybe

// TODO FOR TESTING
// Write test functions for navigation component, property editor, here, and workflow action
// Let's first write a function in nav component, one for undo and one for redo
describe('UndoRedoService', () => {
  let undoRedoService: UndoRedoService;
  let workflowActionService: WorkflowActionService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UndoRedoService,
        WorkflowActionService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    });
    undoRedoService = TestBed.get(UndoRedoService);
    workflowActionService = TestBed.get(WorkflowActionService);
  });

  it('should be created', inject([UndoRedoService], (service: UndoRedoService) => {
    expect(service).toBeTruthy();
  }));

  it('executing command should append to stack', () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    expect(undoRedoService.getUndoLength).toEqual(1);
    expect(undoRedoService.getRedoLength).toEqual(0);
  });

  it('redoing command should move from undo to redo stack and vice versa', () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    undoRedoService.undoAction();
    expect(undoRedoService.getUndoLength).toEqual(0);
    expect(undoRedoService.getRedoLength).toEqual(1);

    undoRedoService.redoAction();
    expect(undoRedoService.getUndoLength).toEqual(1);
    expect(undoRedoService.getRedoLength).toEqual(0);
  });

  it('executing new action clears redo stack', () => {
    workflowActionService.addOperator(mockScanPredicate, mockPoint);

    undoRedoService.undoAction();
    expect(undoRedoService.getUndoLength).toEqual(0);
    expect(undoRedoService.getRedoLength).toEqual(1);

    workflowActionService.addOperator(mockResultPredicate, mockPoint);
    expect(undoRedoService.getUndoLength).toEqual(1);
    expect(undoRedoService.getRedoLength).toEqual(0);
  });
});
