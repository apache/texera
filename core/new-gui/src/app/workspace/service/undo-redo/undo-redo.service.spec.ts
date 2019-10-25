import { StubOperatorMetadataService } from './../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../operator-metadata/operator-metadata.service';
import {Command } from './../workflow-graph/model/workflow-action.service';
import { TestBed, inject } from '@angular/core/testing';

import { UndoRedoService } from './undo-redo.service';
// make some mock data maybe

// TODO FOR TESTING
// Write test functions for navigation component, property editor, here, and workflow action
// Let's first write a function in nav component, one for undo and one for redo
describe('UndoRedoService', () => {
  let undoRedoService = UndoRedoService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UndoRedoService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    });
    undoRedoService = TestBed.get(UndoRedoService);
  });

  it('should be created', inject([UndoRedoService], (service: UndoRedoService) => {
    expect(service).toBeTruthy();
  }));
});
