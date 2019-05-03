import { TestBed, inject } from '@angular/core/testing';

import { SaveWorkflowService, SavedWorkflow } from './save-workflow.service';
import { mockScanPredicate, mockResultPredicate } from '../workflow-graph/model/mock-workflow-data';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';

describe('SaveWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SaveWorkflowService]
    });
  });

  it('should be created', inject([SaveWorkflowService], (service: SaveWorkflowService) => {
    // expect(service).toBeTruthy();
    // const test: SavedWorkflow = {
    //   operators: [mockScanPredicate, mockResultPredicate],
    //   operatorPositions: {'1': {x: 1, y: 1}, }
    // };

    // sessionStorage.setItem();
    // service.loadWorkflow();

    // WorkflowActionService.getWo()


  }));
});
