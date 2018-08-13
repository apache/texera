import { TestBed, inject } from '@angular/core/testing';

import { ExecuteWorkflowService } from './execute-workflow.service';

describe('ExecuteWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ExecuteWorkflowService]
    });
  });

  it('should be created', inject([ExecuteWorkflowService], (service: ExecuteWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
