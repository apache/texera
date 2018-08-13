import { TestBed, inject } from '@angular/core/testing';

import { WorkflowModelActionService } from './workflow-model-action.service';

describe('WorkflowModelActionService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowModelActionService]
    });
  });

  it('should be created', inject([WorkflowModelActionService], (service: WorkflowModelActionService) => {
    expect(service).toBeTruthy();
  }));
});
