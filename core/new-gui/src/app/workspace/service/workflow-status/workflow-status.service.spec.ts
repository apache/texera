import { TestBed, inject } from '@angular/core/testing';

import { WorkflowStatusService } from './workflow-status.service';

describe('WorkflowStatusService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowStatusService]
    });
  });

  it('should be created', inject([WorkflowStatusService], (service: WorkflowStatusService) => {
    expect(service).toBeTruthy();
  }));
});
