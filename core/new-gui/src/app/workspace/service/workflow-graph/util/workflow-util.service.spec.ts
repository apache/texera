import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUtilService } from './workflow-util.service';

describe('WorkflowUtilService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUtilService]
    });
  });

  it('should be created', inject([WorkflowUtilService], (service: WorkflowUtilService) => {
    expect(service).toBeTruthy();
  }));
});
