import { TestBed, inject } from '@angular/core/testing';

import { ValidationWorkflowService } from './validation-workflow.service';

describe('ValidationWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ValidationWorkflowService]
    });
  });

  it('should be created', inject([ValidationWorkflowService], (service: ValidationWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
