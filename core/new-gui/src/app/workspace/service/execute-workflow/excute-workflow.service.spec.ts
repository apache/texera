import { TestBed, inject } from '@angular/core/testing';

import { ExcuteWorkflowService } from './excute-workflow.service';

describe('ExcuteWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [ExcuteWorkflowService]
    });
  });

  it('should be created', inject([ExcuteWorkflowService], (service: ExcuteWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
