import { TestBed, inject } from '@angular/core/testing';

import { WorkflowViewEventService } from './workflow-view-event.service';

describe('WorkflowViewEventService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowViewEventService]
    });
  });

  it('should be created', inject([WorkflowViewEventService], (service: WorkflowViewEventService) => {
    expect(service).toBeTruthy();
  }));
});
