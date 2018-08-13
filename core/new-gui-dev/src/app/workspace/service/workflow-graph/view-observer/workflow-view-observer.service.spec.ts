import { TestBed, inject } from '@angular/core/testing';

import { WorkflowViewObserverService } from './workflow-view-observer.service';

describe('WorkflowViewObserverService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowViewObserverService]
    });
  });

  it('should be created', inject([WorkflowViewObserverService], (service: WorkflowViewObserverService) => {
    expect(service).toBeTruthy();
  }));
});
