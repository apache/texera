import { TestBed, inject } from '@angular/core/testing';

import { WorkflowSyncModelService } from './workflow-sync-model.service';

describe('WorkflowSyncModelService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowSyncModelService]
    });
  });

  it('should be created', inject([WorkflowSyncModelService], (service: WorkflowSyncModelService) => {
    expect(service).toBeTruthy();
  }));
});
