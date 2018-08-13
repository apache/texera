import { TestBed, inject } from '@angular/core/testing';

import { WorkflowGraphUtilsService } from './workflow-graph-utils.service';

describe('WorkflowGraphUtilsService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowGraphUtilsService]
    });
  });

  it('should be created', inject([WorkflowGraphUtilsService], (service: WorkflowGraphUtilsService) => {
    expect(service).toBeTruthy();
  }));
});
