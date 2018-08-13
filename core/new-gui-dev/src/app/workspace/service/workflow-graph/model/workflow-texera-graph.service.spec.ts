import { TestBed, inject } from '@angular/core/testing';

import { WorkflowTexeraGraphService } from './workflow-texera-graph.service';

describe('WorkflowTexeraGraphService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowTexeraGraphService]
    });
  });

  it('should be created', inject([WorkflowTexeraGraphService], (service: WorkflowTexeraGraphService) => {
    expect(service).toBeTruthy();
  }));
});
