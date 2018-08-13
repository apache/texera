import { TestBed, inject } from '@angular/core/testing';

import { WorkflowJointGraphService } from './workflow-joint-graph.service';

describe('WorkflowJointGraphService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowJointGraphService]
    });
  });

  it('should be created', inject([WorkflowJointGraphService], (service: WorkflowJointGraphService) => {
    expect(service).toBeTruthy();
  }));
});
