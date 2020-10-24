import {TestBed} from '@angular/core/testing';

import {WorkflowPersistService} from './workflow-persist.service';

describe('WorkflowPersistService', () => {
  let service: WorkflowPersistService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WorkflowPersistService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
