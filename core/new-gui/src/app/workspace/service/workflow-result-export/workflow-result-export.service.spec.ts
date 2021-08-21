import { TestBed } from '@angular/core/testing';

import { WorkflowResultExportService } from './workflow-result-export.service';

describe('WorkflowResultExportService', () => {
  let service: WorkflowResultExportService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(WorkflowResultExportService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
