import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUtilService } from './workflow-util.service';

describe('WorkflowUtilService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        WorkflowUtilService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    });
  });

  it('should be created', inject([WorkflowUtilService], (service: WorkflowUtilService) => {
    expect(service).toBeTruthy();
  }));
});
