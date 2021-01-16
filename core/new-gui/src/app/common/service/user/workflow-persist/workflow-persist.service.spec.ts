import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';

import { AppSettings } from '../../../app-setting';
import { WorkflowPersistService } from './workflow-persist.service';

describe('WorkflowPersistService', () => {
  let service: WorkflowPersistService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [
        HttpClientTestingModule
      ]
    });
    service = TestBed.inject(WorkflowPersistService);

  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
