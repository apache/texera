import { TestBed } from '@angular/core/testing';

import { SyncJointModelServiceService } from './sync-joint-model-service.service';

describe('SyncJointModelServiceService', () => {
  let service: SyncJointModelServiceService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SyncJointModelServiceService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
