import { TestBed, inject } from '@angular/core/testing';

import { OperatorMetadataService } from './operator-metadata.service';

describe('OperatorMetadataService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OperatorMetadataService]
    });
  });

  it('should be created', inject([OperatorMetadataService], (service: OperatorMetadataService) => {
    expect(service).toBeTruthy();
  }));
});
