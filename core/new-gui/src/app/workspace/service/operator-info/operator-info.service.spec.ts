import { TestBed, inject } from '@angular/core/testing';

import { OperatorInfoService } from './operator-info.service';

describe('OperatorInfoService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OperatorInfoService]
    });
  });

  it('should be created', inject([OperatorInfoService], (service: OperatorInfoService) => {
    expect(service).toBeTruthy();
  }));
});
