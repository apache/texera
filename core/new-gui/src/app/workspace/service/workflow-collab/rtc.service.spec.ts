import { TestBed } from '@angular/core/testing';

import { RtcService } from './rtc.service';

describe('RtcService', () => {
  let service: RtcService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(RtcService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
