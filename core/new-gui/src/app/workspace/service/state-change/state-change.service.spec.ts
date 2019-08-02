import { TestBed, inject } from '@angular/core/testing';

import { StateChangeService } from './state-change.service';

describe('StateChangeService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StateChangeService]
    });
  });

  it('should be created', inject([StateChangeService], (service: StateChangeService) => {
    expect(service).toBeTruthy();
  }));
});
