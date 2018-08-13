import { TestBed, inject } from '@angular/core/testing';

import { OperatorUIElementService } from './operator-ui-element.service';

describe('OperatorUIElementService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OperatorUIElementService]
    });
  });

  it('should be created', inject([OperatorUIElementService], (service: OperatorUIElementService) => {
    expect(service).toBeTruthy();
  }));
});
