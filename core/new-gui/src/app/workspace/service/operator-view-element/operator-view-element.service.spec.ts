import { TestBed, inject } from '@angular/core/testing';

import { OperatorViewElementService } from './operator-view-element.service';

describe('OperatorViewElementService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [OperatorViewElementService]
    });
  });

  it('should be created', inject([OperatorViewElementService], (service: OperatorViewElementService) => {
    expect(service).toBeTruthy();
  }));
});
