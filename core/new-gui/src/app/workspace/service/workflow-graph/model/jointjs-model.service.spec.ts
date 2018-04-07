import { TestBed, inject } from '@angular/core/testing';

import { JointjsModelService } from './jointjs-model.service';

describe('JointjsModelService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [JointjsModelService]
    });
  });

  it('should be created', inject([JointjsModelService], (service: JointjsModelService) => {
    expect(service).toBeTruthy();
  }));
});
