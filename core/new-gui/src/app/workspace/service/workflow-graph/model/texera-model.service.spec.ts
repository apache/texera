import { TestBed, inject } from '@angular/core/testing';

import { TexeraModelService } from './texera-model.service';

describe('TexeraModelService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TexeraModelService]
    });
  });

  it('should be created', inject([TexeraModelService], (service: TexeraModelService) => {
    expect(service).toBeTruthy();
  }));
});
