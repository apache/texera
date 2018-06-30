import { TestBed, inject } from '@angular/core/testing';

import { SourceTableNamesService } from './source-table-names.service';

describe('SourceTableNamesService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SourceTableNamesService]
    });
  });

  it('should be created', inject([SourceTableNamesService], (service: SourceTableNamesService) => {
    expect(service).toBeTruthy();
  }));
});
