import { TestBed, inject } from '@angular/core/testing';

import { TableAutocompleteService } from './table-autocomplete.service';

describe('TableAutocompleteService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [TableAutocompleteService]
    });
  });

  it('should be created', inject([TableAutocompleteService], (service: TableAutocompleteService) => {
    expect(service).toBeTruthy();
  }));
});
