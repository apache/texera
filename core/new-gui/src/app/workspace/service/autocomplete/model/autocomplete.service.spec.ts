import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { AutocompleteService } from './autocomplete.service';
import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from './../../operator-metadata/stub-operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';
import { SourceTableNamesAPIResponse } from '../../../types/source-table-names.interface';
import { mockSourceTableAPIResponse } from './mock-autocomplete-service.data';

class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public get(url: string): Observable<SourceTableNamesAPIResponse> {
    return Observable.of(mockSourceTableAPIResponse).delay(1);
  }
}

describe('AutocompleteService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [AutocompleteService,
        { provide: HttpClient, useClass: StubHttpClient },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService}
      ]
    });
  });

  it('should be created', inject([AutocompleteService], (service: AutocompleteService) => {
    expect(service).toBeTruthy();
  }));
});
