import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { AutocompleteService } from './autocomplete.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';
import { SourceTableNamesAPIResponse, AutocompleteSucessResult } from '../../../types/autocomplete.interface';
import { mockSourceTableAPIResponse, mockAutocompleteAPISchemaSuggestionResponse } from './mock-autocomplete-service.data';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { marbles } from 'rxjs-marbles';

class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public get(url: string): Observable<SourceTableNamesAPIResponse> {
    return Observable.of(mockSourceTableAPIResponse).delay(1);
  }

  public post(url: string): Observable<AutocompleteSucessResult> {
    return Observable.of(mockAutocompleteAPISchemaSuggestionResponse).delay(1);
  }

}

describe('AutocompleteService', () => {
  let autocompleteService: AutocompleteService;
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [AutocompleteService,
        WorkflowActionService,
        JointUIService,
        { provide: HttpClient, useClass: StubHttpClient },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService}
      ]
    });

    autocompleteService = TestBed.get(AutocompleteService);
  });

  it('should be created', inject([AutocompleteService], (service: AutocompleteService) => {
    expect(service).toBeTruthy();
  }));

  it('should call post when invoking autocomplete API', () => {
    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockAutocompleteAPISchemaSuggestionResponse)
    );

    autocompleteService.invokeAutocompleteAPI(true);
    expect(httpClient.post).toHaveBeenCalledTimes(1);
  });

  it('should notify autocompleteAPIExecutedStream when autcomplete API is invoked with true parameter', marbles((m) => {
    const apiExecutedStream = autocompleteService.getAutocompleteAPIExecutedStream()
      .map(() => 'a');

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockAutocompleteAPISchemaSuggestionResponse)
    );

    m.hot('-a-').do(() => autocompleteService.invokeAutocompleteAPI(true)).subscribe();

    const expectedStream = m.hot('-a-');

    m.expect(apiExecutedStream).toBeObservable(expectedStream);
  }));

  it('should not notify autocompleteAPIExecutedStream when autcomplete API is invoked with false parameter', marbles((m) => {
    const apiExecutedStream = autocompleteService.getAutocompleteAPIExecutedStream()
      .map(() => 'a');

    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockAutocompleteAPISchemaSuggestionResponse)
    );

    m.hot('-a-').do(() => autocompleteService.invokeAutocompleteAPI(false)).subscribe();

    const expectedStream = m.hot('---');

    m.expect(apiExecutedStream).toBeObservable(expectedStream);
  }));
});
