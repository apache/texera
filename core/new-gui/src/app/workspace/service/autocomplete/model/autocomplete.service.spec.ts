import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { AutocompleteService } from './autocomplete.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';
import { SourceTableNamesAPIResponse, SuccessExecutionResult } from '../../../types/autocomplete.interface';
import { mockSourceTableAPIResponse, mockAutocompleteAPISchemaSuggestionResponse } from '../../../mock-data/mock-autocomplete-service.data';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { JointUIService } from '../../joint-ui/joint-ui.service';

class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public get(url: string): Observable<SourceTableNamesAPIResponse> {
    return Observable.of(mockSourceTableAPIResponse).delay(1);
  }

  public post(url: string): Observable<SuccessExecutionResult> {
    return Observable.of(mockAutocompleteAPISchemaSuggestionResponse).delay(1);
  }

}

describe('AutocompleteService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [AutocompleteService,
        WorkflowActionService,
        JointUIService,
        { provide: HttpClient, useClass: StubHttpClient },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService}
      ]
    });
  });

  it('should be created', inject([AutocompleteService], (service: AutocompleteService) => {
    expect(service).toBeTruthy();
  }));
});
