import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { AutocompleteService } from './autocomplete.service';
import { OperatorMetadataService } from '../../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';
import { SourceTableNamesAPIResponse, AutocompleteSucessResult } from '../../../types/autocomplete.interface';
import { mockSourceTableAPIResponse, mockAutocompleteAPISchemaSuggestionResponse, mockAutocompletedOperatorSchema
 } from './mock-autocomplete-service.data';
import { WorkflowActionService } from '../../workflow-graph/model/workflow-action.service';
import { JointUIService } from '../../joint-ui/joint-ui.service';
import { AutocompleteUtils } from '../util/autocomplete.utils';

import { mockPoint, mockSentimentPredicate } from '../../workflow-graph/model/mock-workflow-data';
import { mockOperatorMetaData } from '../../operator-metadata/mock-operator-metadata.data';

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
  let workflowActionService: WorkflowActionService;

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
    workflowActionService = TestBed.get(WorkflowActionService);
  });

  it('should be created', inject([AutocompleteService], (service: AutocompleteService) => {
    expect(service).toBeTruthy();
  }));

  it('should call post when invoking autocomplete API', () => {
    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockAutocompleteAPISchemaSuggestionResponse)
    );

    autocompleteService.invokeAutocompleteAPI();
    expect(httpClient.post).toHaveBeenCalledTimes(1);
  });

  it('should update dynamic schema based on the input attributes returned from the backend', () => {
    const httpClient: HttpClient = TestBed.get(HttpClient);
    spyOn(httpClient, 'post').and.returnValue(
      Observable.of(mockAutocompleteAPISchemaSuggestionResponse)
    );

    const mockTables: ReadonlyArray<string> = ['promed', 'twitter_sample'];
    const mockMetaData = AutocompleteUtils.addSourceTableNamesToMetadata(mockOperatorMetaData, mockTables);
    (autocompleteService as any).operatorSchemaList = mockMetaData.operators;
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);
    autocompleteService.invokeAutocompleteAPI();
    const schema = autocompleteService.getDynamicSchema(mockSentimentPredicate);
    expect(schema).toBeTruthy();
    expect(schema.operatorType).toEqual(mockSentimentPredicate.operatorType);
    expect(schema).toEqual(mockAutocompletedOperatorSchema[1]);
  });
});
