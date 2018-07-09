import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';

import { StubOperatorMetadataService } from '../../operator-metadata/stub-operator-metadata.service';
import { AutocompleteUtils } from '../util/autocomplete.utils';
import { mockSourceTableAPIResponse } from '../../../mock-data/mock-autocomplete-service.data';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';

import { combineLatest } from 'rxjs/observable/combineLatest';

export const SOURCE_OPERATORS_REQUIRING_TABLENAMES: ReadonlyArray<string> = ['KeywordSource', 'RegexSource', 'WordCountIndexSource',
                                                                            'DictionarySource', 'FuzzyTokenSource', 'ScanSource'];

@Injectable()
export class StubAutocompleteService {

  private sourceTableNamesObservable = Observable.of(mockSourceTableAPIResponse)
                                          .map(response => AutocompleteUtils.processSourceTableAPIResponse(response))
                                          .shareReplay(1);

  constructor(private stubOperatorMetadataService: StubOperatorMetadataService) { }

  public getSourceTablesNamesObservable(): Observable<ReadonlyArray<string>> {
    return this.sourceTableNamesObservable;
  }

  public getSourceTableAddedOperatorMetadataObservable(): Observable<OperatorMetadata> {
    return  combineLatest(this.getSourceTablesNamesObservable(), this.stubOperatorMetadataService.getOperatorMetadata())
            .map(([tableNames, operatorMetadata]) => AutocompleteUtils.addSourceTableNamesToMetadata(operatorMetadata, tableNames));
  }
}
