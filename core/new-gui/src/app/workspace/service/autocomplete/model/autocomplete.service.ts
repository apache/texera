import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { AppSettings } from '../../../../common/app-setting';
import { SourceTableNamesAPIResponse } from '../../../types/source-table-names.interface';

import { AutocompleteUtils } from '../util/autocomplete-util.service';
import { OperatorMetadata, OperatorSchema } from '../../../types/operator-schema.interface';
import { OperatorMetadataService } from '../../../service/operator-metadata/operator-metadata.service';
import { Observable } from 'rxjs/Observable';
import '../../../../common/rxjs-operators';

import { combineLatest } from 'rxjs/observable/combineLatest';

export const SOURCE_TABLE_NAMES_ENDPOINT = 'resources/table-metadata';

/**
 * Autocomplete service is used for the purpose of automated schema propagation. The service does
 * following tasks: -
 *  1. Originally the source operators have tableName property which has no options. This service fetches
 *    source table names from backend and adds them as an enum array to the tableName property.
 *  2. (To be added as class develops)
 */
@Injectable()
export class AutocompleteService {
  private sourceTableAddedOperatorMetadata: OperatorMetadata | undefined;

  private sourceTableNamesObservable = this.httpClient
  .get<SourceTableNamesAPIResponse>(`${AppSettings.getApiEndpoint()}/${SOURCE_TABLE_NAMES_ENDPOINT}`)
  .map(response => AutocompleteUtils.processSourceTableAPIResponse(response))
  .shareReplay(1);

  constructor(private httpClient: HttpClient,
              private operatorMetadataService: OperatorMetadataService) {

    this.getSourceTableAddedOperatorMetadataObservable().subscribe(
      metadata => { this.sourceTableAddedOperatorMetadata = metadata; }
    );
  }

  public getSourceTablesNamesObservable(): Observable<ReadonlyArray<string>> {
    return this.sourceTableNamesObservable;
  }

  /**
   * This function returns the observable for operatorMetadata modified using the function addSourceTableNamesToMetadata. An important
   * part to note here is that the function uses an combineLatest which fires first when it receives one value from each observable
   * and thereafter a new value from any observable leads to refiring of the observables with the latest value fired by all the observables.
   * Thus, any new value in either of the observables will cause recomputation of the modifed operator schema.
   */
  public getSourceTableAddedOperatorMetadataObservable(): Observable<OperatorMetadata> {
    return  combineLatest(this.getSourceTablesNamesObservable(), this.operatorMetadataService.getOperatorMetadata())
            .map(([tableNames, operatorMetadata]) =>
            AutocompleteUtils.addSourceTableNamesToMetadata(operatorMetadata, tableNames));
  }

}
