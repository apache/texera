import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import '../../../common/rxjs-operators';

import { AppSettings } from '../../../common/app-setting';

import { OperatorMetadata } from '../../types/operator-schema';

export const OPERATOR_METADATA_ENDPOINT = 'resources/operator-metadata';

export const EMPTY_OPERATOR_METADATA: OperatorMetadata = {operators: [], groups: []};

/**
 * OperatorMetadataService talks to the backend to fetch the operator metadata,
 *  which contains a list of operator schemas.
 * Each operator schema contains all the information related to an operator,
 *  for example, operatorType, userFriendlyName, and the jsonSchema of its properties.
 *
 * fetchAllOperatorMetadata is called in ngOnInit function of the root WorkspaceComponent,
 *  to initiate the HTTP request to get the data.
 *
 * Components and Services should subscribe to the metadataChanged Observable in order to to get the metadata,
 *  once the metadata is fetched from the backend, it will be broadcasted through the observable.
 * Other components should not call fetchAllOperatorMetadata, to unnecessarily avoid fetching it twice.
 *
 * The mock operator metadata is also available in mock-operator-metadata.ts for testing. It contains schema for 3 single operators.
 *
 * @author Zuozhi Wang
 *
 */
@Injectable()
export class OperatorMetadataService {

  constructor(private httpClient: HttpClient) { }

  /**
   * Emit an empty OperatorMetadata first.
   * Send a http request immediately, after the response is received,
   *   emit the OperatorMetadata Object after it's fetched from the backend.
   *
   * This observable is shared among subscribers with replay buffer size 1.
   *
   * Marble diagram:
   *
   * url------------>
   *  http
   * ------res------>
   *  startWith
   * E-----res------>
   * (E stands for empty OperatorMetadata)
   *
   */
  public operatorMetadataObservable = this.httpClient
    .get<OperatorMetadata>(`${AppSettings.API_ENDPOINT}/${OPERATOR_METADATA_ENDPOINT}`)
    .startWith(EMPTY_OPERATOR_METADATA)
    .shareReplay(1);

}
