import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import { AppSettings } from '../../../common/app-setting';
import { OperatorMetadata } from '../../types/operator-schema.interface';

export const MOCK_OPERATOR_METADATA_ENDPOINT = 'resources/operator-metadata';

export const EMPTY_OPERATOR_METADATA: OperatorMetadata = {
  operators: [],
  groups: []
};

/**
 * OperatorMetadataService talks to the backend to fetch the operator metadata,
 *  which contains a list of operator schemas.
 * Each operator schema contains all the information related to an operator,
 *  for example, operatorType, userFriendlyName, and the jsonSchema of its properties.
 *
 *
 * Components and Services should call getOperatorMetadata() and subscribe to the Observable in order to to get the metadata,
 *  an empty operator metadata will be broadcasted before the metadata is fetched,
 *  after the metadata is fetched from the backend, it will be broadcasted through the observable.
 *
 * The mock operator metadata is also available in mock-operator-metadata.ts for testing. It contains schema for 3 single operators.
 *
 * @author Zuozhi Wang
 *
 */
@Injectable()
export class OperatorMetadataService {

  // holds the current version of operator metadata
  private currentOperatorMetadata: OperatorMetadata | undefined;

  private operatorMetadataObservable = this.httpClient
    .get<OperatorMetadata>(`${AppSettings.getApiEndpoint()}/${MOCK_OPERATOR_METADATA_ENDPOINT}`)
    .startWith(EMPTY_OPERATOR_METADATA)
    .shareReplay(1);

  constructor(private httpClient: HttpClient) {
    this.getOperatorMetadata().subscribe(
      data => this.currentOperatorMetadata = data
    );
  }

  /**
   * Gets an Observable for operator metadata.
   * This observable will emit OperatorMetadataValue after the data is fetched from the backend.
   *
   * Upon subscription of this observable, if the data hasn't arrived from the backend,
   *   you will receive an empty OperatorMetadata.
   */
  public getOperatorMetadata(): Observable<OperatorMetadata> {
    return this.operatorMetadataObservable;
  }

  /**
   * Returns if the operator type exists *in the current operator metadata*.
   * For example, if the first HTTP request to the backend hasn't returned yet,
   *  the current operator metadata is empty, and no operator type exists.
   *
   * @param operatorType
   */
  public operatorTypeExists(operatorType: string): boolean {
    if (! this.currentOperatorMetadata) {
      return false;
    }
    const operator = this.currentOperatorMetadata.operators.filter(op => op.operatorType === operatorType);
    if (operator.length === 0) {
      return false;
    }
    return true;
  }

}
