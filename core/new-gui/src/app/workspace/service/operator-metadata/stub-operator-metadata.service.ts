import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { MOCK_OPERATOR_METADATA } from './mock-operator-metadata.data';
import { OperatorMetadata } from '../../types/operator-schema';

import '../../../common/rxjs-operators';
import { EMPTY_OPERATOR_METADATA } from './operator-metadata.service';

@Injectable()
export class StubOperatorMetadataService {

  private operatorMetadataObservable = Observable
    .of(MOCK_OPERATOR_METADATA)
    .shareReplay(1);

  public getOperatorMetadata(): Observable<OperatorMetadata> {
    return this.operatorMetadataObservable;
  }

  constructor() { }

}
