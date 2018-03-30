import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { MOCK_OPERATOR_METADATA } from './mock-operator-metadata.data';
import { OperatorMetadata } from '../../types/operator-schema';

import '../../../common/rxjs-operators';

@Injectable()
export class StubOperatorMetadataService {

    public getOperatorMetadata(): Observable<OperatorMetadata> {
        return Observable.of(MOCK_OPERATOR_METADATA).shareReplay(1);
    }

    constructor() { }

}
