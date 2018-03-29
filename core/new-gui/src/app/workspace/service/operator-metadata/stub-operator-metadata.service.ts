import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { OPERATOR_METADATA } from './mock-operator-metadata.data';
import { OperatorMetadata } from '../../types/operator-schema';

import '../../../common/rxjs-operators';

import { EMPTY_OPERATOR_METADATA } from './operator-metadata.service';


@Injectable()
export class StubOperatorMetadataService {

    public operatorMetadataObservable: Observable<OperatorMetadata> =
        Observable.of( OPERATOR_METADATA )
        .startWith(EMPTY_OPERATOR_METADATA)
        .shareReplay(1);

    constructor() { }

}
