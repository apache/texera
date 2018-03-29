import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { OPERATOR_METADATA } from './mock-operator-metadata.data';
import { OperatorMetadata } from '../../types/operator-schema';

import '../../../common/rxjs-operators';


@Injectable()
export class StubOperatorMetadataService {

    public operatorMetadataObservable: Observable<OperatorMetadata> = Observable.of( OPERATOR_METADATA );

    constructor() { }

}
