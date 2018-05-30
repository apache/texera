import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { OperatorMetadataService, EMPTY_OPERATOR_METADATA } from './operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../common/rxjs-operators';
import { getMockOperatorMetaData } from './mock-operator-metadata.data';


class StubHttpClient {
  constructor() { }

  // fake an async http response with a very small delay
  public get(url: string): Observable<any> {
    return Observable.of(getMockOperatorMetaData()).delay(1);
  }
}

describe('OperatorMetadataService', () => {

  let service: OperatorMetadataService;
  let stubHttp: StubHttpClient;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        OperatorMetadataService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });
    stubHttp = TestBed.get(HttpClient);
  });

  beforeEach(inject([OperatorMetadataService, HttpClient], (ser: OperatorMetadataService) => {
    service = ser;
  }));

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should emit an empty operator metadata first', () => {
    service.getOperatorMetadata().first().subscribe(
      value => expect(<any>value).toEqual(EMPTY_OPERATOR_METADATA)
    );
  });

  it('should send http request once', () => {
    service.getOperatorMetadata().last().subscribe(
      value => expect(<any>value).toBeTruthy()
    );
  });

  it('should check if operatorType exists correctly', () => {
    service.getOperatorMetadata().last().subscribe(
      value => {
        expect(service.operatorTypeExists('ScanSource')).toBeTruthy();
        expect(service.operatorTypeExists('InvalidOperatorType')).toBeFalsy();
      }
    );
  });

});
