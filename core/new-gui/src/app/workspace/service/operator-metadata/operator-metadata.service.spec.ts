import { TestBed, inject } from '@angular/core/testing';

import { HttpClient } from '@angular/common/http';
import { OperatorMetadataService } from './operator-metadata.service';

import { Observable } from 'rxjs/Observable';

import '../../../common/rxjs-operators';


class StubHttpClient {
  constructor() { }

  public get(url: string): Observable<any> {
    return Observable.of('test response');
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

  it ('should send http request once', () => {
    service.getOperatorMetadata().subscribe(
      value => expect(<any>value).toEqual('test response')
    );
  });

});
