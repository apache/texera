import { TestBed, inject } from '@angular/core/testing';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

import { HttpClient } from '@angular/common/http';
import { OperatorMetadataService } from './operator-metadata.service';

import 'rxjs/add/operator/startWith';


class StubHttpClient {
  constructor() { }

  public get(url: string): Observable<any> {
    return Observable.of('response');
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
    spyOn(stubHttp, 'get');
  });

  beforeEach(inject([OperatorMetadataService], (ser: OperatorMetadataService) => {
    service = ser;
  }));

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it ('should send http request once', () => {
    expect(stubHttp.get).toHaveBeenCalledTimes(1);
  });

});
