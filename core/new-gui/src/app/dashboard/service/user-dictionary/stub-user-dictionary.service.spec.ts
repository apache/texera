import { TestBed, inject } from '@angular/core/testing';

import { StubUserDictionaryService } from './stub-user-dictionary.service';

import { Observable } from 'rxjs/Observable';
import { HttpClient } from '@angular/common/http';

class StubHttpClient {
  constructor() { }

  public post(): Observable<string> { return Observable.of('a'); }
}

describe('StubUserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        StubUserDictionaryService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });
  });

  it('should be created', inject([StubUserDictionaryService], (service: StubUserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
