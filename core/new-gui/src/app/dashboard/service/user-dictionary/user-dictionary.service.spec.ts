import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryService } from './user-dictionary.service';

import { Observable } from 'rxjs/Observable';
import { HttpClient } from '@angular/common/http';

class StubHttpClient {
  constructor() { }

  public post(): Observable<string> { return Observable.of('a'); }
}

describe('UserDictionaryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UserDictionaryService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });
  });

  it('should be created', inject([UserDictionaryService], (service: UserDictionaryService) => {
    expect(service).toBeTruthy();
  }));
});
