import { TestBed, inject } from '@angular/core/testing';

import { UserDictionaryService } from './user-dictionary.service';

import { Observable } from 'rxjs/Observable';
import { HttpClient } from '@angular/common/http';
import { marbles} from 'rxjs-marbles';

class StubHttpClient {
  constructor() { }

  public post(): Observable<string> { return Observable.of('a'); }
  public get(): Observable<string> { return Observable.of('a'); }
}

describe('UserDictionaryService', () => {

  let service: UserDictionaryService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UserDictionaryService,
        { provide: HttpClient, useClass: StubHttpClient }
      ]
    });

    service = TestBed.get(UserDictionaryService);
  });

  it('should be created', inject([UserDictionaryService], (InjectableService: UserDictionaryService) => {
    expect(InjectableService).toBeTruthy();
  }));
});
