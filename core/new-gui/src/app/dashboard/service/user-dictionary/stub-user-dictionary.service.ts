import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

import { MOCK_USER_DICTIONARY_LIST } from './mock-user-dictionary.data';

@Injectable()
export class StubUserDictionaryService {

  constructor() { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return Observable.of(MOCK_USER_DICTIONARY_LIST);
  }

}
