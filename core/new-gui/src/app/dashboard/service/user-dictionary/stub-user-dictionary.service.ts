import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

import { MOCK_USER_DICTIONARY_LIST } from './mock-user-dictionary.data';

@Injectable()
export class StubUserDictionaryService {

  constructor(private http: Http) { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return Observable.of(MOCK_USER_DICTIONARY_LIST);
  }

  public addUserDictionaryData(addDict: UserDictionary): void {
    console.log('dict added');
  }

}
