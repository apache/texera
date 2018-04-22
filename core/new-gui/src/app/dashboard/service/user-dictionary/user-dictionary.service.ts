import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

@Injectable()
export class UserDictionaryService {

  constructor(private http: Http) { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return null;
  }

  public addUserDictionaryData(addDict: UserDictionary): void {
    console.log('dict added');
  }

}
