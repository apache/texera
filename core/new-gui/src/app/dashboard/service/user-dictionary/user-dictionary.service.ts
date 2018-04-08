import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

@Injectable()
export class UserDictionaryService {

  constructor() { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return null;
  }

}
