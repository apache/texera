import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

import { MOCK_USER_DICTIONARY_LIST } from './mock-user-dictionary.data';

const apiUrl = 'http://localhost:8080/api';

const uploadDictionaryUrl = apiUrl + '/upload/dictionary';

@Injectable()
export class StubUserDictionaryService {

  constructor(private http: Http) { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return Observable.of(MOCK_USER_DICTIONARY_LIST);
  }

  public addUserDictionaryData(addDict: UserDictionary): void {
    console.log('dict added');
  }

  public uploadDictionary(file: File) {
    const formData: FormData = new FormData();
    formData.append('file', file, file.name);
    this.http.post(uploadDictionaryUrl, formData, null)
      .subscribe(
        data => {
          alert(file.name + ' is uploaded');
          // after adding a new dictionary, refresh the list
          // this.getDictionaries();
        },
        err => {
            alert('Error occurred while uploading ' + file.name);
            console.log('Error occurred while uploading ' + file.name + '\nError message: ' + err);
        }
      );
  }

}
