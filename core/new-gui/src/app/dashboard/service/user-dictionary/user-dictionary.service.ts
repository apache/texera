import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import { UserDictionary } from '../../type/user-dictionary';

const apiUrl = 'http://localhost:8080/api';

const uploadDictionaryUrl = apiUrl + '/upload/dictionary';

@Injectable()
export class UserDictionaryService {

  private saveStartedStream = new Subject<string>();

  constructor(private http: HttpClient) { }

  public getUserDictionaryData(): Observable<UserDictionary[]> {
    return Observable.of([]);
  }

  public addUserDictionaryData(addDict: UserDictionary): void {
    console.log('dict added');
  }

  public uploadDictionary(file: File) {
    const formData: FormData = new FormData();
    formData.append('file', file, file.name);

    this.saveStartedStream.next('start to upload dictionary');

    this.http.post(uploadDictionaryUrl, formData, undefined)
      .subscribe(
        data => {
          alert(file.name + ' is uploaded');
        },
        err => {
            alert('Error occurred while uploading ' + file.name);
            console.log('Error occurred while uploading ' + file.name + '\nError message: ' + err);
        }
      );
  }

  public getUploadDictionary(): Observable<string> {
    return this.saveStartedStream.asObservable();
  }

  public deleteUserDictionaryData(deleteDictionary: UserDictionary) {
    // console.log('delete: ', deleteDictionary.id.toString());
    return null;
  }

}
