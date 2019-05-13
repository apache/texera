import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import { UserDictionary } from './user-dictionary.interface';
import { environment } from '../../../../environments/environment';

const dictionaryUrl = 'users/dictionaries';
const uploadDictionaryUrl = 'users/dictionaries/upload-file';

export interface GenericWebResponse {
  code: number;
  message: string;
}

/**
 * User Dictionary service should be able to get all the saved-dictionary
 * data from the back end for a specific user. The user can also upload new
 * dictionary, view dictionaries, and edit the keys in a specific dictionary
 * by calling methods in service. StubUserDictionaryService is used for replacing
 * real service to complete testing cases. It uploads the mock data to the dashboard.
 *
 *
 * @author Zhaomin Li
 */

@Injectable()
export class UserDictionaryService {

  constructor(private http: HttpClient) { }

  public listUserDictionaries(): Observable<UserDictionary[]> {
    return this.http.get<UserDictionary[]>(`${environment.apiUrl}/${dictionaryUrl}`);
  }

  public getUserDictionary(dictID: string): Observable<UserDictionary> {
    return this.http.get<UserDictionary>(`${environment.apiUrl}/${dictionaryUrl}/${dictID}`);
  }

  public putUserDictionaryData(userDict: UserDictionary): Observable<GenericWebResponse> {
    return this.http.put<GenericWebResponse>(
      `${environment.apiUrl}/${dictionaryUrl}/${userDict.id}`,
      JSON.stringify(userDict),
      {
        headers: new HttpHeaders({
          'Content-Type':  'application/json',
        })
      }
    );
  }

  public uploadDictionary(file: File): Observable<GenericWebResponse> {
    const formData: FormData = new FormData();
    formData.append('file', file, file.name);

    return this.http.post<GenericWebResponse>(`${environment.apiUrl}/${uploadDictionaryUrl}`, formData);
  }

  public deleteUserDictionaryData(dictID: string): Observable<GenericWebResponse> {
    return this.http.delete<GenericWebResponse>(`${environment.apiUrl}/${dictionaryUrl}/${dictID}`);
  }

}
