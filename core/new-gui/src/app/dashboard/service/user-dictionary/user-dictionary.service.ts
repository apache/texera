import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import { UserDictionary } from './user-dictionary.interface';
import { environment } from '../../../../environments/environment';

const dictionaryUrl = 'users/dictionaries';
const uploadDictionaryUrl = 'users/dictionaries/upload-file';

const uploadFileListURL = 'assume it exist';
const uploadUserDictionaryURL = 'assume it exist';
export interface GenericWebResponse {
  code: number;
  message: string;
}

/**
 * User Dictionary service should be able to get all the saved-dictionary
 *  data from the back end for a specific user. The user can also upload new
 *  dictionary, view dictionaries, and edit the keys in a specific dictionary
 *  by calling methods in service. StubUserDictionaryService is used for replacing
 *  real service to complete testing cases. It uploads the mock data to the dashboard.
 *
 * @author Zhaomin Li
 */

@Injectable()
export class UserDictionaryService {

  constructor(private http: HttpClient) { }

  /**
   * This method will list all the dictionaries existing in the
   *  backend.
   */
  public listUserDictionaries(): Observable<UserDictionary[]> {
    return this.http.get<UserDictionary[]>(`${environment.apiUrl}/${dictionaryUrl}`);
  }

  /**
   * This method will get the user dictionary information using the
   *  dictionary ID.
   *
   * The information includes
   *  1. dictionary ID
   *  2. dictionary Name
   *  3. dictionary items
   *  4. dictionary description
   *
   * @param dictID
   */
  public getUserDictionary(dictID: string): Observable<UserDictionary> {
    return this.http.get<UserDictionary>(`${environment.apiUrl}/${dictionaryUrl}/${dictID}`);
  }

  /**
   * This method handles the request for uploading a user dictionary
   *  type object to the backend.
   *
   * @param userDict new user dictionary
   */
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

  public uploadFileList(fileList: File[]): Observable<GenericWebResponse> {
    const formDataList: FormData[] = [];
    fileList.forEach(file => {
      const formData: FormData = new FormData();
      formData.append('file', file, file.name);
      formDataList.push(formData);
    });
    return this.http.post<GenericWebResponse>(`${environment.apiUrl}/${uploadFileListURL}`, formDataList);
  }

  public uploadUserDictionary(dict: UserDictionary): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(`${environment.apiUrl}/${uploadUserDictionaryURL}`, dict);
  }

  /**
   * This method will handle the request for uploading a File type
   *  dictionary object.
   *
   * @param file
   */
  public uploadDictionary(file: File): Observable<GenericWebResponse> {
    const formData: FormData = new FormData();
    formData.append('file', file, file.name);

    return this.http.post<GenericWebResponse>(`${environment.apiUrl}/${uploadDictionaryUrl}`, formData);
  }

  /**
   * This method will send a request to the backend
   *  to remove the dictionary information stored
   *  in the disk.
   *
   * @param dictID dictionary ID
   */
  public deleteUserDictionaryData(dictID: string): Observable<GenericWebResponse> {
    return this.http.delete<GenericWebResponse>(`${environment.apiUrl}/${dictionaryUrl}/${dictID}`);
  }

}
