import { Injectable, EventEmitter } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../../type/user-dictionary';
import { environment } from '../../../../../environments/environment';
import { GenericWebResponse } from '../../../../dashboard/type/generic-web-response';
import { UserService } from '../user.service';
import { User } from '../../../type/user';

const getDictionaryUrl = 'user/dictionary/list';
const deleteDictionaryUrl = 'user/dictionary/delete';
const updateDictionaryUrl = 'user/dictionary/update';

/**
 * User Dictionary service should be able to get all the saved-dictionary
 *  data from the back end for a specific user. The user can also upload new
 *  dictionary, view dictionaries, and edit the keys in a specific dictionary
 *  by calling methods in service. StubUserDictionaryService is used for replacing
 *  real service to complete testing cases. It uploads the mock data to the dashboard.
 *
 * @author Chen He
 */

@Injectable()
export class UserDictionaryService {
  private dictionaryArray: UserDictionary[] = [];

  constructor(
    private http: HttpClient,
    private userService: UserService) {
      this.detectUserChanges();
      if (this.userService.isLogin()) {
        this.refreshDictionary();
      }
    }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getDictionaryArray}.
   */
  public refreshDictionary(): void {
    if (!this.userService.isLogin()) {return; }

    this.getDictionaryHttpRequest(
      (this.userService.getUser() as User).userID
      ).subscribe(
      dictionaries => this.dictionaryArray = dictionaries
    );
  }

  public deleteDictionary(dictID: number) {
    this.deleteDictionaryHttpRequest(dictID).subscribe(
      () => this.refreshDictionary()
    );
  }

  /**
   * update the given dictionary in the backend and then refresh the dictionaries in the frontend
   * @param userDictionary
   */
  public updateDictionary(userDictionary: UserDictionary): void {
    this.updateDictionaryHttpRequest(userDictionary)
      .subscribe(
        () => this.refreshDictionary()
      );
  }

  /**
   * this function will return the dictionaryArray store in the service.
   * This is required for HTML page since HTML can only loop through collection instead of index number.
   * Be carefully with the return array because it may cause unexpected error.
   * You can change the UserDictionary inside the array but do not change the array itself.
   */
  public getDictionaryArray(): UserDictionary[] {
    return this.dictionaryArray;
  }

  public getDictionaryArrayLength(): number {
    return this.dictionaryArray.length;
  }

  private getDictionaryHttpRequest(userID: number): Observable<UserDictionary[]> {
    return this.http.get<UserDictionary[]>(`${environment.apiUrl}/${getDictionaryUrl}/${userID}`);
  }

  private deleteDictionaryHttpRequest(dictID: number): Observable<GenericWebResponse> {
    return this.http.delete<GenericWebResponse>(`${environment.apiUrl}/${deleteDictionaryUrl}/${dictID}`);
  }

  private updateDictionaryHttpRequest(userDictionary: UserDictionary): Observable<GenericWebResponse> {
    return this.http.post<GenericWebResponse>(`${environment.apiUrl}/${updateDictionaryUrl}`,
    JSON.stringify(userDictionary),
    {
      headers: new HttpHeaders({
        'Content-Type':  'application/json',
      })
    }
    );
  }

  /**
   * refresh the dictionaries in the service whenever the user changes.
   */
  private detectUserChanges(): void {
    this.userService.getUserChangedEvent().subscribe(
      () => {
        if (this.userService.isLogin()) {
          this.refreshDictionary();
        } else {
          this.clearDictionary();
        }
      }
    );
  }

  private clearDictionary(): void {
    this.dictionaryArray = [];
  }

}
