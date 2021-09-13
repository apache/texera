import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Subject } from "rxjs";
import { Observable } from "rxjs";
import { AppSettings } from "../../../common/app-setting";
import { GenericWebResponse } from "../../../common/type/generic-web-response";

import { UserDictionary } from "../../../common/type/user-dictionary";
import { UserService } from "../../../common/service/user/user.service";

export const USER_DICTIONARY_LIST_URL = "user/dictionary/list";
export const USER_DICTIONARY_DELETE_URL = "user/dictionary/delete";
export const USER_DICTIONARY_UPDATE_URL = "user/dictionary/update";

/**
 * User Dictionary service should be able to get all the saved-dictionary
 *  data from the back end for a specific user. The user can also upload new
 *  dictionary, view dictionaries, and edit the keys in a specific dictionary
 *  by calling methods in service.
 */

@Injectable()
export class UserDictionaryService {
  private userDictionaries: UserDictionary[] | undefined;
  private userDictionariesChanged = new Subject<ReadonlyArray<UserDictionary> | undefined>();

  constructor(private http: HttpClient, private userService: UserService) {
    this.detectUserChanges();
  }

  public getUserDictionaries(): ReadonlyArray<UserDictionary> | undefined {
    return this.userDictionaries;
  }

  public getUserDictionariesChangedEvent(): Observable<ReadonlyArray<UserDictionary> | undefined> {
    return this.userDictionariesChanged.asObservable();
  }

  /**
   * retrieve the files from the backend and store in the user-file service.
   * these file can be accessed by function {@link getDictionaryArray}.
   */
  public refreshDictionaries(): void {
    if (!this.userService.isLogin()) {
      return;
    }

    this.http
      .get<UserDictionary[]>(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_LIST_URL}`)
      .subscribe(dictionaries => {
        this.userDictionaries = dictionaries;
        this.userDictionariesChanged.next(this.userDictionaries);
      });
  }

  public deleteDictionary(dictID: number) {
    this.http
      .delete<GenericWebResponse>(`${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_DELETE_URL}/${dictID}`)
      .subscribe(() => this.refreshDictionaries());
  }

  /**
   * update the given dictionary in the backend and then refresh the dictionaries in the frontend
   * @param userDictionary
   */
  public updateDictionary(userDictionary: UserDictionary): void {
    this.http
      .put<GenericWebResponse>(
        `${AppSettings.getApiEndpoint()}/${USER_DICTIONARY_UPDATE_URL}`,
        JSON.stringify(userDictionary),
        {
          headers: new HttpHeaders({
            "Content-Type": "application/json",
          }),
        }
      )
      .subscribe(() => this.refreshDictionaries());
  }

  /**
   * refresh the dictionaries in the service whenever the user changes.
   */
  private detectUserChanges(): void {
    this.userService.userChanged().subscribe(() => {
      if (this.userService.isLogin()) {
        this.refreshDictionaries();
      } else {
        this.clearDictionary();
      }
    });
  }

  private clearDictionary(): void {
    this.userDictionaries = [];
    this.userDictionariesChanged.next(this.userDictionaries);
  }
}
