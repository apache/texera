import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable, of, Subject } from "rxjs";
import { AppSettings } from "src/app/common/app-setting";
import { UserService } from "../../../common/service/user/user.service";
import { shareReplay, tap } from "rxjs/operators";

export type UserDictionary = {
  [key: string]: string;
};

@Injectable({
  providedIn: "root",
})
export class DictionaryService {
  public static readonly USER_DICTIONARY_ENDPOINT = "users/dictionary";

  private dictionaryChangedSubject = new Subject<void>();
  private localUserDictionary: UserDictionary = {};

  constructor(private http: HttpClient, private userService: UserService) {
    if (this.userService.isLogin()) {
      this.fetchAll();
    }
    this.userService.userChanged().subscribe(() => {
      if (this.userService.isLogin()) {
        this.fetchAll();
      } else {
        this.updateDict({});
      }
    });
  }

  public getDict(): Readonly<UserDictionary> {
    return this.localUserDictionary;
  }

  /**
   * get a value from the backend.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns string value corresponding to the key from the backend;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public fetchKey(key: string): Observable<string> {
    if (!this.userService.isLogin()) {
      throw new Error("user not logged in");
    }
    if (key.trim().length === 0) {
      throw new Error("Dictionary Service: key cannot be empty");
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.get<string>(url).pipe(
      tap(res => this.updateEntry(key, res)),
      shareReplay(1)
    );
    req.subscribe(); // causes post request to be sent regardless caller's subscription
    return req;
  }

  /**
   * get the entire dictionary from the backend.
   * @returns UserDictionary object with string attributes;
   */
  public fetchAll(): Observable<Readonly<UserDictionary>> {
    if (!this.userService.isLogin()) {
      throw new Error("user not logged in");
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
    const req = this.http.get<UserDictionary>(url).pipe(
      tap(res => this.updateDict(res)),
      shareReplay(1)
    );
    req.subscribe(); // causes post request to be sent regardless caller's subscription
    return req;
  }

  /**
   * saves or updates (if it already exists) an entry (key-value pair) on the backend.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @param value string value corresponding to the key from the backend
   * @returns observable indicating the backend has been successfully updated
   */
  public set(key: string, value: string): Observable<void> {
    if (!this.userService.isLogin()) {
      throw new Error("user not logged in");
    }
    if (key.trim().length === 0) {
      throw new Error("Dictionary Service: key cannot be empty");
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.put<void>(url, { value: value }).pipe(
      tap(_ => this.updateEntry(key, value)),
      shareReplay(1)
    );
    req.subscribe();
    return req;
  }

  /**
   * delete a value from the backend.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns observable indicating the backend has been successfully updated
   */
  public delete(key: string): Observable<void> {
    if (!this.userService.isLogin()) {
      throw new Error("user not logged in");
    }
    if (key.trim().length === 0) {
      throw new Error("Dictionary Service: key cannot be empty");
    }
    if (!(key in this.localUserDictionary)) {
      return of();
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.delete<void>(url).pipe(
      tap(_ => this.updateEntry(key, undefined)),
      shareReplay(1)
    );
    req.subscribe();
    return req;
  }

  private updateEntry(key: string, value: string | undefined) {
    if (key.trim().length === 0) {
      throw new Error("Dictionary Service: key cannot be empty");
    }
    if (value === undefined) {
      if (key in this.localUserDictionary) {
        delete this.localUserDictionary[key];
        this.dictionaryChangedSubject.next();
      }
    } else {
      if (this.localUserDictionary[key] !== value) {
        this.localUserDictionary[key] = value;
        this.dictionaryChangedSubject.next();
      }
    }
  }

  private updateDict(newDict: UserDictionary) {
    this.localUserDictionary = newDict;
    this.dictionaryChangedSubject.next();
  }
}
