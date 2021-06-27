import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';
import { AppSettings } from 'src/app/common/app-setting';
import { UserService } from '../user.service';


export type UserDictionary = {
  [key: string]: string
};

/**
 * This error is only thrown if attempting to get a dictionary proxy before initialization is complete
 */
export class NotReadyError extends Error {
  constructor(message?: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'NotReadyError';
  }
}

@Injectable({
  providedIn: 'root'
})
export class DictionaryService {

  public static readonly USER_DICTIONARY_ENDPOINT = 'users/dictionary';

  private dictionaryChangedSubject = new Subject<void>();
  private localUserDictionary: UserDictionary = {};

  constructor(private http: HttpClient, private userService: UserService) {
    if (this.userService.isLogin()) {
      this.fetchAll();
    }
    this.userService.userChanged().subscribe(
      () => {
        if (this.userService.isLogin()) {
          this.fetchAll();
        } else {
          this.updateDict({});
        }
      }
    );
  }

  public getDict(): Readonly<UserDictionary> {
    return this.localUserDictionary;
  }

  /**
   * get a value from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns string value corresponding to the key from the backend;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public fetchKey(key: string): Observable<string> {
    if (key.trim().length === 0) {
      throw new Error('Dictionary Service: key cannot be empty');
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.get<string>(url).do(res => this.updateEntry(key, res)).shareReplay(1);
    req.subscribe(); // causes post request to be sent regardless caller's subscription
    return req;
  }

  /**
   * get the entire dictionary from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * @returns UserDictionary object with string attributes;
   * Throws Error("Invalid Session") (not logged in)
   */
  public fetchAll(): Observable<Readonly<UserDictionary>> {
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}`;
    const req = this.http.get<UserDictionary>(url)
      .do(res => this.updateDict(res))
      .shareReplay(1);
    req.subscribe(); // causes post request to be sent regardless caller's subscription
    return req;
  }

  /**
   * saves or updates (if it already exists) an entry (key-value pair) on the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @param value string value corresponding to the key from the backend
   * @returns true if operation succeeded;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public set(key: string, value: string): Observable<void> {
    if (key.trim().length === 0) {
      throw new Error('Dictionary Service: key cannot be empty');
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.put<void>(url, { value: value })
      .do(_ => this.updateEntry(key, value))
      .shareReplay(1);
    req.subscribe();
    return req;
  }

  /**
   * delete a value from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns true if entry was deleted successfully;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public delete(key: string): Observable<void> {
    if (key.trim().length === 0) {
      throw new Error('Dictionary Service: key cannot be empty');
    }
    if (! (key in this.localUserDictionary)) {
      return Observable.of();
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/${key}`;
    const req = this.http.delete<void>(url)
      .do(_ => this.updateEntry(key, undefined))
      .shareReplay(1);
    req.subscribe();
    return req;
  }

  private updateEntry(key: string, value: string | undefined) {
    if (key.trim().length === 0) {
      throw new Error('Dictionary Service: key cannot be empty');
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
