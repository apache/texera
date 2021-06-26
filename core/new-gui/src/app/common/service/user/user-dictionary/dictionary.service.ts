import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { from, Observable, Subject } from 'rxjs';
import * as Ajv from 'ajv';
import { AppSettings } from 'src/app/common/app-setting';
import { JSONSchema7 } from 'json-schema';
import { catchError, map, shareReplay } from 'rxjs/operators';
import { isEqual } from 'lodash';


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

  private dictionaryEventSubject = new Subject<USER_DICT_EVENT>();
  private localUserDictionary: UserDictionary = {}; // asynchronously initialized after construction (see initLocalDict)
  private ready: { promise: Promise<boolean>, value: boolean } = {promise: Promise.resolve(false), value: false};

  constructor(private http: HttpClient) {
    this.initLocalDict();
    this.handleDictionaryEventStream();
  }

  /**
   * get a value from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns string value corresponding to the key from the backend;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public get(key: string): Observable<string> {
    if (key.trim().length === 0) {
      throw new Error('Dictionary Service: key cannot be empty');
    }
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get/${key}`;
    return this.http.get<string>(url);
  }

  /**
   * get the entire dictionary from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * @returns UserDictionary object with string attributes;
   * Throws Error("Invalid Session") (not logged in)
   */
  public getAll(): Observable<Readonly<UserDictionary>> {
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/get`;
    const request: GetAllRequest = {requestType: GET_REQUEST_TYPE.GET_DICT};
    const result = this.http.post<UserDictionaryResponse>(url, request)
      .pipe(
        map(this.handleDictResponse.bind(this)),
        catchError(DictionaryService.handleErrorResponse),
        shareReplay());

    result.subscribe(); // causes post request to actually be sent (see cold observables)
    return result;
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
  public set(key: string, value: string): Observable<boolean> {
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/set`;
    const request: SetEntryRequest = {key: key, value: value};
    const result = this.http.post<UserDictionaryResponse>(url, request)
      .pipe(
        map(this.handleSetConfirmationResponse.bind(this, key, value)),
        catchError(DictionaryService.handleErrorResponse),
        shareReplay());

    result.subscribe(); // causes post request to actually be sent (see cold observables)
    return result;
  }

  /**
   * delete a value from the backend.
   * Also produces dictionary events and updates dictionary proxy objects as a side effect.
   * keys and values must be strings.
   * @param key string key that uniquely identifies a value
   * @returns true if entry was deleted successfully;
   * throws Error("No such entry") (invalid key) or Error("Invalid session") (not logged in).
   */
  public delete(key: string): Observable<boolean> {
    const url = `${AppSettings.getApiEndpoint()}/${DictionaryService.USER_DICTIONARY_ENDPOINT}/delete`;
    const request: DeleteEntryRequest = {key: key};
    const result = this.http.request<UserDictionaryResponse>('DELETE', url, {body: request})
      .pipe(
        map(this.handleDeleteConfirmationResponse.bind(this, key)),
        catchError(DictionaryService.handleErrorResponse),
        shareReplay());

    result.subscribe(); // causes post request to actually be sent (see cold observables)
    return result;
  }

  /**
   * Dictionary events are created every time a get/getAll/set/delete operation returns sucessfully
   * @returns USER_DICT_EVENTs, see type definition
   */
  public dictionaryEventStream(): Observable<USER_DICT_EVENT> {
    return this.dictionaryEventSubject.asObservable();
  }

  /**
   * get a proxy dictionary: access a local copy of the backend dictionary as an object *synchronously*
   * without callbacks or subscriptions.
   * All instances of proxy dictionaries are synced (changing one affects the others).
   * assignment and deletion of attributes cause requests to be sent to the backend
   * and the proxy dictionary only changes if those requests are successful.
   * *Warning: proxy dictionaries are synced w/ backend each getAll() and should be correct 99.99% of the time.
   * However they may fall out of sync due to unforseen networking errors.*
   * @returns Proxy of a local copy, Throws NotReadyError if initial getAll() request hasn't finished
   */
  public getUserDictionary(): UserDictionary {
    if (!this.ready.value) { throw new NotReadyError('incomplete initialization of user-dictionary service'); }
    return this.proxyUserDictionary();
  }

  /**
   * get a proxy dictionary: access a local copy of the backend dictionary as an object *synchronously*
   * without callbacks or subscriptions.
   * All instances of proxy dictionaries are synced (changing one affects the others).
   * assignment and deletion of attributes cause requests to be sent to the backend
   * and the proxy dictionary only changes if those requests are successful.
   * *Warning: proxy dictionaries are synced w/ backend each getAll() and should be correct 99.99% of the time.
   * However they may fall out of sync due to unforeseen networking errors.*
   * @returns Proxy of a local copy
   */
  public forceGetUserDictionary(): UserDictionary {
    // gets user dictionary even if local dictionary isn't initialized
    return this.proxyUserDictionary();
  }

  /**
   * get a proxy dictionary: access a local copy of the backend dictionary as an object *synchronously*
   * without callbacks or subscriptions.
   * All instances of proxy dictionaries are synced (changing one affects the others).
   * assignment and deletion of attributes cause requests to be sent to the backend
   * and the proxy dictionary only changes if those requests are successful.
   * *Warning: proxy dictionaries are synced w/ backend each getAll() and should be correct 99.99% of the time.
   * However they may fall out of sync due to unforseen networking errors.*
   * @returns Proxy of a local copy once initial getAll() is finished
   */
  public getUserDictionaryAsync(): Observable<UserDictionary> {
    return from(this.ready.promise.then(() => this.getUserDictionary()));
  }

  /**
   * initializes proxy dictionaries by calling getAll() and then setting dictionaryService.ready state
   */
  private initLocalDict() {
    let resolveReady: (read: boolean) => void;

    this.ready = {promise: new Promise((resolver) => resolveReady = resolver), value: false};

    // getAll automatically creates a dictionaryEvent that updates the localdict with the remote
    this.getAll().subscribe({
      next: () => {
        this.ready.value = true;
        resolveReady(true);
      },
      error: (error: Error) => {
        // user not logged in - print error but continue
        if (error.message === 'Invalid session') {
          console.error(error);
        } else {
          throw error;
        }
      }
    });

  }

  /**
   * subscribes to dictionaryEventStream and updates proxy dictionaries with each dictionary event
   * warnings are printed when the local copy is found to be different from the backend
   */
  private handleDictionaryEventStream() {
    this.dictionaryEventStream().subscribe(event => {
      switch (event.type) {
        case EVENT_TYPE.GET:
          if (event.key in this.localUserDictionary &&
            this.localUserDictionary[event.key] !== event.value) {
            console.warn(`[user-dictionary service] Dictionary desynchronized at key "${event.key}": `
              + `locally had ${this.localUserDictionary[event.key]} but remote reported  ${event.value}`);
          }
          this.localUserDictionary[event.key] = event.value;
          break;

        case EVENT_TYPE.SET:
          this.localUserDictionary[event.key] = event.value;
          break;

        case EVENT_TYPE.DELETE:
          delete this.localUserDictionary[event.key];
          break;

        case EVENT_TYPE.GET_ALL:
          if (!isEqual(this.localUserDictionary, event.value) && !isEqual(this.localUserDictionary, {})) {
            console.warn(`[user-dictionary service] Dictionary was desynchronized, local had `
              + `${this.localUserDictionary}, but remote reported ${event.value}`);
          }
          // setting this.localUserDictionary = event.value would not
          // change the references to this.localUserDictionary used in all proxy dictionaries
          for (const key in this.localUserDictionary) {
            delete this.localUserDictionary[key];
          }
          Object.assign(this.localUserDictionary, event.value);
      }
    });
  }

  /**
   * create a proxy dictionary
   * @returns UserDictionary
   */
  private proxyUserDictionary(): UserDictionary {
    return new Proxy<UserDictionary>(this.localUserDictionary, this.generateProxyHandler());
  }

  /**
   * create a proxy handler that converts attribute assignment and deletion operations
   * on a proxy object to calls to set() and delete()
   * @returns ProxyHandler<UserDictionary>
   */
  private generateProxyHandler(): ProxyHandler<UserDictionary> {
    const dictionaryService = this;
    return {
      set(localUserDictionary: Readonly<UserDictionary>, key: string, value: string) {
        console.log('set', key, value);
        dictionaryService.set(key, value);
        return true;
      },
      deleteProperty(localUserDictionary: Readonly<UserDictionary>, key: string) {
        dictionaryService.delete(key);
        return true;
      },
      defineProperty(localUserDictionary: Readonly<UserDictionary>, key: string, attributes: PropertyDescriptor) {
        console.log('set', key, attributes.value);
        dictionaryService.set(key, attributes.value);
        return true;
      }
    };
  }

  /**
   * Validate and parse UserDictionaryResponses that are expected to be EntryResponses (created by get()).
   * if response is an EntryResponse, return string result and create GET dictionary event.
   * otherwise detect that server response is malformed and throw TypeError
   * @param key string key that identifies value
   * @param response UserDictionaryResponse that should be an EntryResponse
   * @returns string value corresponding to requested entry
   */
  private handleEntryResponse(key: string, response: UserDictionaryResponse): string {
    if (DictionaryService.isEntryResponse(response)) {
      this.dictionaryEventSubject.next(<GET_EVENT>{type: EVENT_TYPE.GET, key: key, value: response.result});
      return response.result;
    }
    throw new TypeError(`expected EntryResponse but server replied with: ${response}`);
  }

  /**
   * Validate and parse UserDictionaryResponses that are expected to be DictResponses (created by getAll()).
   * if response is a DictResponse, return UserDictionary result and create GET_ALL dictionary event.
   * otherwise detect that server response is malformed and throw TypeError
   * @param response UserDictionaryResponse that should be a DictResponse
   * @returns object representing copy of backend dictionary
   */
  private handleDictResponse(response: UserDictionaryResponse): UserDictionary {
    if (DictionaryService.isDictResponse(response)) {
      this.dictionaryEventSubject.next(<GET_ALL_EVENT>{type: EVENT_TYPE.GET_ALL, value: response.result});
      return response.result;
    }
    throw new TypeError(`expected DictResponse but server replied with: ${response}`);
  }

  /**
   * Validate and parse UserDictionaryResponses that are expected to be ConfirmationResponses (created by set()).
   * if response is a ConfirmationResponse, return string result and create SET dictionary event.
   * otherwise detect that server response is malformed and throw TypeError
   * @param response UserDictionaryResponse that should be a DictResponse
   * @returns object representing copy of backend dictionary
   */
  private handleSetConfirmationResponse(key: string, value: string, response: UserDictionaryResponse): boolean {
    if (DictionaryService.isConfirmationResponse(response)) {
      this.dictionaryEventSubject.next(<SET_EVENT>{type: EVENT_TYPE.SET, key: key, value: value});
      return true;
    }
    throw new TypeError(`expected ConfirmationResponse but server replied with: ${response}`);
  }

  /**
   * Validate and parse UserDictionaryResponses that are expected to be ConfirmationResponses (created by delete()).
   * if response is a ConfirmationResponse, return string result and create DELETE dictionary event.
   * otherwise detect that server response is malformed and throw TypeError
   * @param response UserDictionaryResponse that should be a DictResponse
   * @returns object representing copy of backend dictionary
   */
  private handleDeleteConfirmationResponse(key: string, response: UserDictionaryResponse): boolean {
    if (DictionaryService.isConfirmationResponse(response)) {
      this.dictionaryEventSubject.next(<DELETE_EVENT>{type: EVENT_TYPE.DELETE, key: key});
      return true;
    }
    throw new TypeError(`expected ConfirmationResponse but server replied with: ${response}`);
  }

  /**
   * Validate and parse UserDictionaryResponses that are expected to be ErrorResponses.
   * if response is an ErrorResponse, rethrow as an error with message.
   * otherwise detect that server response is malformed and throw TypeError
   * @param response
   * @returns
   */
  private static handleErrorResponse(response: Error): Observable<never> {
    if (!(response instanceof HttpErrorResponse)) {
      return Observable.throwError(response);
    }
    if (DictionaryService.isErrorResponse(response.error)) {
      return Observable.throwError(new Error(response.error.message));
    }
    return Observable.throwError(new TypeError(`expected ErrorResponse but server replied with: ${response.error}`));
  }

  /**
   * uses AJV compiled schema validator to validate ErrorResponses
   */
  private static isErrorResponse(response: UserDictionaryResponse): response is ErrorResponse {
    return <boolean>DictionaryService.validateErrorResponse(response);
  }

  /**
   * uses AJV compiled schema validator to validate EntryResponses
   */
  private static isEntryResponse(response: UserDictionaryResponse): response is EntryResponse {
    return <boolean>DictionaryService.validateEntryResponse(response);
  }

  /**
   * uses AJV compiled schema validator to validate DictResponses
   */
  private static isDictResponse(response: UserDictionaryResponse): response is DictResponse {
    return <boolean>DictionaryService.validateDictResponse(response);
  }

  /**
   * uses AJV compiled schema validator to validate ConfirmationResponses
   */
  private static isConfirmationResponse(response: UserDictionaryResponse): response is ConfirmationResponse {
    return <boolean>DictionaryService.validateConfirmationResponse(response);
  }
}
