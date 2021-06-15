import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ReplaySubject } from 'rxjs';
import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { AppSettings } from '../../app-setting';
import { User } from '../../type/user';

/**
 * User Service contains the function of registering and logging the user.
 * It will save the user account inside for future use.
 *
 * @author Adam
 */
@Injectable({
  providedIn: 'root'
})
export class UserService {

  public static readonly AUTH_STATUS_ENDPOINT = 'users/auth/status';
  public static readonly LOGIN_ENDPOINT = 'users/login';
  public static readonly REGISTER_ENDPOINT = 'users/register';
  public static readonly LOG_OUT_ENDPOINT = 'users/logout';
  public static readonly GOOGLE_LOGIN_ENDPOINT = 'users/google-login';

  public gapiSetUp: boolean = false;
  public oauthInstance?: gapi.auth2.GoogleAuth;

  private currentUser: User | undefined = undefined;
  private userChangeSubject: ReplaySubject<User | undefined> = new ReplaySubject<User | undefined>(1);

  constructor(private http: HttpClient) {
    if (environment.userSystemEnabled) {
      this.loginFromSession();
    }
  }

  /**
   * This method will handle the request for user registration.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   * @param password
   */
  public register(userName: string, password: string): Observable<Response> {
    // assume the text passed in should be correct
    if (this.currentUser) {
      throw new Error('Already logged in when register.');
    }

    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${UserService.REGISTER_ENDPOINT}`, {userName, password});

  }

  /**
   * this method will init a Google OAuth instance and gapi if gapi has not been set up yet
   * it will return an existing Google OAuth instance if gapi has already been set up
   */
  public initGoogleOauth(): Promise<gapi.auth2.GoogleAuth> {
    if (!this.gapiSetUp) {
      // load gapi
      const gapiLoad = new Promise((resolve) => {
        gapi.load('auth2', resolve);
      });
      // gapi is loaded when the first promise resolves
      // mark gapi library as been loaded
      // then we can call gapi.auth2 init
      return new Promise((resolve) => {
        gapiLoad.then(
        () => gapi.auth2
          .init({client_id: environment.google.clientID})
          .then(auth => {
            this.oauthInstance = auth;
            this.gapiSetUp = true;
            resolve(auth);
          })
        );
      });
    } else {
      return new Promise((resolve) => {
        if (this.oauthInstance !== undefined) {
          resolve(this.oauthInstance);
        }
      });
    }
}

  /**
   * this method will return the current user's name of a given Google Oauth Instance
   */
  public getGoogleUserName(auth: gapi.auth2.GoogleAuth): Promise<String> {
    return new Promise((resolve) => resolve(auth.currentUser.get().getBasicProfile().getName()));
  }

  /**
   * this method allows application to access specified scopes offline
   * TODO: specify scopes here
   */
  public getGoogleAuthCode(auth: gapi.auth2.GoogleAuth): Promise<{code: string}> {
      return auth.grantOfflineAccess();
  }


  /**
   * This method will handle the request for Google login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param authoCode string
   */
  public googleLogin(authoCode: string): Observable<Response> {
    if (this.currentUser) {
      throw new Error('Already logged in when login in.');
    }
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${UserService.GOOGLE_LOGIN_ENDPOINT}`, {authoCode});
  }

  /**
   * This method will handle the request for user login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   * @param password
   */
  public login(userName: string, password: string): Observable<Response> {
    if (this.currentUser) {
      throw new Error('Already logged in when login in.');
    }
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${UserService.LOGIN_ENDPOINT}`, {userName, password});
  }

  /**
   * this method will clear the saved user account and trigger userChangeEvent
   */
  public logOut(): void {
    this.http.get<Response>(`${AppSettings.getApiEndpoint()}/${UserService.LOG_OUT_ENDPOINT}`)
      .subscribe(() => this.changeUser(undefined));
  }

  public getUser(): User | undefined {
    return this.currentUser;
  }

  public isLogin(): boolean {
    return this.currentUser !== undefined;
  }

  /**
   * changes the current user and triggers currentUserSubject
   * @param user
   */
  public changeUser(user: User | undefined): void {
    if (this.currentUser !== user) {
      this.currentUser = user;
      this.userChangeSubject.next(this.currentUser);
    }
  }

  /**
   * check the given parameter is legal for login/registration
   * @param userName
   */
  public validateUsername(userName: string): { result: boolean, message: string } {
    if (userName.trim().length === 0) {
      return {result: false, message: 'userName should not be empty'};
    }
    return {result: true, message: 'userName frontend validation success'};
  }

  public userChanged(): Observable<User | undefined> {

    return this.userChangeSubject.asObservable();
  }

  private loginFromSession(): void {
    this.http.get<User>(`${AppSettings.getApiEndpoint()}/${UserService.AUTH_STATUS_ENDPOINT}`)
      .filter(user => user != null)
      .subscribe(user => this.changeUser(user));
  }

}
