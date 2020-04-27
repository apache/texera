import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable, BehaviorSubject } from 'rxjs';
import { UserAccount } from '../../type/user-account';
import { UserAccountResponse } from '../../type/user-account';

export const registerURL = 'users/accounts/register';
export const loginURL = 'users/accounts/login';

/**
 * User Account Service contains the function of registering and logging the user.
 * It will save the user account inside for future use.
 *
 * @author Adam
 */
@Injectable()
export class UserAccountService {
  private userChangeEvent: EventEmitter<UserAccount> = new EventEmitter();
  private isLoginFlag: boolean = false;
  private currentUser: UserAccount = this.createEmptyUser();

  constructor(private http: HttpClient) {
    const userString: string | null = window.localStorage.getItem('currentUser');
    if (userString !== null) { // null checks here
      // tslint:disable-next-line:no-non-null-assertion
      const storedUser = JSON.parse(window.localStorage.getItem('currentUser')!);
      this.changeUser(storedUser, 0);
    }
  }

  /**
   * This method will handle the request for user registration.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   */
  public registerUser(userName: string): Observable<UserAccountResponse> {
    // assume the text passed in should be correct
    if (this.isLogin()) {throw new Error('Already logged in when register.'); }
    if (!this.checkUserAuthorizationLegal(userName)) {throw new Error(`userName ${userName} is illegal`); }

    return this.registerHttpRequest(userName).map(
      res => {
        if (res.code === 0) {
          window.localStorage.setItem('currentUser', JSON.stringify(res.userAccount));
          this.changeUser(res.userAccount, res.code);
          return res;
        } else { // register failed
          return res;
        }
      }
    );
  }

  /**
   * This method will handle the request for user login.
   * It will automatically login, save the user account inside and trigger userChangeEvent when success
   * @param userName
   */
  public loginUser(userName: string):  Observable<UserAccountResponse> {
    if (this.isLogin()) {throw new Error('Already logged in when login in.'); }
    if (!this.checkUserAuthorizationLegal(userName)) {throw new Error(`userName ${userName} is illegal`); }

    return this.loginHttpRequest(userName).map(
      res => {
        if (res.code === 0) {
          window.localStorage.setItem('currentUser', JSON.stringify(res.userAccount));
          this.changeUser(res.userAccount, res.code);
          return res;
        } else { // login failed
          return res;
        }
      }
    );
  }

  /**
   * this method will clear the saved user account and trigger userChangeEvent
   */
  public logOut(): void {
    window.localStorage.removeItem('currentUser');
    this.changeUser(this.createEmptyUser(), 1);
  }

  /**
   * this method will return true if there is saved user account inside
   */
  public isLogin(): boolean {
    return this.isLoginFlag;
  }

  public getUserID(): number {
    return this.getCurrentUserField('userID');
  }

  public getUserName(): string {
    return this.getCurrentUserField('userName');
  }

  /**
   * this method will return the fields inside the current user
   * @param field the field name of the {@link UserAccount}, should be string
   */
  public getCurrentUserField<Field extends keyof UserAccount>(field: Field): UserAccount[Field] {
    if (!this.isLogin()) {throw new Error('User is not login yet'); }
    return this.currentUser[field];
  }

  /**
   * this method will return the userChangeEvent, which can be subscribe
   * userChangeEvent will be triggered when the current user changes (login or log out)
   */
  public getUserChangeEvent(): EventEmitter<UserAccount> {
    return this.userChangeEvent;
  }

  /**
   * construct the request body as formData and create http request
   * @param userName
   */
  private registerHttpRequest(userName: string): Observable<UserAccountResponse> {
    const formData: FormData = new FormData();
    formData.append('userName', userName);
    return this.http.post<UserAccountResponse>(`${environment.apiUrl}/${registerURL}`, formData);
  }

  /**
   * construct the request body as formData and create http request
   * @param userName
   */
  private loginHttpRequest(userName: string): Observable<UserAccountResponse> {
    const formData: FormData = new FormData();
    formData.append('userName', userName);
    return this.http.post<UserAccountResponse>(`${environment.apiUrl}/${loginURL}`, formData);
  }

  /**
   * this method create and return an empty user.
   */
  private createEmptyUser(): UserAccount {
    const emptyUser: UserAccount = {
      userName : '',
      userID : -1
    };
    return emptyUser;
  }

  /**
   * this method change the saved user to the given parameter and trigger userChangeEvent
   * @param userAccount
   * @param code 0 indicates login while 1 indicates logging out
   */
  private changeUser(userAccount: UserAccount, code: 0 | 1): void {
    this.isLoginFlag = code === 0;
    this.currentUser = userAccount;
    this.userChangeEvent.emit(this.currentUser);
  }

  /**
   * check the given parameter is legal for login/registration
   * @param userName
   */
  private checkUserAuthorizationLegal(userName: string) {
    return !this.isLogin() &&
      userName !== null &&
      userName.length > 0;
  }

}
