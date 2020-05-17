import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable, Subject } from 'rxjs';
import { User } from '../../type/user';
import { UserWebResponse } from '../../type/user';
import { UserService } from './user.service';

export const STUB_USER_ID = 1;
export const STUB_USER_NAME = 'testUser';
const stubUser = {
  userName: STUB_USER_NAME,
  userID: STUB_USER_ID
};

/**
 * This StubUserService is to test other service's functionality that depends on UserService
 * This StubUserService is by default login and can login/register/logout multiply times.
 * The login/register will succeed when receive the user name {@link stubUserName} and fail otherwise.
 * It will correctly emit UserChangedEvent as the normal UserService do.
 */
@Injectable()
export class StubUserService extends UserService {
  private stubUserChangeEvent: Subject<User | undefined> = new Subject();
  private stubIsLoginFlag: boolean = true;
  private stubUser: User | undefined = stubUser;

  constructor(private stubHttp: HttpClient) {
    super(stubHttp);
  }

  public getUser(): User | undefined {
    return this.stubIsLoginFlag ? this.stubUser : undefined;
  }

  public register(userName: string): Observable<UserWebResponse> {
    return this.stubLoginAction(userName);
  }

  public login(userName: string):  Observable<UserWebResponse> {
    return this.stubLoginAction(userName);
  }

  public logOut(): void {
    this.stubIsLoginFlag = false;
    this.stubUserChangeEvent.next(undefined);
  }

  public isLogin(): boolean {
    return this.stubIsLoginFlag;
  }

  public getUserChangedEvent(): Observable<User | undefined> {
    return this.stubUserChangeEvent.asObservable();
  }

  private stubLoginAction(userName: string): Observable<UserWebResponse> {
    if (userName === STUB_USER_NAME) {
      this.stubIsLoginFlag = true;
      this.stubUserChangeEvent.next(this.stubUser);
      return Observable.of(
        {
          code: 0,
          user: {
            userName: STUB_USER_NAME,
            userID: STUB_USER_ID
          }
        }
      );
    } else {
      return Observable.of(
        {
          code: 1,
          message: 'user name does not match stub userName'
        }
      );
    }
  }

}
