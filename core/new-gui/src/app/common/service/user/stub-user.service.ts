import { Injectable } from '@angular/core';

import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs';
import { User } from '../../type/user';
import { UserService } from './user.service';

export const MOCK_USER_ID = 1;
export const MOCK_USER_NAME = 'testUser';
export const MOCK_USER = {
  name: MOCK_USER_NAME,
  uid: MOCK_USER_ID
};

type PublicInterfaceOf<Class> = {
  [Member in keyof Class]: Class[Member];
};

/**
 * This StubUserService is to test other service's functionality that depends on UserService
 * The login/register will succeed when receive the user name {@link stubUserName} and fail otherwise.
 * It will correctly emit UserChangedEvent as the normal UserService do.
 */
@Injectable()
export class StubUserService implements PublicInterfaceOf<UserService> {
  public userChangeSubject: Subject<User | undefined> = new Subject();
  public user: User | undefined;

  constructor() {
    this.user = MOCK_USER;
    this.userChangeSubject.next(this.user);
  }

  public getGoogleAuthInstance(): Observable<gapi.auth2.GoogleAuth> {
    throw new Error('Method not implemented.');
  }

  public googleLogin(authCode: string): Observable<User> {
    throw new Error('Method not implemented.');
  }

  public validateUsername(userName: string): { result: boolean; message: string; } {
    throw new Error('Method not implemented.');
  }

  public userChanged(): Observable<User | undefined> {

    return this.userChangeSubject.asObservable();
  }

  public getUser(): User | undefined {
    return this.user;
  }

  public register(userName: string): Observable<Response> {
    throw new Error('unimplemented');
  }

  public login(userName: string): Observable<Response> {
    if (this.user) {
      throw new Error('user is already logged in');
    }
    throw new Error('unimplemented');
  }

  public logOut(): void {
    throw new Error('unimplemented');
  }

  public isLogin(): boolean {
    return this.user !== undefined;
  }

  public changeUser(user: User | undefined): void {
    this.user = user;
  }

}
