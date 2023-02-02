import { Injectable } from "@angular/core";

import { Observable, of, ReplaySubject, Subject } from "rxjs";
import { Role, User } from "../../type/user";
import { UserService } from "./user.service";
import { PublicInterfaceOf } from "../../util/stub";

export const MOCK_USER_ID = 1;
export const MOCK_USER_NAME = "testUser";
export const MOCK_USER_EMAIL = "testUser@testemail.com";
export const MOCK_USER = {
  uid: MOCK_USER_ID,
  name: MOCK_USER_NAME,
  email: MOCK_USER_EMAIL,
  googleId: undefined,
};

/**
 * This StubUserService is to test other service's functionality that depends on UserService
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

  googleLogin(): Observable<void> {
    throw new Error("Method not implemented.");
  }

  isLogin(): boolean {
    return this.user !== undefined;
  }

  isAdmin(): boolean {
    return this.user?.role === Role.ADMIN;
  }

  login(username: string, password: string): Observable<void> {
    return of();
  }

  logout(): void {}

  register(username: string, password: string): Observable<void> {
    return of();
  }

  userChanged(): Observable<User | undefined> {
    return this.userChangeSubject.asObservable();
  }

  getCurrentUser(): User | undefined {
    return this.user;
  }
}
