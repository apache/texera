/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";

import { Observable, of, Subject } from "rxjs";
import { Role, User } from "../../type/user";
import { UserService } from "./user.service";
import { PublicInterfaceOf } from "../../util/stub";

export const MOCK_USER_ID = 1;
export const MOCK_USER_NAME = "testUser";
export const MOCK_USER_EMAIL = "testUser@testemail.com";
export const MOCK_USER_COMMENT = "testComent";
export const MOCK_USER = {
  uid: MOCK_USER_ID,
  name: MOCK_USER_NAME,
  email: MOCK_USER_EMAIL,
  googleId: undefined,
  role: Role.REGULAR,
  comment: MOCK_USER_COMMENT,
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

  getAvatar(googleAvatar: string): Observable<string | undefined> {
    return of(undefined);
  }
}
