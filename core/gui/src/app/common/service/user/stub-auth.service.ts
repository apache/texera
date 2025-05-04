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

import { Observable, of, throwError } from "rxjs";
import { User } from "../../type/user";
import { PublicInterfaceOf } from "../../util/stub";
import { AuthService } from "./auth.service";
import { MOCK_USER } from "./stub-user.service";

export const MOCK_TOKEN = {
  accessToken:
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJZaWNvbmcgSHVhbmciLCJ1c2VySWQiOjMsImV4cCI6OTk5OTk5OTk5OX0.aAM9pw_qIBs0EjD5hiCGHR4GEe2YPXPVenceJ3zaU_g",
};

export const MOCK_INVALID_TOKEN = {
  accessToken:
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJZaWNvbmcgSHVhbmciLCJ1c2VySWQiOjMsImV4cCI6MTYzNTEyODc2OX0.L3e93VQx91RMXpoN4sjtXXoX2llXQoEpCYd44oYftSQ",
};

/**
 * This StubUserService is to test other service's functionality that depends on UserService
 * It will correctly emit UserChangedEvent as the normal UserService do.
 */
@Injectable()
export class StubAuthService implements PublicInterfaceOf<AuthService> {
  auth(username: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    if (password === "password") {
      return of(MOCK_TOKEN);
    } else {
      return of(MOCK_INVALID_TOKEN);
    }
  }

  googleAuth(): Observable<Readonly<{ accessToken: string }>> {
    return of(MOCK_TOKEN);
  }

  loginWithExistingToken(): User | undefined {
    if (AuthService.getAccessToken() === MOCK_TOKEN.accessToken) {
      return MOCK_USER;
    } else {
      return undefined;
    }
  }

  logout(): undefined {
    return undefined;
  }

  register(username: string, password: string): Observable<Readonly<{ accessToken: string }>> {
    if (username !== "existing_user") {
      return of(MOCK_TOKEN);
    } else {
      return of(MOCK_INVALID_TOKEN);
    }
  }

  validateUsername(username: string): { result: boolean; message: string } {
    return { message: "", result: false };
  }
}
