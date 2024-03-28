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
