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

import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { JwtHelperService } from "@auth0/angular-jwt";
import { NzModalService } from "ng-zorro-antd/modal";
import { AppSettings } from "../../app-setting";
import { Role } from "../../type/user";
import { AuthService, TOKEN_KEY } from "./auth.service";
import { NotificationService } from "../notification/notification.service";
import { GmailService } from "../gmail/gmail.service";
import { GuiConfigService } from "../gui-config.service";

describe("AuthService", () => {
  let service: AuthService;
  let httpMock: HttpTestingController;
  let jwt: {
    isTokenExpired: ReturnType<typeof vi.fn>;
    decodeToken: ReturnType<typeof vi.fn>;
    getTokenExpirationDate: ReturnType<typeof vi.fn>;
  };
  let notification: { error: ReturnType<typeof vi.fn> };
  let config: { env: { inviteOnly: boolean } };
  let modal: { info: ReturnType<typeof vi.fn>; create: ReturnType<typeof vi.fn> };

  const api = AppSettings.getApiEndpoint();
  const claims = {
    role: Role.REGULAR,
    userId: 5,
    email: "u@x.com",
    sub: "Ursula",
    googleId: "g",
    googleAvatar: "a",
    comment: "c",
    joiningReason: "r",
  };

  beforeEach(() => {
    localStorage.clear();
    jwt = {
      isTokenExpired: vi.fn().mockReturnValue(false),
      decodeToken: vi.fn().mockReturnValue(claims),
      getTokenExpirationDate: vi.fn().mockReturnValue(new Date(Date.now() + 60_000)),
    };
    notification = { error: vi.fn() };
    config = { env: { inviteOnly: false } };
    modal = { info: vi.fn(), create: vi.fn() };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        AuthService,
        { provide: JwtHelperService, useValue: jwt },
        { provide: NotificationService, useValue: notification },
        { provide: GmailService, useValue: {} },
        { provide: GuiConfigService, useValue: config },
        { provide: NzModalService, useValue: modal },
      ],
    });
    service = TestBed.inject(AuthService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // loginWithExistingToken()'s valid-token path schedules an auto-logout timer;
    // logout() unsubscribes it so no real timer leaks into later tests / keeps Vitest alive.
    service.logout();
    httpMock.verify();
    localStorage.clear();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("static access-token helpers", () => {
    it("set/get/remove roundtrip through localStorage", () => {
      expect(AuthService.getAccessToken()).toBeNull();

      AuthService.setAccessToken("tok");
      expect(localStorage.getItem(TOKEN_KEY)).toEqual("tok");
      expect(AuthService.getAccessToken()).toEqual("tok");

      AuthService.removeAccessToken();
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });

  describe("HTTP auth endpoints", () => {
    it("register() POSTs username/password to the register endpoint", () => {
      service.register("alice", "pw").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.REGISTER_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({ username: "alice", password: "pw" });
      req.flush({ accessToken: "t" });
    });

    it("auth() POSTs username/password to the login endpoint", () => {
      service.auth("alice", "pw").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({ username: "alice", password: "pw" });
      req.flush({ accessToken: "t" });
    });

    it("googleAuth() POSTs the raw credential with a text/plain content type", () => {
      service.googleAuth("cred").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.GOOGLE_LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual("cred");
      expect(req.request.headers.get("Content-Type")).toEqual("text/plain");
      req.flush({ accessToken: "t" });
    });

    const errorCases = [
      { name: "register", call: () => service.register("alice", "pw"), endpoint: AuthService.REGISTER_ENDPOINT },
      { name: "auth", call: () => service.auth("alice", "pw"), endpoint: AuthService.LOGIN_ENDPOINT },
      { name: "googleAuth", call: () => service.googleAuth("cred"), endpoint: AuthService.GOOGLE_LOGIN_ENDPOINT },
    ];

    errorCases.forEach(({ name, call, endpoint }) => {
      it(`${name}() propagates HTTP errors to the subscriber`, () => {
        const onError = vi.fn();
        call().subscribe({ error: onError });

        const req = httpMock.expectOne(`${api}/${endpoint}`);
        req.flush("nope", { status: 401, statusText: "Unauthorized" });

        expect(onError).toHaveBeenCalledTimes(1);
        expect(onError.mock.calls[0][0].status).toEqual(401);
      });
    });
  });

  describe("logout", () => {
    it("removes the stored access token and returns undefined", () => {
      AuthService.setAccessToken("tok");
      expect(service.logout()).toBeUndefined();
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });

  describe("loginWithExistingToken", () => {
    it("logs out and returns undefined when no token is stored", () => {
      expect(service.loginWithExistingToken()).toBeUndefined();
    });

    it("errors and logs out when the stored token is expired", () => {
      AuthService.setAccessToken("tok");
      jwt.isTokenExpired.mockReturnValue(true);

      const result = service.loginWithExistingToken();

      expect(result).toBeUndefined();
      expect(notification.error).toHaveBeenCalledWith("Access token is expired!");
      expect(AuthService.getAccessToken()).toBeNull();
    });

    it("returns a User built from the decoded claims for a valid token", () => {
      AuthService.setAccessToken("tok");

      const user = service.loginWithExistingToken();

      expect(user).toEqual({
        uid: 5,
        name: "Ursula",
        email: "u@x.com",
        googleId: "g",
        googleAvatar: "a",
        role: Role.REGULAR,
        comment: "c",
        joiningReason: "r",
      });
    });

    it("in invite-only mode, an inactive user is logged out and registration is checked", () => {
      AuthService.setAccessToken("tok");
      config.env.inviteOnly = true;
      jwt.decodeToken.mockReturnValue({ ...claims, role: Role.INACTIVE });

      const result = service.loginWithExistingToken();

      expect(result).toBeUndefined();
      const req = httpMock.expectOne(r => r.url === `${api}/user/joining-reason/required`);
      expect(req.request.params.get("uid")).toEqual("5");
      req.flush(false);

      expect(modal.info).toHaveBeenCalledTimes(1);
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });
});
