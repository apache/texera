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

import { HTTP_INTERCEPTORS, HttpClient } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { Router } from "@angular/router";
import { ABOUT } from "../../app-routing.constant";
import { NotificationService } from "./notification/notification.service";
import { UserService } from "./user/user.service";
import { UnauthorizedHttpInterceptor } from "./unauthorized-http-interceptor.service";

describe("UnauthorizedHttpInterceptor", () => {
  let http: HttpClient;
  let httpMock: HttpTestingController;
  let routerSpy: { navigate: ReturnType<typeof vi.fn>; url: string };
  let notificationSpy: { error: ReturnType<typeof vi.fn> };
  let userServiceSpy: { logout: ReturnType<typeof vi.fn>; isLogin: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    routerSpy = { navigate: vi.fn(), url: "/user/workflow/42" };
    notificationSpy = { error: vi.fn() };
    userServiceSpy = { logout: vi.fn(), isLogin: vi.fn().mockReturnValue(true) };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: HTTP_INTERCEPTORS, useClass: UnauthorizedHttpInterceptor, multi: true },
        { provide: Router, useValue: routerSpy },
        { provide: NotificationService, useValue: notificationSpy },
        { provide: UserService, useValue: userServiceSpy },
      ],
    });

    http = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  function authedGet(url: string) {
    return http.get(url, { headers: { Authorization: "Bearer stale-token" } });
  }

  it("logs out, notifies, and redirects to ABOUT on 401 for an authenticated request", () => {
    // The decision to log out hinges on whether *this* request was authenticated.
    // A 401 from an anonymous request is the server saying "you need to log in",
    // not "your session is invalid" — clearing the session there would wipe a
    // freshly-stored token (e.g. mid-login race).
    authedGet("/api/secret").subscribe({ error: () => {} });
    httpMock.expectOne("/api/secret").flush(null, { status: 401, statusText: "Unauthorized" });

    expect(userServiceSpy.logout).toHaveBeenCalledTimes(1);
    expect(notificationSpy.error).toHaveBeenCalledTimes(1);
    expect(notificationSpy.error.mock.calls[0][0]).toMatch(/session.*expired|log in/i);
    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], {
      queryParams: { returnUrl: "/user/workflow/42" },
    });
  });

  it("leaves the session untouched when 401 comes back for an anonymous request", () => {
    // Reproduces the #5026 / #4903-revert scenario: a public endpoint (or one
    // whose token JwtModule skipped because it was expired) returning 401
    // must NOT trigger a logout — the user may not even be logged in yet.
    http.get("/api/public").subscribe({ error: () => {} });
    httpMock.expectOne("/api/public").flush(null, { status: 401, statusText: "Unauthorized" });

    expect(userServiceSpy.logout).not.toHaveBeenCalled();
    expect(notificationSpy.error).not.toHaveBeenCalled();
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("does not log out on non-401 errors even when Authorization was sent", () => {
    authedGet("/api/oops").subscribe({ error: () => {} });
    httpMock.expectOne("/api/oops").flush(null, { status: 500, statusText: "Server Error" });

    expect(userServiceSpy.logout).not.toHaveBeenCalled();
    expect(notificationSpy.error).not.toHaveBeenCalled();
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("omits returnUrl when the current route is the root", () => {
    routerSpy.url = "/";
    authedGet("/api/secret").subscribe({ error: () => {} });
    httpMock.expectOne("/api/secret").flush(null, { status: 401, statusText: "Unauthorized" });

    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], { queryParams: { returnUrl: null } });
  });

  // Adversarial-review fix #1: a stale token gets auto-attached to /auth/login
  // by JwtModule. A wrong-password 401 from login must NOT be misread as
  // "your session is invalid", or we'd kick an already-authenticated user
  // out the moment they fat-finger a re-login.
  describe("auth-endpoint allowlist", () => {
    const authPaths = [
      "/api/auth/login",
      "/api/auth/register",
      "/api/auth/refresh",
      "/api/auth/google/login",
      "/api/auth/login?next=/dashboard",
    ];

    authPaths.forEach(path => {
      it(`does not log out on 401 from ${path}`, () => {
        authedGet(path).subscribe({ error: () => {} });
        httpMock.expectOne(path).flush(null, { status: 401, statusText: "Unauthorized" });

        expect(userServiceSpy.logout).not.toHaveBeenCalled();
        expect(notificationSpy.error).not.toHaveBeenCalled();
        expect(routerSpy.navigate).not.toHaveBeenCalled();
      });
    });

    it("still logs out on 401 from a URL that merely contains 'login' in a non-auth segment", () => {
      // Guards against an over-broad regex like /login/ that would skip e.g.
      // /api/dashboard/recent-logins.
      authedGet("/api/dashboard/recent-logins").subscribe({ error: () => {} });
      httpMock.expectOne("/api/dashboard/recent-logins").flush(null, { status: 401, statusText: "Unauthorized" });

      expect(userServiceSpy.logout).toHaveBeenCalledTimes(1);
    });
  });

  // Adversarial-review fix #2: dashboard load fires 8-10 parallel requests; a
  // server-side revoke makes them all 401 within microseconds of each other.
  // Side effects must fire exactly once, not once per request — otherwise we
  // get a toast pile-up and redundant navigations.
  describe("concurrent 401 deduplication", () => {
    it("fires logout side effects exactly once for a burst of 401s", () => {
      // Simulate UserService transitioning from logged-in → logged-out as soon
      // as logout() is called; this is what UserService.changeUser(undefined)
      // does in production and is the discriminator the interceptor reads.
      userServiceSpy.logout.mockImplementation(() => {
        userServiceSpy.isLogin.mockReturnValue(false);
      });

      authedGet("/api/a").subscribe({ error: () => {} });
      authedGet("/api/b").subscribe({ error: () => {} });
      authedGet("/api/c").subscribe({ error: () => {} });

      httpMock.expectOne("/api/a").flush(null, { status: 401, statusText: "Unauthorized" });
      httpMock.expectOne("/api/b").flush(null, { status: 401, statusText: "Unauthorized" });
      httpMock.expectOne("/api/c").flush(null, { status: 401, statusText: "Unauthorized" });

      expect(userServiceSpy.logout).toHaveBeenCalledTimes(1);
      expect(notificationSpy.error).toHaveBeenCalledTimes(1);
      expect(routerSpy.navigate).toHaveBeenCalledTimes(1);
    });

    it("re-arms after a successful re-login so a later 401 triggers logout again", () => {
      // After logout, isLogin() is false. Dedup keys off isLogin(), so when the
      // user re-logs in (isLogin() flips back to true) and a fresh request 401s,
      // the interceptor must fire side effects again. Otherwise the user gets
      // stuck in a half-logged-in state after a transient session error.
      userServiceSpy.logout.mockImplementationOnce(() => {
        userServiceSpy.isLogin.mockReturnValue(false);
      });

      authedGet("/api/a").subscribe({ error: () => {} });
      httpMock.expectOne("/api/a").flush(null, { status: 401, statusText: "Unauthorized" });
      expect(userServiceSpy.logout).toHaveBeenCalledTimes(1);

      // Simulate re-login: isLogin() back to true.
      userServiceSpy.isLogin.mockReturnValue(true);
      userServiceSpy.logout.mockImplementationOnce(() => {
        userServiceSpy.isLogin.mockReturnValue(false);
      });

      authedGet("/api/b").subscribe({ error: () => {} });
      httpMock.expectOne("/api/b").flush(null, { status: 401, statusText: "Unauthorized" });

      expect(userServiceSpy.logout).toHaveBeenCalledTimes(2);
    });
  });
});
