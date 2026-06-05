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
import { AuthService } from "./user/auth.service";
import { UnauthorizedHttpInterceptor } from "./unauthorized-http-interceptor.service";

describe("UnauthorizedHttpInterceptor", () => {
  let http: HttpClient;
  let httpMock: HttpTestingController;
  let routerSpy: { navigate: ReturnType<typeof vi.fn>; url: string };
  let notificationSpy: { error: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    routerSpy = { navigate: vi.fn(), url: "/user/workflow/42" };
    notificationSpy = { error: vi.fn() };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        { provide: HTTP_INTERCEPTORS, useClass: UnauthorizedHttpInterceptor, multi: true },
        { provide: Router, useValue: routerSpy },
        { provide: NotificationService, useValue: notificationSpy },
      ],
    });

    http = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
    AuthService.setAccessToken("stale-token");
  });

  afterEach(() => {
    httpMock.verify();
    AuthService.removeAccessToken();
  });

  it("clears the session, notifies, and redirects to ABOUT on 401 for a request carrying Authorization", () => {
    // The decision to log out hinges on whether *this* request was authenticated.
    // A 401 from an anonymous request is the server saying "you need to log in",
    // not "your session is invalid" — clearing localStorage there would wipe a
    // freshly-stored token (e.g. mid-login race).
    http.get("/api/secret", { headers: { Authorization: "Bearer stale-token" } }).subscribe({ error: () => {} });

    httpMock.expectOne("/api/secret").flush(null, { status: 401, statusText: "Unauthorized" });

    expect(AuthService.getAccessToken()).toBeNull();
    expect(notificationSpy.error).toHaveBeenCalledTimes(1);
    expect(notificationSpy.error.mock.calls[0][0]).toMatch(/session.*expired|log in/i);
    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], {
      queryParams: { returnUrl: "/user/workflow/42" },
    });
  });

  it("leaves the session untouched when 401 comes back for an anonymous request", () => {
    // Reproduces the #5026 / #4903-revert scenario: a public endpoint
    // (or one whose token JwtModule skipped because it was expired)
    // returning 401 must NOT trigger a logout — the user may not even be
    // logged in yet, and we'd erase a token that was just being set.
    http.get("/api/public").subscribe({ error: () => {} });

    httpMock.expectOne("/api/public").flush(null, { status: 401, statusText: "Unauthorized" });

    expect(AuthService.getAccessToken()).toBe("stale-token");
    expect(notificationSpy.error).not.toHaveBeenCalled();
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("does not log out on non-401 errors even when Authorization was sent", () => {
    http.get("/api/oops", { headers: { Authorization: "Bearer stale-token" } }).subscribe({ error: () => {} });

    httpMock.expectOne("/api/oops").flush(null, { status: 500, statusText: "Server Error" });

    expect(AuthService.getAccessToken()).toBe("stale-token");
    expect(notificationSpy.error).not.toHaveBeenCalled();
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("omits returnUrl when the current route is the root", () => {
    routerSpy.url = "/";

    http.get("/api/secret", { headers: { Authorization: "Bearer stale-token" } }).subscribe({ error: () => {} });

    httpMock.expectOne("/api/secret").flush(null, { status: 401, statusText: "Unauthorized" });

    // Match AuthGuardService's behavior on root: returnUrl=null so the user
    // lands on ABOUT cleanly without a self-referential redirect loop.
    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], { queryParams: { returnUrl: null } });
  });
});
