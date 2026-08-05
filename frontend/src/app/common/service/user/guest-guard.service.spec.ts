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

import { TestBed } from "@angular/core/testing";
import { ActivatedRouteSnapshot, Router } from "@angular/router";

import { GuestGuardService } from "./guest-guard.service";
import { UserService } from "./user.service";
import { MOCK_USER, StubUserService } from "./stub-user.service";
import { USER_WORKFLOW } from "../../../app-routing.constant";
import { commonTestProviders } from "../../testing/test-utils";

describe("GuestGuardService", () => {
  let guard: GuestGuardService;
  let userService: StubUserService;
  let routerSpy: { navigateByUrl: ReturnType<typeof vi.fn> };

  const routeWith = (queryParams: Record<string, string> = {}): ActivatedRouteSnapshot =>
    ({ queryParams }) as unknown as ActivatedRouteSnapshot;

  beforeEach(() => {
    routerSpy = { navigateByUrl: vi.fn() };
    TestBed.configureTestingModule({
      providers: [
        GuestGuardService,
        { provide: UserService, useClass: StubUserService },
        { provide: Router, useValue: routerSpy },
        ...commonTestProviders,
      ],
    });
    guard = TestBed.inject(GuestGuardService);
    userService = TestBed.inject(UserService) as unknown as StubUserService;
  });

  it("allows a logged-out visitor onto the login page", () => {
    userService.user = undefined;
    expect(guard.canActivate(routeWith())).toBe(true);
    expect(routerSpy.navigateByUrl).not.toHaveBeenCalled();
  });

  it("sends a logged-in user to their workflows instead of the login form", () => {
    userService.user = MOCK_USER;
    expect(guard.canActivate(routeWith())).toBe(false);
    expect(routerSpy.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
  });

  // The auth guard and the 401 interceptor both attach a returnUrl; if the user turns out to
  // still be signed in, honour it rather than dumping them on the default page.
  it("honours a returnUrl when redirecting a logged-in user", () => {
    userService.user = MOCK_USER;
    expect(guard.canActivate(routeWith({ returnUrl: "/dashboard/user/dataset" }))).toBe(false);
    expect(routerSpy.navigateByUrl).toHaveBeenCalledWith("/dashboard/user/dataset");
  });
});
