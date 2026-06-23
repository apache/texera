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
import { ActivatedRouteSnapshot, Router, RouterStateSnapshot } from "@angular/router";

import { AuthGuardService } from "./auth-guard.service";
import { UserService } from "./user.service";
import { MOCK_USER, StubUserService } from "./stub-user.service";
import { ABOUT } from "../../../app-routing.constant";
import { commonTestProviders } from "../../testing/test-utils";

describe("AuthGuardService", () => {
  let guard: AuthGuardService;
  let userService: StubUserService;
  let routerSpy: { navigate: ReturnType<typeof vi.fn> };

  const route = {} as ActivatedRouteSnapshot;
  const stateAt = (url: string): RouterStateSnapshot => ({ url }) as RouterStateSnapshot;

  beforeEach(() => {
    routerSpy = { navigate: vi.fn() };
    TestBed.configureTestingModule({
      providers: [
        AuthGuardService,
        { provide: UserService, useClass: StubUserService },
        { provide: Router, useValue: routerSpy },
        ...commonTestProviders,
      ],
    });
    guard = TestBed.inject(AuthGuardService);
    userService = TestBed.inject(UserService) as unknown as StubUserService;
  });

  it("allows navigation and does not redirect when the user is logged in", () => {
    userService.user = MOCK_USER;
    expect(guard.canActivate(route, stateAt("/dashboard/user/workflow/42"))).toBe(true);
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("blocks navigation and redirects to ABOUT with a null returnUrl from the root url", () => {
    userService.user = undefined;
    expect(guard.canActivate(route, stateAt("/"))).toBe(false);
    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], { queryParams: { returnUrl: null } });
  });

  it("blocks navigation and preserves the return url for a deep link", () => {
    userService.user = undefined;
    expect(guard.canActivate(route, stateAt("/dashboard/user/workflow/42"))).toBe(false);
    expect(routerSpy.navigate).toHaveBeenCalledWith([ABOUT], {
      queryParams: { returnUrl: "/dashboard/user/workflow/42" },
    });
  });
});
