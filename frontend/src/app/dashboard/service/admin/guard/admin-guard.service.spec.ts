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
import { Router } from "@angular/router";

import { AdminGuardService } from "./admin-guard.service";
import { UserService } from "../../../../common/service/user/user.service";
import { MOCK_USER, StubUserService } from "../../../../common/service/user/stub-user.service";
import { Role } from "../../../../common/type/user";
import { USER_WORKFLOW } from "../../../../app-routing.constant";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("AdminGuardService", () => {
  let guard: AdminGuardService;
  let userService: StubUserService;
  let routerSpy: { navigate: ReturnType<typeof vi.fn> };

  beforeEach(() => {
    routerSpy = { navigate: vi.fn() };
    TestBed.configureTestingModule({
      providers: [
        AdminGuardService,
        { provide: UserService, useClass: StubUserService },
        { provide: Router, useValue: routerSpy },
        ...commonTestProviders,
      ],
    });
    guard = TestBed.inject(AdminGuardService);
    userService = TestBed.inject(UserService) as unknown as StubUserService;
  });

  it("allows navigation and does not redirect when the user is an admin", () => {
    userService.user = { ...MOCK_USER, role: Role.ADMIN };

    expect(guard.canActivate()).toBe(true);
    expect(routerSpy.navigate).not.toHaveBeenCalled();
  });

  it("blocks navigation and redirects to USER_WORKFLOW when the user is a regular (non-admin) user", () => {
    userService.user = { ...MOCK_USER, role: Role.REGULAR };

    expect(guard.canActivate()).toBe(false);
    expect(routerSpy.navigate).toHaveBeenCalledWith([USER_WORKFLOW]);
  });

  it("blocks navigation and redirects to USER_WORKFLOW when there is no signed-in user", () => {
    userService.user = undefined;

    expect(guard.canActivate()).toBe(false);
    expect(routerSpy.navigate).toHaveBeenCalledWith([USER_WORKFLOW]);
  });
});
