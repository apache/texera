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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { Router } from "@angular/router";
import { UserIconComponent } from "./user-icon.component";
import { UserService } from "../../../../common/service/user/user.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { RouterTestingModule } from "@angular/router/testing";
import { AboutComponent } from "../../../../hub/component/about/about.component";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ABOUT } from "../../../../app-routing.constant";

describe("UserIconComponent", () => {
  let component: UserIconComponent;
  let fixture: ComponentFixture<UserIconComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [{ provide: UserService, useClass: StubUserService }, ...commonTestProviders],
      imports: [
        UserIconComponent,
        RouterTestingModule.withRoutes([{ path: "home", component: AboutComponent }]),
        HttpClientTestingModule,
        NzDropDownModule,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserIconComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("onClickLogout", () => {
    it("navigates to /about (no /dashboard prefix) after logout", () => {
      const router = TestBed.inject(Router);
      const navigateSpy = vi.spyOn(router, "navigate").mockResolvedValue(true);
      const userService = TestBed.inject(UserService);
      const logoutSpy = vi.spyOn(userService, "logout").mockImplementation(() => {});

      component.onClickLogout();

      expect(logoutSpy).toHaveBeenCalledTimes(1);
      expect(navigateSpy).toHaveBeenCalledWith([ABOUT]);
      expect(ABOUT).toBe("/about");
    });

    it("clears the flarum_remember cookie on logout", () => {
      const router = TestBed.inject(Router);
      vi.spyOn(router, "navigate").mockResolvedValue(true);
      const userService = TestBed.inject(UserService);
      vi.spyOn(userService, "logout").mockImplementation(() => {});
      // Seed the cookie so we can observe it being cleared. jsdom's
      // document.cookie is the test surface here; assigning a value with a
      // past expiry should expire the cookie immediately.
      document.cookie = "flarum_remember=token; path=/;";

      component.onClickLogout();

      expect(document.cookie).not.toContain("flarum_remember=token");
    });
  });
});
