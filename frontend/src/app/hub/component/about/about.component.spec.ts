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
import { RouterTestingModule } from "@angular/router/testing";
import { NzIconModule } from "ng-zorro-antd/icon";
import { UserOutline, LockOutline } from "@ant-design/icons-angular/icons";
import { vi } from "vitest";

import { AboutComponent } from "./about.component";
import { UserService } from "../../../common/service/user/user.service";
import { StubUserService } from "../../../common/service/user/stub-user.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../common/service/gui-config.service.mock";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("AboutComponent", () => {
  let component: AboutComponent;
  let fixture: ComponentFixture<AboutComponent>;
  let userService: StubUserService;
  let configService: MockGuiConfigService;

  function build() {
    fixture = TestBed.createComponent(AboutComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  }

  beforeEach(() => {
    const notificationSpy = { info: vi.fn(), success: vi.fn(), error: vi.fn() };
    TestBed.configureTestingModule({
      imports: [
        AboutComponent,
        RouterTestingModule.withRoutes([]),
        // Register the icons used by <texera-local-login>'s nzPrefixIcon
        // bindings. jsdom can't fetch icon SVGs over HTTP, so without this
        // the icon registry emits unhandled errors that fail the run in CI.
        NzIconModule.forChild([UserOutline, LockOutline]),
      ],
      providers: [
        { provide: UserService, useClass: StubUserService },
        { provide: NotificationService, useValue: notificationSpy },
        ...commonTestProviders,
      ],
    });
    userService = TestBed.inject(UserService) as unknown as StubUserService;
    configService = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
  });

  it("should create", () => {
    build();
    expect(component).toBeTruthy();
  });

  it("hides the local login form when the user is already logged in", () => {
    // StubUserService starts with MOCK_USER, so isLogin() === true.
    build();
    expect(fixture.nativeElement.querySelector("texera-local-login")).toBeNull();
  });

  it("shows the local login form when logged out and localLogin is enabled", () => {
    userService.user = undefined;
    build();
    expect(fixture.nativeElement.querySelector("texera-local-login")).not.toBeNull();
  });

  it("hides the local login form when localLogin is disabled in config", () => {
    userService.user = undefined;
    configService.setConfig({ localLogin: false });
    build();
    expect(fixture.nativeElement.querySelector("texera-local-login")).toBeNull();
  });
});
