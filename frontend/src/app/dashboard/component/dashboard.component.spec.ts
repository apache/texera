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
import { DashboardComponent } from "./dashboard.component";
import { ChangeDetectorRef, EventEmitter, NgZone } from "@angular/core";
import { By } from "@angular/platform-browser";
import { EMPTY, of, throwError } from "rxjs";

import { UserService } from "../../common/service/user/user.service";
import { FlarumService } from "../service/user/flarum/flarum.service";
import { SocialAuthService } from "@abacritt/angularx-social-login";
import { AdminSettingsService } from "../service/admin/settings/admin-settings.service";
import {
  ActivatedRoute,
  ActivatedRouteSnapshot,
  convertToParamMap,
  Data,
  NavigationEnd,
  Params,
  Router,
  RouterLink,
  UrlSegment,
} from "@angular/router";
import type { Mock } from "vitest";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../common/testing/test-utils";
import { GuiConfigService } from "../../common/service/gui-config.service";
import {
  ABOUT,
  ADMIN_EXECUTION,
  ADMIN_GMAIL,
  ADMIN_SETTINGS,
  ADMIN_USER,
  USER_COMPUTING_UNIT,
  USER_DATASET,
  USER_DISCUSSION,
  USER_PROJECT,
  USER_QUOTA,
  USER_WORKFLOW,
} from "../../app-routing.constant";

describe("DashboardComponent", () => {
  let component: DashboardComponent;
  let fixture: ComponentFixture<DashboardComponent>;

  let userServiceMock: Partial<UserService>;
  let routerMock: Partial<Router>;
  let flarumServiceMock: Partial<FlarumService>;
  let cdrMock: Partial<ChangeDetectorRef>;
  let ngZoneMock: Partial<NgZone>;
  let socialAuthServiceMock: Partial<SocialAuthService>;
  let adminSettingsServiceMock: Partial<AdminSettingsService>;
  let activatedRouteMock: Partial<ActivatedRoute>;

  const activatedRouteSnapshotMock: Partial<ActivatedRouteSnapshot> = {
    queryParams: {},
    url: [] as UrlSegment[],
    params: {} as Params,
    fragment: null,
    data: {} as Data,
    paramMap: convertToParamMap({}),
    queryParamMap: convertToParamMap({}),
    outlet: "",
    routeConfig: null,
    root: null as any,
    parent: null as any,
    firstChild: null as any,
    children: [],
    pathFromRoot: [],
  };

  beforeEach(async () => {
    userServiceMock = {
      isAdmin: vi.fn().mockReturnValue(false),
      isLogin: vi.fn().mockReturnValue(false),
      userChanged: vi.fn().mockReturnValue(of(null)),
      getCurrentUser: vi.fn().mockReturnValue(undefined),
    };

    routerMock = {
      events: of(new NavigationEnd(1, "/", "/")),
      url: "/",
      navigateByUrl: vi.fn(),
    };

    flarumServiceMock = {
      auth: vi.fn().mockReturnValue(of({ token: "dummyToken" })),
      register: vi.fn().mockReturnValue(of(null)),
    };

    cdrMock = {
      detectChanges: vi.fn(),
    };

    ngZoneMock = {
      hasPendingMicrotasks: false,
      hasPendingMacrotasks: false,
      onUnstable: new EventEmitter<any>(),
      onMicrotaskEmpty: new EventEmitter<any>(),
      onStable: new EventEmitter<any>(),
      onError: new EventEmitter<any>(),
      run: (fn: () => any) => fn(),
      runGuarded: (fn: () => any) => fn(),
      runOutsideAngular: (fn: () => any) => fn(),
      runTask: (fn: () => any) => fn(),
    };

    socialAuthServiceMock = {
      authState: EMPTY,
      // GoogleSigninButtonDirective subscribes to initState in its constructor;
      // EMPTY keeps the subscription open without triggering google.accounts.id.renderButton.
      initState: EMPTY,
    };

    adminSettingsServiceMock = {
      getSetting: vi.fn().mockReturnValue(EMPTY),
    };

    activatedRouteMock = {
      snapshot: activatedRouteSnapshotMock as ActivatedRouteSnapshot,
    };

    await TestBed.configureTestingModule({
      imports: [DashboardComponent, HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: Router, useValue: routerMock },
        { provide: FlarumService, useValue: flarumServiceMock },
        { provide: ChangeDetectorRef, useValue: cdrMock },
        { provide: NgZone, useValue: ngZoneMock },
        { provide: SocialAuthService, useValue: socialAuthServiceMock },
        { provide: AdminSettingsService, useValue: adminSettingsServiceMock },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should render Google sign-in button when user is NOT logged in", () => {
    (userServiceMock.isLogin as Mock).mockReturnValue(false);
    fixture.detectChanges();

    const googleSignInBtn = fixture.debugElement.query(By.css("asl-google-signin-button"));
    expect(googleSignInBtn).toBeTruthy();
  });

  it("should render the powered-by attribution when attributionEnabled is true", () => {
    TestBed.inject(GuiConfigService).env.attributionEnabled = true;
    fixture.detectChanges();

    expect(fixture.debugElement.query(By.css("#powered-by"))).toBeTruthy();
  });

  describe("forumLogin", () => {
    const clearForumCookie = () =>
      (document.cookie = "flarum_remember=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/");

    beforeEach(() => {
      clearForumCookie();
      (userServiceMock.isLogin as Mock).mockReturnValue(true);
      component.isLogin = true;
      component.displayForum = true;
      (flarumServiceMock.auth as Mock).mockClear();
      (flarumServiceMock.register as Mock).mockClear();
    });

    afterEach(() => clearForumCookie());

    it("stores the flarum_remember cookie on successful auth and does not register", () => {
      (flarumServiceMock.auth as Mock).mockReturnValue(of({ token: "tok123" }));

      component.forumLogin();

      expect(document.cookie).toContain("flarum_remember=tok123");
      expect(flarumServiceMock.register).not.toHaveBeenCalled();
    });

    it("hides the forum and does not register when auth fails with 404/500", () => {
      (flarumServiceMock.auth as Mock).mockReturnValue(throwError(() => ({ status: 404 })));

      component.forumLogin();

      expect(component.displayForum).toBe(false);
      expect(flarumServiceMock.register).not.toHaveBeenCalled();
    });

    it("registers at most once and stops when auth keeps failing (no infinite loop)", () => {
      (flarumServiceMock.auth as Mock).mockReturnValue(throwError(() => ({ status: 401 })));
      (flarumServiceMock.register as Mock).mockReturnValue(of(null));

      component.forumLogin();

      // auth -> register -> auth -> stop: register fires once, auth twice, then it terminates.
      expect(flarumServiceMock.register).toHaveBeenCalledTimes(1);
      expect(flarumServiceMock.auth).toHaveBeenCalledTimes(2);
      expect(component.displayForum).toBe(false);
    });

    it("hides the forum when registration fails", () => {
      (flarumServiceMock.auth as Mock).mockReturnValue(throwError(() => ({ status: 401 })));
      (flarumServiceMock.register as Mock).mockReturnValue(throwError(() => ({ status: 500 })));

      component.forumLogin();

      expect(flarumServiceMock.register).toHaveBeenCalledTimes(1);
      expect(component.displayForum).toBe(false);
    });

    it("does nothing when a flarum_remember cookie is already present", () => {
      document.cookie = "flarum_remember=existing;path=/";

      component.forumLogin();

      expect(flarumServiceMock.auth).not.toHaveBeenCalled();
      expect(flarumServiceMock.register).not.toHaveBeenCalled();
    });
  });

  it("should hide the navbar on workflow workspace routes", () => {
    expect(component.isNavbarEnabled("/user/workflow/42")).toBe(false);
    expect(component.isNavbarEnabled("/user/workflow")).toBe(true);
    expect(component.isNavbarEnabled("/user/project")).toBe(true);
  });

  it("exposes route constants without the legacy /dashboard prefix", () => {
    expect(USER_PROJECT).toBe("/user/project");
    expect(USER_WORKFLOW).toBe("/user/workflow");
    expect(USER_DATASET).toBe("/user/dataset");
    expect(USER_COMPUTING_UNIT).toBe("/user/compute");
    expect(USER_QUOTA).toBe("/user/quota");
    expect(USER_DISCUSSION).toBe("/user/discussion");
    expect(ADMIN_USER).toBe("/admin/user");
    expect(ADMIN_EXECUTION).toBe("/admin/execution");
    expect(ADMIN_GMAIL).toBe("/admin/gmail");
    expect(ADMIN_SETTINGS).toBe("/admin/settings");
    expect(ABOUT).toBe("/about");
  });

  it("renders every sidebar tab's routerLink when fully enabled", () => {
    (userServiceMock.isLogin as Mock).mockReturnValue(true);
    component.isLogin = true;
    component.isAdmin = true;
    component.sidebarTabs = {
      hub_enabled: false,
      home_enabled: true,
      workflow_enabled: true,
      dataset_enabled: true,
      your_work_enabled: true,
      projects_enabled: true,
      workflows_enabled: true,
      datasets_enabled: true,
      compute_enabled: true,
      quota_enabled: true,
      forum_enabled: true,
      about_enabled: true,
    };
    fixture.detectChanges();

    // 7 "Your Work" links (incl. Python Venvs) + 4 admin links + 1 about link + 1 feedback link = 13
    expect(fixture.debugElement.queryAll(By.directive(RouterLink)).length).toBe(13);
  });
});
