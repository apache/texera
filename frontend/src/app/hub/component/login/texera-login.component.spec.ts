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
import { ActivatedRoute, ActivatedRouteSnapshot, Router } from "@angular/router";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { EMPTY, Subject, of, throwError } from "rxjs";
import { SocialAuthService, SocialUser } from "@abacritt/angularx-social-login";
import { vi } from "vitest";

import { TexeraLoginComponent } from "./texera-login.component";
import { UserService } from "../../../common/service/user/user.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../common/service/gui-config.service.mock";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { USER_WORKFLOW } from "../../../app-routing.constant";

describe("TexeraLoginComponent", () => {
  let component: TexeraLoginComponent;
  let fixture: ComponentFixture<TexeraLoginComponent>;

  let userServiceMock: Partial<UserService>;
  let notificationServiceMock: Partial<NotificationService>;
  let routerMock: Partial<Router>;
  let socialAuthServiceMock: Partial<SocialAuthService>;
  // Typed to allow null so the replayed-logout case can be exercised.
  let authState$: Subject<SocialUser | null>;

  const googleUser = (idToken: string): SocialUser => ({ provider: "GOOGLE", idToken }) as unknown as SocialUser;

  const createComponent = async (queryParams: Record<string, any> = {}) => {
    TestBed.resetTestingModule();
    authState$ = new Subject<SocialUser | null>();
    userServiceMock = {
      isLogin: vi.fn().mockReturnValue(false),
      login: vi.fn().mockReturnValue(of(undefined)),
      register: vi.fn().mockReturnValue(of(undefined)),
      googleLogin: vi.fn().mockReturnValue(of(undefined)),
    };
    notificationServiceMock = { error: vi.fn(), success: vi.fn() };
    routerMock = { navigateByUrl: vi.fn() };
    socialAuthServiceMock = {
      authState: authState$.asObservable() as SocialAuthService["authState"],
      // GoogleSigninButtonDirective subscribes to initState in its constructor;
      // EMPTY keeps the subscription open without triggering google.accounts.id.renderButton.
      initState: EMPTY,
    };

    await TestBed.configureTestingModule({
      imports: [TexeraLoginComponent, HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: Router, useValue: routerMock },
        { provide: ActivatedRoute, useValue: { snapshot: { queryParams } as Partial<ActivatedRouteSnapshot> } },
        { provide: SocialAuthService, useValue: socialAuthServiceMock },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(TexeraLoginComponent);
    component = fixture.componentInstance;
  };

  beforeEach(async () => {
    await createComponent();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should create the component", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe("ngOnInit", () => {
    it("prefills username/password from defaultLocalUser when populated", () => {
      const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      config.setConfig({ defaultLocalUser: { username: "preset-user", password: "preset-pass" } });

      component.ngOnInit();

      expect(component.form.get("username")!.value).toBe("preset-user");
      expect(component.form.get("password")!.value).toBe("preset-pass");
    });

    it("does not prefill when defaultLocalUser is empty", () => {
      const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      config.setConfig({ defaultLocalUser: {} });

      component.ngOnInit();

      expect(component.form.get("username")!.value).toBe("");
      expect(component.form.get("password")!.value).toBe("");
    });

    // Nothing on this page is useful to someone already signed in. This replaces the
    // route guard that used to bounce them before the component was ever constructed.
    it("sends an already-signed-in visitor to their workflows", () => {
      (userServiceMock.isLogin as any).mockReturnValue(true);

      component.ngOnInit();

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    // The auth guard and the 401 interceptor both attach a returnUrl; if the visitor turns
    // out to still be signed in, honour it rather than dumping them on the default page.
    it("honours a returnUrl when redirecting an already-signed-in visitor", async () => {
      await createComponent({ returnUrl: "/dashboard/user/dataset" });
      (userServiceMock.isLogin as any).mockReturnValue(true);

      component.ngOnInit();

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith("/dashboard/user/dataset");
    });

    it("skips the prefill and the Google subscription when already signed in", () => {
      const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      config.setConfig({ defaultLocalUser: { username: "preset-user", password: "preset-pass" } });
      (userServiceMock.isLogin as any).mockReturnValue(true);

      component.ngOnInit();
      authState$.next(googleUser("tok"));

      expect(component.form.get("username")!.value).toBe("");
      expect(userServiceMock.googleLogin).not.toHaveBeenCalled();
    });
  });

  describe("setMode", () => {
    it("switches mode and clears the error message", () => {
      component.errorMessage = "stale";
      component.setMode("signup");
      expect(component.mode).toBe("signup");
      expect(component.errorMessage).toBeUndefined();
    });
  });

  describe("togglePasswordVisibility", () => {
    it("flips the passwordVisible flag", () => {
      expect(component.passwordVisible).toBe(false);
      component.togglePasswordVisibility();
      expect(component.passwordVisible).toBe(true);
    });
  });

  describe("confirmationValidator (via the confirm control)", () => {
    it("flags a mismatch only in sign-up mode", () => {
      component.setMode("signup");
      component.form.patchValue({ password: "secret1", confirm: "secret2" });
      component.form.controls.confirm.updateValueAndValidity();
      expect(component.form.controls.confirm.errors).toEqual({ confirm: true });
    });

    it("does not flag a mismatch in sign-in mode", () => {
      component.setMode("signin");
      component.form.patchValue({ password: "secret1", confirm: "secret2" });
      component.form.controls.confirm.updateValueAndValidity();
      expect(component.form.controls.confirm.errors).toBeNull();
    });
  });

  describe("submit -> login (sign-in mode)", () => {
    beforeEach(() => component.setMode("signin"));

    it("short-circuits and sets errorMessage when the username is blank", () => {
      component.form.patchValue({ username: "   ", password: "secret1" });
      component.submit();
      expect(userServiceMock.login).not.toHaveBeenCalled();
      expect(component.errorMessage).toBeTruthy();
    });

    it("sets errorMessage when the password is shorter than 6 characters", () => {
      component.form.patchValue({ username: "alice", password: "abc" });
      component.submit();
      expect(userServiceMock.login).not.toHaveBeenCalled();
      expect(component.errorMessage).toBe("Password length should be greater than 5.");
    });

    it("calls UserService.login with a trimmed username and navigates to USER_WORKFLOW", () => {
      component.form.patchValue({ username: "  alice  ", password: "secret1" });
      component.submit();
      expect(userServiceMock.login).toHaveBeenCalledWith("alice", "secret1");
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    it("navigates to queryParams.returnUrl when present", async () => {
      await createComponent({ returnUrl: "/user/dataset" });
      component.setMode("signin");
      component.form.patchValue({ username: "alice", password: "secret1" });
      component.submit();
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith("/user/dataset");
    });

    it("surfaces the error message on login failure and does not navigate", () => {
      (userServiceMock.login as any).mockReturnValue(throwError(() => new Error("bad credentials")));
      component.form.patchValue({ username: "alice", password: "secret1" });
      component.submit();
      expect(component.errorMessage).toBe("bad credentials");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("falls back to a default message when the login error has no message", () => {
      (userServiceMock.login as any).mockReturnValue(throwError(() => ({})));
      component.form.patchValue({ username: "alice", password: "secret1" });
      component.submit();
      expect(component.errorMessage).toBe("Incorrect username or password");
    });
  });

  describe("submit -> register (sign-up mode)", () => {
    beforeEach(() => component.setMode("signup"));

    // Registration requires an email; the backend's register endpoint takes one.
    it("sets errorMessage when the email is missing or malformed", () => {
      component.form.patchValue({
        username: "alice",
        email: "not-an-email",
        password: "secret1",
        confirm: "secret1",
      });
      component.submit();
      expect(userServiceMock.register).not.toHaveBeenCalled();
      expect(component.errorMessage).toBeTruthy();
    });

    it("sets errorMessage when passwords are inconsistent", () => {
      component.form.patchValue({
        username: "alice",
        email: "alice@example.com",
        password: "secret1",
        confirm: "secret2",
      });
      component.submit();
      expect(userServiceMock.register).not.toHaveBeenCalled();
      expect(component.errorMessage).toBe("Two passwords are inconsistent.");
    });

    it("calls UserService.register with username, email and password, then notifies success", () => {
      component.form.patchValue({
        username: "  alice  ",
        email: "  alice@example.com  ",
        password: "secret1",
        confirm: "secret1",
      });
      component.submit();
      expect(userServiceMock.register).toHaveBeenCalledWith("alice", "alice@example.com", "secret1");
      expect(notificationServiceMock.success).toHaveBeenCalled();
    });

    it("surfaces the error message on registration failure", () => {
      (userServiceMock.register as any).mockReturnValue(throwError(() => new Error("username taken")));
      component.form.patchValue({
        username: "alice",
        email: "alice@example.com",
        password: "secret1",
        confirm: "secret1",
      });
      component.submit();
      expect(component.errorMessage).toBe("username taken");
    });
  });

  describe("google sign-in (authState)", () => {
    beforeEach(() => fixture.detectChanges());

    it("hands the id token to UserService.googleLogin and navigates", () => {
      authState$.next(googleUser("google-id-token"));
      expect(userServiceMock.googleLogin).toHaveBeenCalledWith("google-id-token");
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    // authState is a ReplaySubject that emits null on logout, so a stale null is replayed
    // into this subscription on arrival. Without the filter that would call googleLogin
    // with the id token of `undefined`.
    it("ignores a null auth state", () => {
      authState$.next(null);
      expect(userServiceMock.googleLogin).not.toHaveBeenCalled();
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("notifies and does not navigate when the google exchange fails", () => {
      (userServiceMock.googleLogin as any).mockReturnValue(throwError(() => new Error("google boom")));
      authState$.next(googleUser("google-id-token"));
      expect(notificationServiceMock.error).toHaveBeenCalledWith("google boom");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });
  });
});
