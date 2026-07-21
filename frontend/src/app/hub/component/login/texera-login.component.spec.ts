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
import { SocialAuthService, FacebookLoginProvider, SocialUser } from "@abacritt/angularx-social-login";

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
  let activatedRouteMock: { snapshot: Partial<ActivatedRouteSnapshot> };
  let socialAuthServiceMock: Partial<SocialAuthService>;
  let authState$: Subject<SocialUser>;

  const facebookUser = (authToken: string): SocialUser =>
    ({ provider: FacebookLoginProvider.PROVIDER_ID, authToken }) as unknown as SocialUser;
  const googleUser = (idToken: string): SocialUser => ({ provider: "GOOGLE", idToken }) as unknown as SocialUser;

  const createComponent = async (queryParams: Record<string, any> = {}) => {
    TestBed.resetTestingModule();
    authState$ = new Subject<SocialUser>();
    userServiceMock = {
      login: vi.fn().mockReturnValue(of(undefined)),
      register: vi.fn().mockReturnValue(of(undefined)),
      googleLogin: vi.fn().mockReturnValue(of(undefined)),
      facebookLogin: vi.fn().mockReturnValue(of(undefined)),
    };
    notificationServiceMock = {
      error: vi.fn(),
      success: vi.fn(),
    };
    routerMock = {
      navigateByUrl: vi.fn(),
    };
    activatedRouteMock = {
      snapshot: { queryParams } as Partial<ActivatedRouteSnapshot>,
    };
    socialAuthServiceMock = {
      authState: authState$.asObservable(),
      // GoogleSigninButtonDirective subscribes to initState in its constructor;
      // EMPTY keeps the subscription open without triggering google.accounts.id.renderButton.
      initState: EMPTY,
      signIn: vi.fn().mockResolvedValue(facebookUser("fb-token")),
    };

    await TestBed.configureTestingModule({
      imports: [TexeraLoginComponent, HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: Router, useValue: routerMock },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
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
      component.form.get("password")!.setValue("abcdef");
      const confirm = component.form.get("confirm")!;
      confirm.setValue("zzzzzz");
      confirm.updateValueAndValidity();
      expect(confirm.hasError("confirm")).toBe(true);

      confirm.setValue("abcdef");
      confirm.updateValueAndValidity();
      expect(confirm.hasError("confirm")).toBe(false);
    });

    it("does not flag a mismatch in sign-in mode", () => {
      component.setMode("signin");
      component.form.get("password")!.setValue("abcdef");
      const confirm = component.form.get("confirm")!;
      confirm.setValue("different");
      confirm.updateValueAndValidity();
      expect(confirm.hasError("confirm")).toBe(false);
    });
  });

  describe("submit -> login (sign-in mode)", () => {
    beforeEach(() => component.setMode("signin"));

    it("short-circuits and sets errorMessage when validateUsername fails", () => {
      const validateSpy = vi
        .spyOn(UserService, "validateUsername")
        .mockReturnValue({ result: false, message: "Username should not be empty." });
      component.form.patchValue({ username: "", password: "123456" });

      component.submit();

      expect(validateSpy).toHaveBeenCalledWith("");
      expect(component.errorMessage).toBe("Username should not be empty.");
      expect(userServiceMock.login).not.toHaveBeenCalled();
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("sets errorMessage when the password is shorter than 6 characters", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.form.patchValue({ username: "alice", password: "12345" });

      component.submit();

      expect(component.errorMessage).toBe("Password length should be greater than 5.");
      expect(userServiceMock.login).not.toHaveBeenCalled();
    });

    it("calls UserService.login with a trimmed username and navigates to USER_WORKFLOW on success", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.form.patchValue({ username: "  alice  ", password: "secret" });

      component.submit();

      expect(userServiceMock.login).toHaveBeenCalledWith("alice", "secret");
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
      expect(component.errorMessage).toBeUndefined();
    });

    it("navigates to queryParams.returnUrl when present", async () => {
      await createComponent({ returnUrl: "/custom/return" });
      component.setMode("signin");
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.form.patchValue({ username: "alice", password: "secret" });

      component.submit();

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith("/custom/return");
    });

    it("surfaces the error message on login failure and does not navigate", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.login!).mockReturnValueOnce(throwError(() => new Error("boom")));
      component.form.patchValue({ username: "alice", password: "secret" });

      component.submit();

      expect(component.errorMessage).toBe("boom");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("falls back to a default message when the login error has no message", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.login!).mockReturnValueOnce(throwError(() => ({})));
      component.form.patchValue({ username: "alice", password: "secret" });

      component.submit();

      expect(component.errorMessage).toBe("Incorrect username or password");
    });
  });

  describe("submit -> register (sign-up mode)", () => {
    beforeEach(() => component.setMode("signup"));

    it("sets errorMessage when passwords are inconsistent", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.form.patchValue({ username: "alice", password: "abcdef", confirm: "ghijkl" });

      component.submit();

      expect(component.errorMessage).toBe("Two passwords are inconsistent.");
      expect(userServiceMock.register).not.toHaveBeenCalled();
    });

    it("calls UserService.register with a trimmed username and shows a success notification", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.form.patchValue({ username: "  alice  ", password: "abcdef", confirm: "abcdef" });

      component.submit();

      expect(userServiceMock.register).toHaveBeenCalledWith("alice", "abcdef");
      expect(notificationServiceMock.success).toHaveBeenCalledWith(
        "Your account has been created. Please contact the Texera administrator to activate your account."
      );
      expect(component.errorMessage).toBeUndefined();
    });

    it("surfaces the error message on registration failure", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.register!).mockReturnValueOnce(throwError(() => new Error("nope")));
      component.form.patchValue({ username: "alice", password: "abcdef", confirm: "abcdef" });

      component.submit();

      expect(component.errorMessage).toBe("nope");
      expect(notificationServiceMock.success).not.toHaveBeenCalled();
    });
  });

  describe("social sign-in (authState)", () => {
    it("routes a Facebook auth event to UserService.facebookLogin and navigates", () => {
      component.ngOnInit();

      authState$.next(facebookUser("fb-token"));

      expect(userServiceMock.facebookLogin).toHaveBeenCalledWith("fb-token");
      expect(userServiceMock.googleLogin).not.toHaveBeenCalled();
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    it("routes a Google auth event to UserService.googleLogin and navigates", () => {
      component.ngOnInit();

      authState$.next(googleUser("g-token"));

      expect(userServiceMock.googleLogin).toHaveBeenCalledWith("g-token");
      expect(userServiceMock.facebookLogin).not.toHaveBeenCalled();
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    it("notifies and does not navigate when the social login call fails", () => {
      vi.mocked(userServiceMock.facebookLogin!).mockReturnValueOnce(throwError(() => new Error("fb boom")));
      component.ngOnInit();

      authState$.next(facebookUser("fb-token"));

      expect(notificationServiceMock.error).toHaveBeenCalledWith("fb boom");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });
  });

  describe("facebookLogin()", () => {
    it("triggers the Facebook sign-in flow via SocialAuthService", () => {
      component.facebookLogin();
      expect(socialAuthServiceMock.signIn).toHaveBeenCalledWith(FacebookLoginProvider.PROVIDER_ID);
    });

    it("notifies when SocialAuthService.signIn rejects", async () => {
      vi.mocked(socialAuthServiceMock.signIn!).mockRejectedValueOnce(new Error("sign-in refused"));

      component.facebookLogin();
      await Promise.resolve();
      await Promise.resolve();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("sign-in refused");
    });
  });
});
