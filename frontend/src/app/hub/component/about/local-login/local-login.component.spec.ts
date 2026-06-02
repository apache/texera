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
import { FormControl } from "@angular/forms";
import { ActivatedRoute, ActivatedRouteSnapshot, Router } from "@angular/router";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of, throwError } from "rxjs";

import { LocalLoginComponent } from "./local-login.component";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../../common/service/gui-config.service.mock";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { USER_WORKFLOW } from "../../../../app-routing.constant";

describe("LocalLoginComponent", () => {
  let component: LocalLoginComponent;
  let fixture: ComponentFixture<LocalLoginComponent>;

  let userServiceMock: Partial<UserService>;
  let notificationServiceMock: Partial<NotificationService>;
  let routerMock: Partial<Router>;
  let activatedRouteMock: { snapshot: Partial<ActivatedRouteSnapshot> };

  const createComponent = async (queryParams: Record<string, any> = {}) => {
    TestBed.resetTestingModule();
    userServiceMock = {
      login: vi.fn().mockReturnValue(of(undefined)),
      register: vi.fn().mockReturnValue(of(undefined)),
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

    await TestBed.configureTestingModule({
      imports: [LocalLoginComponent, HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: Router, useValue: routerMock },
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(LocalLoginComponent);
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

  describe("form construction", () => {
    it("builds allForms with the expected controls", () => {
      const controls = component.allForms.controls;
      expect(Object.keys(controls).sort()).toEqual(
        [
          "loginPassword",
          "loginUsername",
          "registerConfirmationPassword",
          "registerPassword",
          "registerUsername",
        ].sort()
      );
    });

    it("requires loginUsername and registerUsername", () => {
      const loginUsername = component.allForms.get("loginUsername")!;
      const registerUsername = component.allForms.get("registerUsername")!;
      loginUsername.setValue("");
      registerUsername.setValue("");
      expect(loginUsername.hasError("required")).toBe(true);
      expect(registerUsername.hasError("required")).toBe(true);
    });

    it("requires passwords and enforces minLength(6)", () => {
      const loginPassword = component.allForms.get("loginPassword")!;
      const registerPassword = component.allForms.get("registerPassword")!;
      loginPassword.setValue("");
      registerPassword.setValue("");
      expect(loginPassword.hasError("required")).toBe(true);
      expect(registerPassword.hasError("required")).toBe(true);

      loginPassword.setValue("12345");
      registerPassword.setValue("12345");
      expect(loginPassword.hasError("minlength")).toBe(true);
      expect(registerPassword.hasError("minlength")).toBe(true);

      loginPassword.setValue("123456");
      registerPassword.setValue("123456");
      expect(loginPassword.valid).toBe(true);
      expect(registerPassword.valid).toBe(true);
    });

    it("wires the confirmationValidator on registerConfirmationPassword", () => {
      const registerPassword = component.allForms.get("registerPassword")!;
      const registerConfirmationPassword = component.allForms.get("registerConfirmationPassword")!;
      registerPassword.setValue("abcdef");
      registerConfirmationPassword.setValue("zzzzzz");
      registerConfirmationPassword.updateValueAndValidity();
      expect(registerConfirmationPassword.hasError("confirm")).toBe(true);

      registerConfirmationPassword.setValue("abcdef");
      registerConfirmationPassword.updateValueAndValidity();
      expect(registerConfirmationPassword.hasError("confirm")).toBe(false);
    });

    it("requires registerConfirmationPassword to be non-empty", () => {
      const registerConfirmationPassword = component.allForms.get("registerConfirmationPassword")!;
      registerConfirmationPassword.setValue("");
      expect(registerConfirmationPassword.hasError("required")).toBe(true);
    });
  });

  describe("confirmationValidator", () => {
    it("returns { confirm: true } when the value does not match registerPassword", () => {
      component.allForms.get("registerPassword")!.setValue("password1");
      const control = new FormControl("password2");
      expect(component.confirmationValidator(control as FormControl)).toEqual({ confirm: true });
    });

    it("returns {} when the value matches registerPassword", () => {
      component.allForms.get("registerPassword")!.setValue("password1");
      const control = new FormControl("password1");
      expect(component.confirmationValidator(control as FormControl)).toEqual({});
    });
  });

  describe("updateConfirmValidator", () => {
    it("schedules updateValueAndValidity on registerConfirmationPassword via setTimeout", () => {
      vi.useFakeTimers();
      try {
        const control = component.allForms.controls.registerConfirmationPassword;
        const updateSpy = vi.spyOn(control, "updateValueAndValidity");
        component.updateConfirmValidator();
        expect(updateSpy).not.toHaveBeenCalled();
        vi.runAllTimers();
        expect(updateSpy).toHaveBeenCalledTimes(1);
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("ngOnInit", () => {
    it("patches loginUsername and loginPassword from defaultLocalUser when populated", () => {
      const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      config.setConfig({ defaultLocalUser: { username: "preset-user", password: "preset-pass" } });

      component.ngOnInit();

      expect(component.allForms.get("loginUsername")!.value).toBe("preset-user");
      expect(component.allForms.get("loginPassword")!.value).toBe("preset-pass");
    });

    it("does not patch login fields when defaultLocalUser is empty", () => {
      const config = TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService;
      config.setConfig({ defaultLocalUser: {} });

      component.ngOnInit();

      expect(component.allForms.get("loginUsername")!.value).toBe("");
      expect(component.allForms.get("loginPassword")!.value).toBe("");
    });
  });

  describe("login", () => {
    it("sets loginErrorMessage and short-circuits when validateUsername fails", () => {
      const validateSpy = vi.spyOn(UserService, "validateUsername").mockReturnValue({
        result: false,
        message: "Username should not be empty.",
      });
      component.allForms.patchValue({ loginUsername: "", loginPassword: "123456" });

      component.login();

      expect(validateSpy).toHaveBeenCalledWith("");
      expect(component.loginErrorMessage).toBe("Username should not be empty.");
      expect(userServiceMock.login).not.toHaveBeenCalled();
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
      validateSpy.mockRestore();
    });

    it("calls UserService.login with trimmed username and navigates to USER_WORKFLOW on success", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.allForms.patchValue({ loginUsername: "  alice  ", loginPassword: "secret" });

      component.login();

      expect(userServiceMock.login).toHaveBeenCalledWith("alice", "secret");
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
      expect(component.loginErrorMessage).toBeUndefined();
    });

    it("navigates to queryParams.returnUrl when present", async () => {
      await createComponent({ returnUrl: "/custom/return" });
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.allForms.patchValue({ loginUsername: "alice", loginPassword: "secret" });

      component.login();

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith("/custom/return");
    });

    it("surfaces the error's message via NotificationService.error on failure", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.login!).mockReturnValueOnce(throwError(() => new Error("boom")));
      component.allForms.patchValue({ loginUsername: "alice", loginPassword: "secret" });

      component.login();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("boom");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("falls back to 'Incorrect username or password' when the error has no message", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.login!).mockReturnValueOnce(throwError(() => ({})));
      component.allForms.patchValue({ loginUsername: "alice", loginPassword: "secret" });

      component.login();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("Incorrect username or password");
    });
  });

  describe("register", () => {
    it("sets registerErrorMessage when the password is shorter than 6 characters", () => {
      const validateSpy = vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.allForms.patchValue({
        registerUsername: "alice",
        registerPassword: "abc",
        registerConfirmationPassword: "abc",
      });

      component.register();

      expect(component.registerErrorMessage).toBe("Password length should be greater than 5");
      expect(userServiceMock.register).not.toHaveBeenCalled();
      validateSpy.mockRestore();
    });

    it("sets registerErrorMessage when passwords do not match", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.allForms.patchValue({
        registerUsername: "alice",
        registerPassword: "abcdef",
        registerConfirmationPassword: "ghijkl",
      });

      component.register();

      expect(component.registerErrorMessage).toBe("Passwords do not match");
      expect(userServiceMock.register).not.toHaveBeenCalled();
    });

    it("sets registerErrorMessage when validateUsername fails", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({
        result: false,
        message: "Username should not be empty.",
      });
      component.allForms.patchValue({
        registerUsername: "",
        registerPassword: "abcdef",
        registerConfirmationPassword: "abcdef",
      });

      component.register();

      expect(component.registerErrorMessage).toBe("Username should not be empty.");
      expect(userServiceMock.register).not.toHaveBeenCalled();
    });

    it("calls UserService.register with the trimmed username and surfaces a success notification", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      component.allForms.patchValue({
        registerUsername: "  alice  ",
        registerPassword: "abcdef",
        registerConfirmationPassword: "abcdef",
      });

      component.register();

      expect(userServiceMock.register).toHaveBeenCalledWith("alice", "abcdef");
      expect(notificationServiceMock.success).toHaveBeenCalledWith(
        "Your account has been created. Please contact the Texera administrator to activate your account."
      );
      expect(component.registerErrorMessage).toBeUndefined();
    });

    it("surfaces the error's message via NotificationService.error on failure", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.register!).mockReturnValueOnce(throwError(() => new Error("nope")));
      component.allForms.patchValue({
        registerUsername: "alice",
        registerPassword: "abcdef",
        registerConfirmationPassword: "abcdef",
      });

      component.register();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("nope");
      expect(notificationServiceMock.success).not.toHaveBeenCalled();
    });

    it("falls back to 'Registration failed' when the error has no message", () => {
      vi.spyOn(UserService, "validateUsername").mockReturnValue({ result: true, message: "ok" });
      vi.mocked(userServiceMock.register!).mockReturnValueOnce(throwError(() => ({})));
      component.allForms.patchValue({
        registerUsername: "alice",
        registerPassword: "abcdef",
        registerConfirmationPassword: "abcdef",
      });

      component.register();

      expect(notificationServiceMock.error).toHaveBeenCalledWith("Registration failed");
    });
  });
});
