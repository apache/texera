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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
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
import { ORCID_STATE_KEY } from "../../../common/service/user/orcid-auth.service";
import { By } from "@angular/platform-browser";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabsComponent } from "ng-zorro-antd/tabs";

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

  // ORCID is authorization-code OAuth, so this page's whole job is the redirect: everything after
  // it happens on /callback/orcid and in the backend. What matters here is that the redirect is
  // well formed and that the `state` the callback verifies actually gets stashed first.
  describe("orcid sign-in", () => {
    const ORCID_CONFIG = { clientId: "APP-123", authorizeUrl: "https://sandbox.orcid.org/oauth/authorize" };

    /** Swaps window.location for the duration of `run`, per the pattern in app.component.spec.ts. */
    const withStubbedLocation = (run: (location: { origin: string; href: string }) => void) => {
      const original = window.location;
      const stub = { ...original, origin: "http://127.0.0.1:4200", href: "" } as unknown as {
        origin: string;
        href: string;
      };
      Object.defineProperty(window, "location", { configurable: true, value: stub });
      try {
        run(stub);
      } finally {
        Object.defineProperty(window, "location", { configurable: true, value: original });
      }
    };

    /** Answers the config fetch ngOnInit issues, which is what enables the button. */
    const flushOrcidConfig = (
      body: Record<string, string> | string = ORCID_CONFIG,
      status?: { status: number; statusText: string }
    ) => {
      const httpMock = TestBed.inject(HttpTestingController);
      const req = httpMock.expectOne(r => r.url.endsWith("/auth/orcid/config"));
      if (status) {
        req.flush(body, status);
      } else {
        req.flush(body);
      }
    };

    beforeEach(() => {
      sessionStorage.clear();
      fixture.detectChanges();
    });

    afterEach(() => sessionStorage.clear());

    it("redirects to ORCID with the client id, callback and a fresh state", () => {
      flushOrcidConfig();

      withStubbedLocation(location => {
        (component as any).orcidLogin();

        const url = new URL(location.href);
        expect(`${url.origin}${url.pathname}`).toBe(ORCID_CONFIG.authorizeUrl);
        expect(url.searchParams.get("client_id")).toBe("APP-123");
        expect(url.searchParams.get("response_type")).toBe("code");
        expect(url.searchParams.get("scope")).toBe("/authenticate");
        expect(url.searchParams.get("redirect_uri")).toBe("http://127.0.0.1:4200/callback/orcid");
        // The callback compares this against what comes back; it has to be stored before leaving.
        expect(url.searchParams.get("state")).toBe(sessionStorage.getItem(ORCID_STATE_KEY));
        expect(sessionStorage.getItem(ORCID_STATE_KEY)).toBeTruthy();
      });
    });

    it("uses a different state on each attempt", () => {
      flushOrcidConfig();

      const states: Array<string | null> = [];
      withStubbedLocation(() => {
        (component as any).orcidLogin();
        states.push(sessionStorage.getItem(ORCID_STATE_KEY));
        (component as any).orcidLogin();
        states.push(sessionStorage.getItem(ORCID_STATE_KEY));
      });

      expect(states[0]).not.toBe(states[1]);
    });

    // A deployment with no ORCID credentials reports the provider unavailable, which leaves the
    // button disabled — clicking it anyway must not send the user to a broken authorize URL.
    it("reports unavailable and does not redirect when the config fetch failed", () => {
      flushOrcidConfig("nope", { status: 503, statusText: "Service Unavailable" });

      withStubbedLocation(location => {
        (component as any).orcidLogin();

        expect(notificationServiceMock.error).toHaveBeenCalledWith("ORCID sign-in is unavailable");
        expect(location.href).toBe("");
        expect(sessionStorage.getItem(ORCID_STATE_KEY)).toBeNull();
      });
    });
  });

  // ──────────────────────────────────────────────────────────────────────────
  // Template rendering
  //
  // The suite above drives the class; these render the card. Each test sets every
  // provider flag explicitly so nothing is inherited from the mock's defaults.
  // ──────────────────────────────────────────────────────────────────────────
  describe("template", () => {
    function render(flags: { localLogin: boolean; googleLogin: boolean; orcidLogin: boolean }): void {
      (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig(flags);
      fixture.detectChanges();
    }

    const host = (): HTMLElement => fixture.nativeElement as HTMLElement;
    const input = (name: string): HTMLInputElement | null =>
      host().querySelector<HTMLInputElement>(`input[formcontrolname="${name}"]`);
    const submitButton = (): HTMLElement | null => host().querySelector("form button[type='submit']");
    const passwordIcons = () => fixture.debugElement.queryAll(By.css("nz-icon.ant-input-password-icon"));
    // NzIconDirective declares nzType as a setter with no getter, so read the icon name
    // off the base directive's own field rather than the input.
    const iconTypes = (): string[] =>
      passwordIcons().map(icon => (icon.injector.get(NzIconDirective) as unknown as { type: string }).type);

    const orcidButton = (): HTMLElement | null => host().querySelector("button.orcid-login");

    describe("provider flags", () => {
      it("renders the local form and both social buttons when all are enabled", () => {
        render({ localLogin: true, googleLogin: true, orcidLogin: true });

        expect(host().querySelector("nz-tabs")).toBeTruthy();
        expect(host().querySelector("form")).toBeTruthy();
        expect(host().querySelector("asl-google-signin-button")).toBeTruthy();
        expect(orcidButton()).toBeTruthy();
        // The "or continue with" divider only makes sense when both are offered.
        expect(host().querySelector("nz-divider")).toBeTruthy();
      });

      it("drops both social buttons but keeps the form when only local login is enabled", () => {
        render({ localLogin: true, googleLogin: false, orcidLogin: false });

        expect(host().querySelector("nz-tabs")).toBeTruthy();
        expect(host().querySelector("form")).toBeTruthy();
        expect(host().querySelector("asl-google-signin-button")).toBeNull();
        expect(orcidButton()).toBeNull();
        expect(host().querySelector("nz-divider")).toBeNull();
      });

      it("drops the tabs and the form but keeps the google button when only google is enabled", () => {
        render({ localLogin: false, googleLogin: true, orcidLogin: false });

        expect(host().querySelector("nz-tabs")).toBeNull();
        expect(host().querySelector("form")).toBeNull();
        expect(host().querySelector("asl-google-signin-button")).toBeTruthy();
        expect(orcidButton()).toBeNull();
        expect(host().querySelector("nz-divider")).toBeNull();
      });

      // ORCID carries the divider on its own: it is a second way to "continue with"
      // something other than the local form, so the label still reads correctly.
      it("keeps the orcid button and the divider when google is disabled but orcid is not", () => {
        render({ localLogin: true, googleLogin: false, orcidLogin: true });

        expect(host().querySelector("form")).toBeTruthy();
        expect(host().querySelector("asl-google-signin-button")).toBeNull();
        expect(orcidButton()).toBeTruthy();
        expect(host().querySelector("nz-divider")).toBeTruthy();
      });

      it("drops the tabs and the form but keeps the orcid button when only orcid is enabled", () => {
        render({ localLogin: false, googleLogin: false, orcidLogin: true });

        expect(host().querySelector("nz-tabs")).toBeNull();
        expect(host().querySelector("form")).toBeNull();
        expect(host().querySelector("asl-google-signin-button")).toBeNull();
        expect(orcidButton()).toBeTruthy();
        expect(host().querySelector("nz-divider")).toBeNull();
      });

      it("renders no sign-in path when every provider is disabled", () => {
        render({ localLogin: false, googleLogin: false, orcidLogin: false });

        expect(host().querySelector("nz-tabs")).toBeNull();
        expect(host().querySelector("form")).toBeNull();
        expect(host().querySelector("asl-google-signin-button")).toBeNull();
        expect(orcidButton()).toBeNull();
        expect(host().querySelector("nz-divider")).toBeNull();
        // The brand and footer are outside every flag, so the card is never empty.
        expect(host().querySelector(".brand")).toBeTruthy();
        expect(host().querySelector("p.foot")).toBeTruthy();
      });
    });

    describe("sign-in / sign-up mode", () => {
      beforeEach(() => render({ localLogin: true, googleLogin: true, orcidLogin: true }));

      it("shows only the sign-in fields by default", () => {
        expect(component.mode).toBe("signin");
        expect(input("username")).toBeTruthy();
        expect(input("password")).toBeTruthy();
        expect(input("email")).toBeNull();
        expect(input("confirm")).toBeNull();
        expect(host().querySelector("p.hint")).toBeNull();
        expect(submitButton()?.textContent?.trim()).toBe("Sign in");
      });

      it("adds the email, confirm and password-policy hint in sign-up mode", () => {
        component.setMode("signup");
        fixture.detectChanges();

        expect(input("email")).toBeTruthy();
        expect(input("confirm")).toBeTruthy();
        expect(host().querySelector("p.hint")?.textContent?.replace(/\s+/g, " ").trim()).toBe(
          "Password must be at least 6 characters. After registering, contact the Texera administrator to activate your account."
        );
        expect(submitButton()?.textContent?.trim()).toBe("Sign up");
      });

      it("switches mode from the tab strip in both directions", () => {
        const tabs = fixture.debugElement.query(By.css("nz-tabs"));

        tabs.triggerEventHandler("nzSelectedIndexChange", 1);
        fixture.detectChanges();
        expect(component.mode).toBe("signup");
        expect(input("confirm")).toBeTruthy();

        tabs.triggerEventHandler("nzSelectedIndexChange", 0);
        fixture.detectChanges();
        expect(component.mode).toBe("signin");
        expect(input("confirm")).toBeNull();
      });

      it("binds the selected tab to the current mode", () => {
        const selectedIndex = () =>
          fixture.debugElement.query(By.directive(NzTabsComponent)).componentInstance.nzSelectedIndex;

        expect(selectedIndex()).toBe(0);

        component.setMode("signup");
        fixture.detectChanges();
        expect(selectedIndex()).toBe(1);
      });

      it("submits the form through its ngSubmit binding", () => {
        const submitSpy = vi.spyOn(component, "submit").mockImplementation(() => {});

        fixture.debugElement.query(By.css("form")).triggerEventHandler("ngSubmit", new Event("submit"));

        expect(submitSpy).toHaveBeenCalledTimes(1);
      });
    });

    describe("password visibility", () => {
      beforeEach(() => {
        render({ localLogin: true, googleLogin: true, orcidLogin: true });
        component.setMode("signup");
        fixture.detectChanges();
      });

      it("starts hidden on both password fields", () => {
        expect(input("password")?.type).toBe("password");
        expect(input("confirm")?.type).toBe("password");
        // The suffix template is reused by both groups, so both icons render.
        expect(passwordIcons().length).toBe(2);
        expect(iconTypes()).toEqual(["eye-invisible", "eye-invisible"]);
      });

      it("reveals both password fields when the toggle is clicked", () => {
        passwordIcons()[0].triggerEventHandler("click", new MouseEvent("click"));
        fixture.detectChanges();

        expect(component.passwordVisible).toBe(true);
        expect(input("password")?.type).toBe("text");
        expect(input("confirm")?.type).toBe("text");
        expect(iconTypes()).toEqual(["eye", "eye"]);
      });

      it("also toggles from the keyboard, so the control is reachable without a mouse", () => {
        passwordIcons()[0].triggerEventHandler("keydown.enter", new KeyboardEvent("keydown", { key: "Enter" }));
        fixture.detectChanges();

        expect(component.passwordVisible).toBe(true);
        expect(input("password")?.type).toBe("text");
      });

      it("hides them again on a second click", () => {
        passwordIcons()[0].triggerEventHandler("click", new MouseEvent("click"));
        fixture.detectChanges();
        passwordIcons()[0].triggerEventHandler("click", new MouseEvent("click"));
        fixture.detectChanges();

        expect(component.passwordVisible).toBe(false);
        expect(input("password")?.type).toBe("password");
        expect(input("confirm")?.type).toBe("password");
      });
    });
  });
});
