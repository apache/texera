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

import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { firstValueFrom, Observable } from "rxjs";
import { JwtHelperService } from "@auth0/angular-jwt";
import { NzModalService } from "ng-zorro-antd/modal";
import { AppSettings } from "../../app-setting";
import { Role } from "../../type/user";
import { AuthService, TOKEN_KEY } from "./auth.service";
import { NotificationService } from "../notification/notification.service";
import { GmailService } from "../gmail/gmail.service";
import { GuiConfigService } from "../gui-config.service";

describe("AuthService", () => {
  let service: AuthService;
  let httpMock: HttpTestingController;
  let jwt: {
    isTokenExpired: ReturnType<typeof vi.fn>;
    decodeToken: ReturnType<typeof vi.fn>;
    getTokenExpirationDate: ReturnType<typeof vi.fn>;
  };
  let notification: { error: ReturnType<typeof vi.fn> };
  let config: { env: { inviteOnly: boolean; emailVerification: boolean } };
  let modal: { info: ReturnType<typeof vi.fn>; create: ReturnType<typeof vi.fn> };

  const api = AppSettings.getApiEndpoint();
  const claims = {
    role: Role.REGULAR,
    userId: 5,
    email: "u@x.com",
    sub: "Ursula",
    googleId: "g",
    avatar: "a",
    comment: "c",
    joiningReason: "r",
  };

  beforeEach(() => {
    localStorage.clear();
    jwt = {
      isTokenExpired: vi.fn().mockReturnValue(false),
      decodeToken: vi.fn().mockReturnValue(claims),
      getTokenExpirationDate: vi.fn().mockReturnValue(new Date(Date.now() + 60_000)),
    };
    notification = { error: vi.fn() };
    config = { env: { inviteOnly: false, emailVerification: false } };
    modal = { info: vi.fn(), create: vi.fn() };

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        AuthService,
        { provide: JwtHelperService, useValue: jwt },
        { provide: NotificationService, useValue: notification },
        { provide: GmailService, useValue: {} },
        { provide: GuiConfigService, useValue: config },
        { provide: NzModalService, useValue: modal },
      ],
    });
    service = TestBed.inject(AuthService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // loginWithExistingToken()'s valid-token path schedules an auto-logout timer;
    // logout() unsubscribes it so no real timer leaks into later tests / keeps Vitest alive.
    service.logout();
    httpMock.verify();
    localStorage.clear();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("static access-token helpers", () => {
    it("set/get/remove roundtrip through localStorage", () => {
      expect(AuthService.getAccessToken()).toBeNull();

      AuthService.setAccessToken("tok");
      expect(localStorage.getItem(TOKEN_KEY)).toEqual("tok");
      expect(AuthService.getAccessToken()).toEqual("tok");

      AuthService.removeAccessToken();
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });

  describe("HTTP auth endpoints", () => {
    it("register() POSTs username/email/password to the register endpoint", () => {
      service.register("alice", "alice@example.com", "pw").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.REGISTER_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({ username: "alice", email: "alice@example.com", password: "pw" });
      req.flush({ accessToken: "t" });
    });

    it("auth() POSTs username/password to the login endpoint", () => {
      service.auth("alice", "pw").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({ username: "alice", password: "pw" });
      req.flush({ accessToken: "t" });
    });

    it("googleAuth() POSTs the raw credential with a text/plain content type", () => {
      service.googleAuth("cred").subscribe();
      const req = httpMock.expectOne(`${api}/${AuthService.GOOGLE_LOGIN_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual("cred");
      expect(req.request.headers.get("Content-Type")).toEqual("text/plain");
      req.flush({ accessToken: "t" });
    });

    // Annotated because the calls return differently-shaped observables (a registration result vs a
    // token); without it the inferred union of call signatures is not callable.
    const errorCases: { name: string; call: () => Observable<unknown>; endpoint: string }[] = [
      {
        name: "register",
        call: () => service.register("alice", "alice@example.com", "pw"),
        endpoint: AuthService.REGISTER_ENDPOINT,
      },
      { name: "auth", call: () => service.auth("alice", "pw"), endpoint: AuthService.LOGIN_ENDPOINT },
      { name: "googleAuth", call: () => service.googleAuth("cred"), endpoint: AuthService.GOOGLE_LOGIN_ENDPOINT },
    ];

    errorCases.forEach(({ name, call, endpoint }) => {
      it(`${name}() propagates HTTP errors to the subscriber`, () => {
        const onError = vi.fn();
        call().subscribe({ error: onError });

        const req = httpMock.expectOne(`${api}/${endpoint}`);
        req.flush("nope", { status: 401, statusText: "Unauthorized" });

        expect(onError).toHaveBeenCalledTimes(1);
        expect(onError.mock.calls[0][0].status).toEqual(401);
      });
    });
  });

  describe("logout", () => {
    it("removes the stored access token and returns undefined", () => {
      AuthService.setAccessToken("tok");
      expect(service.logout()).toBeUndefined();
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });

  describe("loginWithExistingToken", () => {
    it("logs out and returns undefined when no token is stored", () => {
      expect(service.loginWithExistingToken()).toBeUndefined();
    });

    it("errors and logs out when the stored token is expired", () => {
      AuthService.setAccessToken("tok");
      jwt.isTokenExpired.mockReturnValue(true);

      const result = service.loginWithExistingToken();

      expect(result).toBeUndefined();
      expect(notification.error).toHaveBeenCalledWith("Access token is expired!");
      expect(AuthService.getAccessToken()).toBeNull();
    });

    it("returns a User built from the decoded claims for a valid token", () => {
      AuthService.setAccessToken("tok");

      const user = service.loginWithExistingToken();

      expect(user).toEqual({
        uid: 5,
        name: "Ursula",
        email: "u@x.com",
        googleId: "g",
        avatar: "a",
        role: Role.REGULAR,
        comment: "c",
        joiningReason: "r",
      });
    });

    it("reads the avatar from a pre-rename googleAvatar claim", () => {
      AuthService.setAccessToken("tok");
      const { avatar, ...legacyClaims } = claims;
      jwt.decodeToken.mockReturnValue({ ...legacyClaims, googleAvatar: "legacy-a" });

      expect(service.loginWithExistingToken()?.avatar).toEqual("legacy-a");
    });

    it("in invite-only mode, an inactive user is logged out and registration is checked", () => {
      AuthService.setAccessToken("tok");
      config.env.inviteOnly = true;
      jwt.decodeToken.mockReturnValue({ ...claims, role: Role.INACTIVE });

      const result = service.loginWithExistingToken();

      expect(result).toBeUndefined();
      const req = httpMock.expectOne(r => r.url === `${api}/user/joining-reason/required`);
      expect(req.request.params.get("uid")).toEqual("5");
      req.flush(false);

      expect(modal.info).toHaveBeenCalledTimes(1);
      expect(AuthService.getAccessToken()).toBeNull();
    });
  });

  // Every path that writes a credential supplies an address, so this should be unreachable — it
  // covers the rows older deployments carry from the admin panel's "add user" before that stopped
  // creating credentialed accounts. The contract under test: such a token yields no `User` at all,
  // rather than one with the field blank.
  describe("the missing-email prompt", () => {
    const emaillessClaims = (role: Role = Role.REGULAR) => ({ ...claims, email: null, role });

    beforeEach(() => {
      modal.create.mockReturnValue({
        getContentComponent: () => ({ modalTitle: "t", getValues: () => ({ email: "typed@x.com" }) }),
        updateConfig: vi.fn(),
      });
    });

    // The load-bearing assertion: no User escapes while the account has no address, so nothing
    // downstream ever sees `User.email` absent and the type stays honest.
    it("hands out no user and opens the prompt when the token carries no email", () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());

      const result = service.loginWithExistingToken();

      expect(result).toBeUndefined();
      expect(modal.create).toHaveBeenCalledTimes(1);
      expect(modal.create.mock.calls[0][0].nzData).toMatchObject({ name: "Ursula" });
      // The token stays put so the prompt can spend it on PUT /auth/email.
      expect(AuthService.getAccessToken()).toEqual("tok");
    });

    it("does not open the prompt for an account that has an address", () => {
      AuthService.setAccessToken("tok");

      service.loginWithExistingToken();

      expect(modal.create).not.toHaveBeenCalled();
    });

    // loginWithExistingToken runs on every token refresh; a second dialog stacked on the one still
    // waiting for an answer would be unanswerable.
    it("does not stack a second prompt while one is open", () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());

      service.loginWithExistingToken();
      service.loginWithExistingToken();

      expect(modal.create).toHaveBeenCalledTimes(1);
    });

    // The address has to be collected before the invite-only branch, not after: that branch mails
    // the admin the account's address, and /gmail/notify-unauthorized rejects a null one.
    it("asks for an address instead of making the invite-only request", () => {
      AuthService.setAccessToken("tok");
      config.env.inviteOnly = true;
      jwt.decodeToken.mockReturnValue(emaillessClaims(Role.INACTIVE));

      expect(service.loginWithExistingToken()).toBeUndefined();

      expect(modal.create).toHaveBeenCalledTimes(1);
      httpMock.expectNone(r => r.url === `${api}/user/joining-reason/required`);
    });

    it("still runs the invite-only request for an inactive user that has an address", () => {
      AuthService.setAccessToken("tok");
      config.env.inviteOnly = true;
      jwt.decodeToken.mockReturnValue({ ...claims, role: Role.INACTIVE });

      service.loginWithExistingToken();

      expect(modal.create).not.toHaveBeenCalled();
      httpMock.expectOne(r => r.url === `${api}/user/joining-reason/required`).flush(false);
    });

    it("stores the reissued token and announces it once the address is saved", async () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());
      const announced = firstValueFrom(service.sessionChanged());

      service.loginWithExistingToken();
      const accepted = modal.create.mock.calls[0][0].nzOnOk();

      const req = httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`);
      expect(req.request.method).toEqual("PUT");
      expect(req.request.body).toEqual({ email: "typed@x.com" });
      req.flush({ accessToken: "fresh-token" });

      expect(await accepted).toBe(true);
      expect(AuthService.getAccessToken()).toEqual("fresh-token");
      await expect(announced).resolves.toBeUndefined();
    });

    // The reissued token carries an address, so the very next pass produces a real user — which is
    // what turns the prompt into a way through rather than a dead end.
    it("hands out a user once the reissued token carries an address", async () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());

      service.loginWithExistingToken();
      const accepted = modal.create.mock.calls[0][0].nzOnOk();
      httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`).flush({ accessToken: "fresh-token" });
      await accepted;

      jwt.decodeToken.mockReturnValue(claims);
      expect(service.loginWithExistingToken()).toMatchObject({ email: claims.email });
    });

    it("keeps the dialog open and reports why when the address is refused", async () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());

      service.loginWithExistingToken();
      const accepted = modal.create.mock.calls[0][0].nzOnOk();

      httpMock
        .expectOne(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`)
        .flush(
          { message: "That email address already belongs to an account." },
          { status: 409, statusText: "Conflict" }
        );

      expect(await accepted).toBe(false);
      expect(notification.error).toHaveBeenCalledWith("That email address already belongs to an account.");
      // The old token is untouched, so the user can try another address.
      expect(AuthService.getAccessToken()).toEqual("tok");
    });

    it("rejects a malformed address without calling the backend", async () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());
      modal.create.mockReturnValue({
        getContentComponent: () => ({ modalTitle: "t", getValues: () => ({ email: "not-an-address" }) }),
        updateConfig: vi.fn(),
      });

      service.loginWithExistingToken();

      expect(await modal.create.mock.calls[0][0].nzOnOk()).toBe(false);
      expect(notification.error).toHaveBeenCalledWith("Email format is invalid.");
      httpMock.expectNone(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`);
    });

    it("signs out and announces the change when the prompt is cancelled", async () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());
      const announced = firstValueFrom(service.sessionChanged());

      service.loginWithExistingToken();
      modal.create.mock.calls[0][0].nzOnCancel();

      expect(AuthService.getAccessToken()).toBeNull();
      await expect(announced).resolves.toBeUndefined();
    });

    // A second prompt has to be possible after a cancel, or a user who dismissed it once could
    // never be asked again for the life of the tab.
    it("can prompt again after a cancel", () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());

      service.loginWithExistingToken();
      modal.create.mock.calls[0][0].nzOnCancel();
      AuthService.setAccessToken("tok");
      service.loginWithExistingToken();

      expect(modal.create).toHaveBeenCalledTimes(2);
    });
  });

  // Where `user-sys.email-verification` is on the dialog runs in two steps: the first mails a code
  // and the second presents it. Nothing is stored server-side in between, so the address has to
  // travel with the code on the second call.
  describe("the missing-email prompt with verification on", () => {
    const emaillessClaims = () => ({ ...claims, email: null });
    let content: { modalTitle: string; step: string; getValues: () => { email: string; code: string } };
    let updateConfig: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      config.env.emailVerification = true;
      updateConfig = vi.fn();
      content = {
        modalTitle: "t",
        step: "address",
        getValues: () => ({ email: "typed@x.com", code: content.step === "code" ? "123456" : "" }),
      };
      modal.create.mockReturnValue({ getContentComponent: () => content, updateConfig });
    });

    const openPrompt = () => {
      AuthService.setAccessToken("tok");
      jwt.decodeToken.mockReturnValue(emaillessClaims());
      service.loginWithExistingToken();
    };

    it("mails a code on the first confirm, then sends it with the address on the second", async () => {
      openPrompt();
      expect(modal.create.mock.calls[0][0].nzOkText).toBe("Send code");

      // First confirm: a code goes out and the dialog stays open on the code step.
      const first = modal.create.mock.calls[0][0].nzOnOk();
      const codeReq = httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_CODE_ENDPOINT}`);
      expect(codeReq.request.method).toEqual("POST");
      expect(codeReq.request.body).toEqual({ email: "typed@x.com" });
      codeReq.flush(null);

      // False keeps the modal open; the address is not written yet.
      expect(await first).toBe(false);
      expect(content.step).toBe("code");
      expect(updateConfig).toHaveBeenCalledWith({ nzOkText: "Verify" });

      // Second confirm: the address travels with the code, not a server-side pending row.
      const second = modal.create.mock.calls[0][0].nzOnOk();
      const setReq = httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`);
      expect(setReq.request.method).toEqual("PUT");
      expect(setReq.request.body).toEqual({ email: "typed@x.com", code: "123456" });
      setReq.flush({ accessToken: "fresh-token" });

      expect(await second).toBe(true);
      expect(AuthService.getAccessToken()).toEqual("fresh-token");
    });

    it("keeps the dialog on the address step and reports why when the code cannot be sent", async () => {
      openPrompt();

      const accepted = modal.create.mock.calls[0][0].nzOnOk();
      httpMock
        .expectOne(`${api}/${AuthService.SET_EMAIL_CODE_ENDPOINT}`)
        .flush({ message: "A code was just sent." }, { status: 429, statusText: "Too Many Requests" });

      expect(await accepted).toBe(false);
      expect(content.step).toBe("address");
      expect(notification.error).toHaveBeenCalledWith("A code was just sent.");
    });

    it("rejects a malformed address before asking for a code", async () => {
      openPrompt();
      content.getValues = () => ({ email: "nope", code: "" });

      const accepted = modal.create.mock.calls[0][0].nzOnOk();

      expect(await accepted).toBe(false);
      expect(notification.error).toHaveBeenCalledWith("Email format is invalid.");
      httpMock.expectNone(`${api}/${AuthService.SET_EMAIL_CODE_ENDPOINT}`);
    });

    it("reports a refused code and keeps the dialog open", async () => {
      openPrompt();
      const first = modal.create.mock.calls[0][0].nzOnOk();
      httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_CODE_ENDPOINT}`).flush(null);
      await first;

      const accepted = modal.create.mock.calls[0][0].nzOnOk();
      httpMock
        .expectOne(`${api}/${AuthService.SET_EMAIL_ENDPOINT}`)
        .flush({ message: "That code is not valid or has expired." }, { status: 406, statusText: "Not Acceptable" });

      expect(await accepted).toBe(false);
      expect(notification.error).toHaveBeenCalledWith("That code is not valid or has expired.");
    });
  });

  describe("registration endpoints", () => {
    it("registerVerify() POSTs the same fields plus the code", () => {
      service.registerVerify("alice", "alice@example.com", "pw", "123456").subscribe();

      const req = httpMock.expectOne(`${api}/${AuthService.REGISTER_VERIFY_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({
        username: "alice",
        email: "alice@example.com",
        password: "pw",
        code: "123456",
      });
      req.flush({ accessToken: "tok" });
    });

    it("requestEmailCode() POSTs the address to the code endpoint", () => {
      service.requestEmailCode("a@b.com").subscribe();

      const req = httpMock.expectOne(`${api}/${AuthService.SET_EMAIL_CODE_ENDPOINT}`);
      expect(req.request.method).toEqual("POST");
      expect(req.request.body).toEqual({ email: "a@b.com" });
      req.flush(null);
    });
  });

  describe("registerAutoLogout", () => {
    afterEach(() => {
      // Restore real timers before the outer afterEach runs logout()/verify().
      vi.useRealTimers();
    });

    it("schedules a logout that fires once the token expiry elapses", () => {
      vi.useFakeTimers();
      AuthService.setAccessToken("tok");
      jwt.isTokenExpired.mockReturnValue(false);
      // Expiry one second into the (frozen) fake clock.
      jwt.getTokenExpirationDate.mockReturnValue(new Date(Date.now() + 1000));
      const logoutSpy = vi.spyOn(service, "logout");

      (service as any).registerAutoLogout();
      expect(logoutSpy).not.toHaveBeenCalled();

      vi.advanceTimersByTime(1000);
      expect(logoutSpy).toHaveBeenCalledTimes(1);
    });

    it("does not schedule a logout when the token is already expired", () => {
      vi.useFakeTimers();
      AuthService.setAccessToken("tok");
      jwt.isTokenExpired.mockReturnValue(true);
      const logoutSpy = vi.spyOn(service, "logout");

      (service as any).registerAutoLogout();
      vi.advanceTimersByTime(1_000_000);

      expect(logoutSpy).not.toHaveBeenCalled();
    });
  });

  describe("invite-only registration gating", () => {
    // Drives loginWithExistingToken down the inactive/invite-only branch and
    // answers the registration-required probe with `true`, which is what makes
    // openRegistrationModal run.
    const openModalViaInactiveLogin = (): void => {
      AuthService.setAccessToken("tok");
      config.env.inviteOnly = true;
      jwt.decodeToken.mockReturnValue({ ...claims, role: Role.INACTIVE });

      expect(service.loginWithExistingToken()).toBeUndefined();

      const req = httpMock.expectOne(r => r.url === `${api}/user/joining-reason/required`);
      expect(req.request.method).toEqual("GET");
      expect(req.request.params.get("uid")).toEqual("5");
      req.flush(true);
    };

    it("opens the registration modal when registration is required", () => {
      modal.create.mockReturnValue({
        getContentComponent: () => ({
          modalTitle: "Request Access",
          getValues: () => ({ affiliation: "", reason: "" }),
        }),
        updateConfig: vi.fn(),
      });

      openModalViaInactiveLogin();

      expect(modal.create).toHaveBeenCalledTimes(1);
      expect(modal.create.mock.calls[0][0]).toEqual(
        expect.objectContaining({
          nzData: { uid: 5, email: "u@x.com", name: "Ursula" },
          nzOkText: "Send request to Admin",
        })
      );
    });

    it("submitRegistration PUTs the affiliation/reason when the modal is confirmed", async () => {
      const gmail = TestBed.inject(GmailService) as unknown as { notifyUnauthorizedLogin: ReturnType<typeof vi.fn> };
      gmail.notifyUnauthorizedLogin = vi.fn();
      modal.create.mockReturnValue({
        getContentComponent: () => ({
          modalTitle: "Request Access",
          getValues: () => ({ affiliation: "Texera", reason: "research" }),
        }),
        updateConfig: vi.fn(),
      });

      openModalViaInactiveLogin();

      const okHandler = modal.create.mock.calls[0][0].nzOnOk as () => Promise<boolean>;
      const okPromise = okHandler();

      const req = httpMock.expectOne(`${api}/user/joining-reason`);
      expect(req.request.method).toEqual("PUT");
      expect(req.request.body).toEqual({ uid: 5, affiliation: "Texera", joiningReason: "research" });
      req.flush(null);

      await expect(okPromise).resolves.toBe(true);
      expect(gmail.notifyUnauthorizedLogin).toHaveBeenCalledWith("u@x.com", "Texera", "research");
    });
  });
});
