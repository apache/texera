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

import "zone.js/testing";

import { fakeAsync, TestBed, tick } from "@angular/core/testing";
import { UserService } from "./user.service";
import { AuthService } from "./auth.service";
import { MOCK_TOKEN, StubAuthService } from "./stub-auth.service";
import { skip } from "rxjs/operators";
import { firstValueFrom, Subject, throwError } from "rxjs";
import { commonTestProviders } from "../../testing/test-utils";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { GuiConfigService } from "../gui-config.service";

describe("UserService", () => {
  let service: UserService;
  let config: GuiConfigService;

  beforeEach(() => {
    AuthService.removeAccessToken();
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [UserService, { provide: AuthService, useClass: StubAuthService }, ...commonTestProviders],
    });

    service = TestBed.inject(UserService);
    config = TestBed.inject(GuiConfigService);
  });

  afterAll(() => {
    TestBed.resetTestingModule();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should login after register user", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeTruthy());
    service.register("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();
    });
  });

  it("should login after login user", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeTruthy());
    service.login("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();
    });
  });

  it("should not login after register failed", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeFalsy());
    service.register("existing_user", "password").subscribe(() => {
      expect((service as any).currentUser).toBeFalsy();
    });
  });

  it("should not login after login failed", () => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(user => expect(user).toBeFalsy());
    service.login("test", "wrong_password").subscribe(() => {
      expect((service as any).currentUser).toBeFalsy();
    });
  });

  it("should log out when called log out function", fakeAsync(() => {
    expect((service as any).currentUser).toBeFalsy();
    service
      .userChanged()
      .pipe(skip(2))
      .subscribe(user => expect(user).toBeFalsy());
    service.login("test", "password").subscribe(() => {
      expect((service as any).currentUser).toBeTruthy();

      tick(10);
      service.logout();

      tick(10);
      expect((service as any).currentUser).toBeFalsy();
    });
  }));

  // ─── post-login config fetch coordination ─────────────────────────────────

  it("loads the authenticated config when a fresh login succeeds", async () => {
    // /config/gui and /config/user-system are @RolesAllowed; their values must
    // be in memory before any post-login component reads config.env, otherwise
    // the dashboard renders against undefined flags.
    const spy = vi.spyOn(config, "loadPostLogin");
    await firstValueFrom(service.login("test", "password"));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(service.isLogin()).toBe(true);
  });

  it("loads the authenticated config when a googleLogin succeeds", async () => {
    // googleLogin shares the same handleAccessToken plumbing as username/password
    // login, so the post-login config fetch must fire here too — otherwise a
    // user who only ever signs in through Google would see undefined flags.
    const spy = vi.spyOn(config, "loadPostLogin");
    await firstValueFrom(service.googleLogin("any-credential"));
    expect(spy).toHaveBeenCalledTimes(1);
    expect(service.isLogin()).toBe(true);
  });

  it("orders the post-login config fetch before the userChanged event fires", async () => {
    // Subscribers to userChanged (header, sidebar, routing guards) drive the
    // initial dashboard render. If userChanged fires before loadPostLogin
    // resolves, those subscribers see env without the authenticated fields and
    // mis-render (e.g. copilot button missing, inviteOnly check skipped).
    const gate = new Subject<unknown>();
    vi.spyOn(config, "loadPostLogin").mockReturnValue(gate.asObservable() as any);

    const userEmissions: Array<unknown> = [];
    service
      .userChanged()
      .pipe(skip(1))
      .subscribe(u => userEmissions.push(u));

    const loginPromise = firstValueFrom(service.login("test", "password"));
    // Login is in-flight; loadPostLogin has not resolved yet, so userChanged
    // must NOT have emitted a logged-in user yet.
    expect(userEmissions).toEqual([]);

    gate.next({});
    gate.complete();
    await loginPromise;

    expect(userEmissions.length).toBe(1);
    expect(userEmissions[0]).toBeTruthy();
  });

  it("still completes login when loadPostLogin fails", async () => {
    // Backend hiccup on /config/gui must not strand the user on a blank screen.
    // The JwtAuthFilter on every protected endpoint is the authoritative gate;
    // degraded config is preferable to a stuck spinner.
    vi.spyOn(config, "loadPostLogin").mockReturnValue(throwError(() => new Error("simulated 500")));
    await firstValueFrom(service.login("test", "password"));
    expect(service.isLogin()).toBe(true);
  });

  // ─── admin impersonation token stash / restore ────────────────────────────

  describe("impersonation", () => {
    // The stashed admin token must be a value the StubAuthService recognizes as a
    // valid session (MOCK_TOKEN), so that restoring it logs the admin back in.
    const ADMIN_TOKEN = MOCK_TOKEN.accessToken;
    // Any other value stands in for the target user's freshly minted token.
    const TARGET_TOKEN = "target-user-token";

    beforeEach(() => {
      AuthService.removeAccessToken();
      AuthService.removeImpersonatorToken();
    });

    afterEach(() => {
      AuthService.removeAccessToken();
      AuthService.removeImpersonatorToken();
    });

    it("stashes the admin token and swaps to the target token on start", async () => {
      AuthService.setAccessToken(ADMIN_TOKEN);

      await firstValueFrom(service.startImpersonation(TARGET_TOKEN));

      // Active session is now the target user's token; the admin token is stashed.
      expect(AuthService.getAccessToken()).toBe(TARGET_TOKEN);
      expect(AuthService.getImpersonatorToken()).toBe(ADMIN_TOKEN);
      expect(service.isImpersonating()).toBe(true);
    });

    it("restores the stashed admin token and clears the stash on stop", async () => {
      // Simulate an in-progress impersonation: target session active, admin stashed.
      AuthService.setAccessToken(TARGET_TOKEN);
      AuthService.setImpersonatorToken(ADMIN_TOKEN);

      await firstValueFrom(service.stopImpersonation());

      // The admin token is restored as the active session and the stash is cleared.
      expect(AuthService.getAccessToken()).toBe(ADMIN_TOKEN);
      expect(AuthService.getImpersonatorToken()).toBeNull();
      expect(service.isImpersonating()).toBe(false);
      expect(service.isLogin()).toBe(true);
    });

    it("is a no-op on stop when not impersonating", async () => {
      AuthService.setAccessToken(TARGET_TOKEN);

      await firstValueFrom(service.stopImpersonation());

      // Nothing stashed, so the active token is left untouched.
      expect(AuthService.getAccessToken()).toBe(TARGET_TOKEN);
      expect(service.isImpersonating()).toBe(false);
    });

    it("reflects impersonation state via isImpersonating()", () => {
      expect(service.isImpersonating()).toBe(false);
      AuthService.setImpersonatorToken(ADMIN_TOKEN);
      expect(service.isImpersonating()).toBe(true);
      AuthService.removeImpersonatorToken();
      expect(service.isImpersonating()).toBe(false);
    });
  });
});
