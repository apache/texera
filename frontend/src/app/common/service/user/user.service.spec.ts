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
import { StubAuthService } from "./stub-auth.service";
import { MOCK_USER } from "./stub-user.service";
import { skip } from "rxjs/operators";
import { firstValueFrom, of, Subject, throwError } from "rxjs";
import { commonTestProviders } from "../../testing/test-utils";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { GuiConfigService } from "../gui-config.service";
import { Role, User } from "../../type/user";

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
    service.register("test", "test@example.com", "password").subscribe(() => {
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
    service.register("existing_user", "existing_user@example.com", "password").subscribe(() => {
      expect((service as any).currentUser).toBeFalsy();
    });
  });

  // Where verification is on the first call creates nothing, so nobody may be signed in off the
  // back of it — the account does not exist until the code comes back.
  it("reports a pending verification and signs nobody in", async () => {
    const auth = TestBed.inject(AuthService) as unknown as StubAuthService;
    vi.spyOn(auth, "register").mockReturnValue(of({ accessToken: null }));

    const outcome = await firstValueFrom(service.register("pending", "pending@example.com", "password"));

    expect(outcome).toEqual({ verificationRequired: true });
    expect(service.isLogin()).toBe(false);
    expect(AuthService.getAccessToken()).toBeNull();
  });

  it("signs the user in once registerVerify accepts the code", async () => {
    await firstValueFrom(service.registerVerify("test", "test@example.com", "password", "123456"));

    expect(service.isLogin()).toBe(true);
  });

  // The verify endpoint answers with `accessToken: string | null`, so a backend that accepted the
  // code but declined to mint a session is representable. registerVerify substitutes "" for the
  // absent token. What is pinned here is narrow and deliberate: no *usable* token is left behind,
  // so any substituted placeholder fails.
  //
  // What is deliberately NOT pinned is whether the absent-token path runs `handleAccessToken` at
  // all. It does today, and that is the defect: it writes "" over whatever token was there and
  // fires the post-login @RolesAllowed config fetch for a session that was never granted. With a
  // token already in localStorage that path reaches `AuthService.loginWithExistingToken`, whose
  // `token == null` guard does not catch "", so an unrelated failed verification pops "Access
  // token is expired!" and signs the current user out. `register()` above handles the same
  // `string | null` shape by short-circuiting instead. Asserting either symptom — the "" write, or
  // the config fetch firing — would cement that, so this test stays silent about it and accepts
  // that a short-circuiting rewrite of line 108 is not distinguishable from here.
  it("leaves no usable session when registerVerify answers without a token", async () => {
    const auth = TestBed.inject(AuthService) as unknown as StubAuthService;
    vi.spyOn(auth, "registerVerify").mockReturnValue(of({ accessToken: null }));

    await firstValueFrom(service.registerVerify("pending", "pending@example.com", "password", "123456"));

    // Tolerant of both "" (today) and null (a short-circuiting fix), intolerant of any substituted
    // placeholder. `isLogin()` is deliberately not asserted next to it: the stub's
    // loginWithExistingToken answers with a user only for MOCK_TOKEN, which no mutation of this
    // path can synthesise out of `{ accessToken: null }`, so that assertion could never have
    // failed and read as assurance it did not provide.
    expect(AuthService.getAccessToken() ?? "").toBe("");
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

  // ─── current-user state ───────────────────────────────────────────────────

  const baseUser: User = {
    uid: 1,
    name: "alice",
    email: "alice@x.io",
    role: Role.REGULAR,
    comment: "",
    joiningReason: "",
  };

  it("changeUser sets the current user (assigning a color) and emits it on userChanged", async () => {
    // The constructor already replayed an initial `undefined`; skip it and wait
    // for the emission our changeUser call produces.
    const nextEmission = firstValueFrom(service.userChanged().pipe(skip(1)));

    (service as any).changeUser(baseUser);

    expect(service.getCurrentUser()).toMatchObject({ uid: 1, name: "alice" });
    expect(service.getCurrentUser()?.color).toMatch(/^hsl\(/);
    expect(await nextEmission).toMatchObject({ uid: 1, name: "alice" });
  });

  // The email prompt lives in AuthService (it owns the token it replaces); what UserService owes
  // it is a refreshed currentUser once that token lands. See auth.service.spec.ts for the prompt.
  it("re-derives the current user when AuthService changes the session", async () => {
    const auth = TestBed.inject(AuthService) as unknown as StubAuthService;
    await firstValueFrom(service.login("test", "password"));
    expect(service.isLogin()).toBe(true);

    const nextEmission = firstValueFrom(service.userChanged().pipe(skip(1)));
    auth.emitSessionChanged();

    // Re-derived, not merely re-emitted: the value comes back out of the (stubbed) token rather
    // than from the copy UserService was holding.
    expect(await nextEmission).toMatchObject({ uid: MOCK_USER.uid, name: MOCK_USER.name });
  });

  it("isAdmin reflects only the ADMIN role of the current user", () => {
    expect(service.isAdmin()).toBe(false); // no user

    (service as any).changeUser({ ...baseUser, role: Role.REGULAR });
    expect(service.isAdmin()).toBe(false);

    (service as any).changeUser({ ...baseUser, role: Role.ADMIN });
    expect(service.isAdmin()).toBe(true);
  });

  // ─── avatar fetching ──────────────────────────────────────────────────────

  // The stored value is the provider's complete URL, not a Google-specific fragment, so it is
  // fetched as-is and is also the cache key.
  const AVATAR_URL = "https://lh3.googleusercontent.com/a/AVATAR-ID";

  it("getAvatar returns undefined for an empty avatar url", async () => {
    expect(await firstValueFrom(service.getAvatar(""))).toBeUndefined();
  });

  it("getAvatar returns the cached object URL while the entry is still fresh", async () => {
    (service as any).cache.set(AVATAR_URL, { url: "blob:cached", expiry: Date.now() + 60_000 });
    expect(await firstValueFrom(service.getAvatar(AVATAR_URL))).toBe("blob:cached");
  });

  describe("getAvatar network path", () => {
    // fetchBlob() goes through the native `fetch`/`URL` globals (not HttpClient),
    // so stub them deterministically and restore the originals afterwards.
    let originalFetch: typeof globalThis.fetch;
    let originalCreateObjectURL: typeof URL.createObjectURL;
    let originalRevokeObjectURL: typeof URL.revokeObjectURL;

    beforeEach(() => {
      originalFetch = globalThis.fetch;
      originalCreateObjectURL = URL.createObjectURL;
      originalRevokeObjectURL = URL.revokeObjectURL;
    });

    afterEach(() => {
      globalThis.fetch = originalFetch;
      URL.createObjectURL = originalCreateObjectURL;
      URL.revokeObjectURL = originalRevokeObjectURL;
    });

    it("fetches the avatar, wraps the blob in an object URL, and caches it", async () => {
      const blob = new Blob(["img"]);
      globalThis.fetch = vi.fn().mockResolvedValue({ ok: true, blob: () => Promise.resolve(blob) }) as any;
      URL.createObjectURL = vi.fn().mockReturnValue("blob:fetched");

      // Held rather than subscribed inline, and subscribed twice: the returned observable replays
      // its one value to every later subscriber. Under a refCounted share the first subscriber's
      // unsubscribe would tear the buffer down, so the second would refetch and mint a second
      // object URL over the cache entry — orphaning the first blob with nothing left holding the
      // reference needed to revoke it. Same leak class as the expired-entry revoke below.
      const avatar$ = service.getAvatar(AVATAR_URL);

      expect(await firstValueFrom(avatar$)).toBe("blob:fetched");
      expect(await firstValueFrom(avatar$)).toBe("blob:fetched");

      // fetched verbatim — no CDN prefix is reconstructed here any more
      expect(globalThis.fetch).toHaveBeenCalledWith(AVATAR_URL, {
        referrerPolicy: "no-referrer",
      });
      expect(URL.createObjectURL).toHaveBeenCalledWith(blob);
      expect(globalThis.fetch).toHaveBeenCalledTimes(1);
      expect(URL.createObjectURL).toHaveBeenCalledTimes(1);
    });

    it("fetches an avatar hosted anywhere the backend allowed, not just Google's CDN", async () => {
      const blob = new Blob(["img"]);
      globalThis.fetch = vi.fn().mockResolvedValue({ ok: true, blob: () => Promise.resolve(blob) }) as any;
      URL.createObjectURL = vi.fn().mockReturnValue("blob:other");

      const otherHost = "https://avatars.example-provider.com/u/12345";
      expect(await firstValueFrom(service.getAvatar(otherHost))).toBe("blob:other");
      expect(globalThis.fetch).toHaveBeenCalledWith(otherHost, { referrerPolicy: "no-referrer" });
    });

    // The freshness check has an expiry side to it: an object URL that outlives its entry has to be
    // handed back to the browser, or every avatar refresh leaks the blob it was holding. The
    // "returns the cached object URL while the entry is still fresh" spec above only ever exercised
    // the other side of the same comparison.
    it("revokes the stale object URL and refetches once the cached entry has expired", async () => {
      const blob = new Blob(["fresh-img"]);
      globalThis.fetch = vi.fn().mockResolvedValue({ ok: true, blob: () => Promise.resolve(blob) }) as any;
      URL.createObjectURL = vi.fn().mockReturnValue("blob:fresh");
      URL.revokeObjectURL = vi.fn();
      (service as any).cache.set(AVATAR_URL, { url: "blob:stale", expiry: Date.now() - 1 });

      const result = await firstValueFrom(service.getAvatar(AVATAR_URL));

      // Released by its own url, not by the cache key it was filed under.
      expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:stale");
      expect(globalThis.fetch).toHaveBeenCalledWith(AVATAR_URL, { referrerPolicy: "no-referrer" });
      expect(result).toBe("blob:fresh");
      // The whole entry, not just its url. Both freshness tests hand the comparison an expiry the
      // test itself supplied, so without this nothing anywhere reads back the expiry getAvatar
      // *writes*: `Date.now() + cacheDuration` could read `- cacheDuration` and every entry would
      // be born stale, turning every avatar render into a revoke-and-refetch.
      const entry = (service as any).cache.get(AVATAR_URL);
      expect(entry.url).toBe("blob:fresh");
      expect(entry.expiry).toBeGreaterThan(Date.now());
    });

    it("drops the revoked entry from the cache even when the refetch then fails", async () => {
      URL.revokeObjectURL = vi.fn();
      globalThis.fetch = vi.fn().mockResolvedValue({ ok: false, status: 500 }) as any;
      (service as any).cache.set(AVATAR_URL, { url: "blob:stale", expiry: Date.now() - 1 });

      expect(await firstValueFrom(service.getAvatar(AVATAR_URL))).toBeUndefined();

      // The url has been handed back to the browser, so the entry holding it is dead. Keeping it
      // would leave a revoked object URL reachable and revoke it a second time on the next call.
      expect((service as any).cache.has(AVATAR_URL)).toBe(false);
      expect(URL.revokeObjectURL).toHaveBeenCalledTimes(1);
    });

    it("returns undefined when the avatar fetch fails", async () => {
      globalThis.fetch = vi.fn().mockResolvedValue({ ok: false, status: 500 }) as any;
      expect(await firstValueFrom(service.getAvatar("https://lh3.googleusercontent.com/a/BAD"))).toBeUndefined();
    });
  });
});
