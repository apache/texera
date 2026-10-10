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
import { ActivatedRoute, convertToParamMap, Router } from "@angular/router";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { HttpErrorResponse } from "@angular/common/http";
import { of, ReplaySubject, throwError } from "rxjs";
import { vi } from "vitest";

import { OrcidCallbackComponent } from "./orcid-callback.component";
import { UserService } from "../../../common/service/user/user.service";
import { User } from "../../../common/type/user";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ORCID_STATE_KEY } from "../../../common/service/user/orcid-auth.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { LOGIN, USER_WORKFLOW } from "../../../app-routing.constant";

/**
 * The callback page has no interaction: everything it does happens in ngOnInit, and the only
 * observable outcomes are which URL it navigates to and whether the code reached the exchange.
 * The `state` cases are the point of most of this — that value is the flow's CSRF protection, so a
 * missing or mismatched one must never reach `orcidLogin`.
 */
describe("OrcidCallbackComponent", () => {
  let fixture: ComponentFixture<OrcidCallbackComponent>;
  let userServiceMock: {
    orcidLogin: ReturnType<typeof vi.fn>;
    isLogin: ReturnType<typeof vi.fn>;
    userChanged: () => ReplaySubject<User | undefined>;
  };
  /** Stands in for `UserService`'s own subject, replaying the current user the way it does. */
  let userChangedSubject: ReplaySubject<User | undefined>;
  let notificationServiceMock: { error: ReturnType<typeof vi.fn> };
  let routerMock: { navigateByUrl: ReturnType<typeof vi.fn> };

  const STATE = "state-abc";

  /** Builds the component with `queryParams` in the URL and `storedState` in sessionStorage. */
  const createComponent = async (
    queryParams: Record<string, string>,
    storedState: string | null = STATE,
    orcidLogin = vi.fn().mockReturnValue(of(undefined)),
    signedIn = true
  ) => {
    TestBed.resetTestingModule();
    sessionStorage.clear();
    if (storedState !== null) {
      sessionStorage.setItem(ORCID_STATE_KEY, storedState);
    }

    userChangedSubject = new ReplaySubject<User | undefined>(1);
    // `handleAccessToken` publishes the session state before `orcidLogin` completes, so there is
    // always a current value to replay: the user for an address-carrying token, nothing while the
    // address prompt is open.
    userChangedSubject.next(signedIn ? ({ uid: 1 } as User) : undefined);
    userServiceMock = {
      orcidLogin,
      isLogin: vi.fn().mockReturnValue(signedIn),
      userChanged: () => userChangedSubject,
    };
    notificationServiceMock = { error: vi.fn() };
    routerMock = { navigateByUrl: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [OrcidCallbackComponent, HttpClientTestingModule],
      providers: [
        { provide: UserService, useValue: userServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: Router, useValue: routerMock },
        {
          provide: ActivatedRoute,
          useValue: { snapshot: { queryParamMap: convertToParamMap(queryParams) } },
        },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(OrcidCallbackComponent);
    fixture.detectChanges();
  };

  afterEach(() => sessionStorage.clear());

  // ─── the happy path ───────────────────────────────────────────────────────

  it("exchanges the code and lands the user in the dashboard", async () => {
    await createComponent({ code: "auth-code", state: STATE });

    expect(userServiceMock.orcidLogin).toHaveBeenCalledWith("auth-code");
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    expect(notificationServiceMock.error).not.toHaveBeenCalled();
  });

  // Good for exactly one round trip: a leftover key would let a later callback verify against a
  // state nobody is waiting on.
  it("clears the stored state once it has been read", async () => {
    await createComponent({ code: "auth-code", state: STATE });

    expect(sessionStorage.getItem(ORCID_STATE_KEY)).toBeNull();
  });

  // ─── state verification ───────────────────────────────────────────────────

  it("refuses a state that does not match the one it stored", async () => {
    await createComponent({ code: "auth-code", state: "not-the-one" });

    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).toHaveBeenCalled();
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  it("refuses a callback carrying no state at all", async () => {
    await createComponent({ code: "auth-code" });

    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  // Nothing stored means this browser did not start a sign-in: a bookmark, a stale tab, or a code
  // planted by someone else.
  it("refuses when it never stored a state to compare against", async () => {
    await createComponent({ code: "auth-code", state: STATE }, null);

    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  // ─── what ORCID sends back instead of a code ──────────────────────────────

  it("reports the description when ORCID returns a correlated error", async () => {
    await createComponent({ error: "access_denied", error_description: "The user denied access", state: STATE });

    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).toHaveBeenCalledWith("The user denied access");
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  it("falls back to a generic message when ORCID's error carries no description", async () => {
    await createComponent({ error: "access_denied", state: STATE });

    expect(notificationServiceMock.error).toHaveBeenCalledWith("ORCID sign-in was not completed");
  });

  // An error response carries `state` too (RFC 6749 §4.1.2.1), so one that does not correlate is
  // not ORCID answering this browser — it is a planted link, and its `error_description` must not
  // be repeated back as Texera's own message.
  it("refuses an uncorrelated error instead of reporting its description", async () => {
    await createComponent({
      error: "access_denied",
      error_description: "Visit https://evil.example to re-authorize",
      state: "not-the-one",
    });

    expect(notificationServiceMock.error).toHaveBeenCalledWith(
      "ORCID sign-in could not be verified. Please try again."
    );
    expect(notificationServiceMock.error).not.toHaveBeenCalledWith("Visit https://evil.example to re-authorize");
    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  it("refuses an error response carrying no state at all", async () => {
    await createComponent({ error: "access_denied", error_description: "planted" });

    expect(notificationServiceMock.error).toHaveBeenCalledWith(
      "ORCID sign-in could not be verified. Please try again."
    );
    expect(notificationServiceMock.error).not.toHaveBeenCalledWith("planted");
  });

  it("refuses a verified callback that carries no code", async () => {
    await createComponent({ state: STATE });

    expect(userServiceMock.orcidLogin).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).toHaveBeenCalledWith("ORCID sign-in was not completed");
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
  });

  // ─── a failed exchange ────────────────────────────────────────────────────

  // The backend's message is in `error.message`; HttpErrorResponse.message is Angular's generated
  // "Http failure response for …" developer string, which must not reach the visitor.
  it("sends the user back to the login page with the backend's message when the exchange fails", async () => {
    const failing = vi.fn().mockReturnValue(
      throwError(
        () =>
          new HttpErrorResponse({
            status: 401,
            statusText: "Unauthorized",
            error: { message: "Login credentials are incorrect." },
          })
      )
    );
    await createComponent({ code: "auth-code", state: STATE }, STATE, failing);

    expect(notificationServiceMock.error).toHaveBeenCalledWith("Login credentials are incorrect.");
    expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
    expect(routerMock.navigateByUrl).not.toHaveBeenCalledWith(USER_WORKFLOW);
  });

  // An ORCID token carries no email, so the session is not resolved when `orcidLogin` completes:
  // `loginWithExistingToken` has opened the address prompt and handed back no user. Navigating then
  // would hit AuthGuardService and land the user on /login behind the still-open dialog.
  describe("when the address prompt is still open", () => {
    const openPrompt = (queryParams = { code: "auth-code", state: STATE }) =>
      createComponent(queryParams, STATE, vi.fn().mockReturnValue(of(undefined)), false);

    it("stays put rather than navigating into the auth guard", async () => {
      await openPrompt();

      expect(userServiceMock.orcidLogin).toHaveBeenCalledWith("auth-code");
      expect(routerMock.navigateByUrl).not.toHaveBeenCalled();
    });

    it("goes to the dashboard once the answered prompt produces a user", async () => {
      await openPrompt();

      userChangedSubject.next({ uid: 7 } as User);

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });

    // Cancelling the dialog signs out, which republishes an undefined user.
    it("returns to the login page if the prompt is cancelled", async () => {
      await openPrompt();

      userChangedSubject.next(undefined);

      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(LOGIN, { replaceUrl: true });
      expect(routerMock.navigateByUrl).not.toHaveBeenCalledWith(USER_WORKFLOW);
    });

    it("routes on the outcome only once", async () => {
      await openPrompt();

      userChangedSubject.next({ uid: 7 } as User);
      userChangedSubject.next(undefined);

      expect(routerMock.navigateByUrl).toHaveBeenCalledTimes(1);
      expect(routerMock.navigateByUrl).toHaveBeenCalledWith(USER_WORKFLOW);
    });
  });

  it("falls back to a generic message when the failure carries none", async () => {
    const failing = vi
      .fn()
      .mockReturnValue(throwError(() => new HttpErrorResponse({ status: 500, statusText: "Server Error" })));
    await createComponent({ code: "auth-code", state: STATE }, STATE, failing);

    expect(notificationServiceMock.error).toHaveBeenCalledWith("ORCID sign-in failed");
  });
});
