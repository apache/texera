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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { firstValueFrom } from "rxjs";

import { GuiConfigService } from "./gui-config.service";
import { AppSettings } from "../app-setting";

const API = "api";
const TOKEN_KEY = "access_token";

const PRE_LOGIN_PAYLOAD = {
  localLogin: true,
  googleLogin: false,
  defaultLocalUser: { username: "demo", password: "demo-pw" },
  attributionEnabled: true,
};

const GUI_PAYLOAD = {
  copilotEnabled: true,
  limitColumns: 42,
  defaultExecutionMode: "PIPELINED",
};

const USER_SYSTEM_PAYLOAD = {
  inviteOnly: true,
};

describe("GuiConfigService", () => {
  let service: GuiConfigService;
  let http: HttpTestingController;

  beforeEach(() => {
    localStorage.removeItem(TOKEN_KEY);
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [GuiConfigService],
    });
    service = TestBed.inject(GuiConfigService);
    http = TestBed.inject(HttpTestingController);
    vi.spyOn(AppSettings, "getApiEndpoint").mockReturnValue(API);
  });

  afterEach(() => {
    localStorage.removeItem(TOKEN_KEY);
    http.verify();
  });

  // ─── loadPreLogin ─────────────────────────────────────────────────────────

  it("loadPreLogin fetches /config/pre-login and merges the response into env", async () => {
    const pending = firstValueFrom(service.loadPreLogin());

    const req = http.expectOne(`${API}/config/pre-login`);
    expect(req.request.method).toBe("GET");
    req.flush(PRE_LOGIN_PAYLOAD);

    await pending;
    expect(service.env.localLogin).toBe(true);
    expect(service.env.googleLogin).toBe(false);
    expect(service.env.defaultLocalUser).toEqual({ username: "demo", password: "demo-pw" });
    expect(service.env.attributionEnabled).toBe(true);
  });

  it("loadPreLogin propagates errors so APP_INITIALIZER can surface them", async () => {
    // Network failure at bootstrap means the login page can't render correctly.
    // The orchestrator and APP_INITIALIZER both rely on this rejection to log
    // and (in production) show an error banner, so the rejection must escape.
    const pending = firstValueFrom(service.loadPreLogin());
    http.expectOne(`${API}/config/pre-login`).error(new ProgressEvent("net error"));
    await expect(pending).rejects.toThrow(/pre-login configuration/);
  });

  // ─── loadPostLogin ────────────────────────────────────────────────────────

  it("loadPostLogin fetches /config/gui and /config/user-system in parallel and merges", async () => {
    const pending = firstValueFrom(service.loadPostLogin());

    const gui = http.expectOne(`${API}/config/gui`);
    const userSystem = http.expectOne(`${API}/config/user-system`);
    expect(gui.request.method).toBe("GET");
    expect(userSystem.request.method).toBe("GET");

    gui.flush(GUI_PAYLOAD);
    userSystem.flush(USER_SYSTEM_PAYLOAD);

    await pending;
    expect(service.env.copilotEnabled).toBe(true);
    expect(service.env.limitColumns).toBe(42);
    expect(service.env.inviteOnly).toBe(true);
  });

  it("loadPostLogin preserves pre-login fields when both phases run", async () => {
    // Guards a refactor that overwrites the merged map with the gui/user-system
    // response shape. Without this assertion, pre-login fields could silently
    // disappear from env after post-login completes.
    const preLoginPending = firstValueFrom(service.loadPreLogin());
    http.expectOne(`${API}/config/pre-login`).flush(PRE_LOGIN_PAYLOAD);
    await preLoginPending;

    const postLoginPending = firstValueFrom(service.loadPostLogin());
    http.expectOne(`${API}/config/gui`).flush(GUI_PAYLOAD);
    http.expectOne(`${API}/config/user-system`).flush(USER_SYSTEM_PAYLOAD);
    await postLoginPending;

    expect(service.env.localLogin).toBe(true);
    expect(service.env.defaultLocalUser).toEqual({ username: "demo", password: "demo-pw" });
    expect(service.env.copilotEnabled).toBe(true);
    expect(service.env.inviteOnly).toBe(true);
  });

  // ─── load() orchestrator ──────────────────────────────────────────────────

  it("load() only hits /config/pre-login when no access token is in localStorage", async () => {
    const pending = firstValueFrom(service.load());
    http.expectOne(`${API}/config/pre-login`).flush(PRE_LOGIN_PAYLOAD);
    // /config/gui and /config/user-system must not be requested when anonymous;
    // the no-Authorization-header request would 403 and pollute network logs.
    http.expectNone(`${API}/config/gui`);
    http.expectNone(`${API}/config/user-system`);
    await pending;
    expect(service.env.localLogin).toBe(true);
  });

  it("load() chains /config/gui + /config/user-system when a token is stored", async () => {
    localStorage.setItem(TOKEN_KEY, "stored-token");
    const pending = firstValueFrom(service.load());

    http.expectOne(`${API}/config/pre-login`).flush(PRE_LOGIN_PAYLOAD);
    // forkJoin fires both requests in parallel after pre-login resolves.
    http.expectOne(`${API}/config/gui`).flush(GUI_PAYLOAD);
    http.expectOne(`${API}/config/user-system`).flush(USER_SYSTEM_PAYLOAD);

    await pending;
    expect(service.env.localLogin).toBe(true);
    expect(service.env.copilotEnabled).toBe(true);
    expect(service.env.inviteOnly).toBe(true);
  });

  it("load() swallows post-login 403s so bootstrap is not blocked by an expired token", async () => {
    // Stale token in localStorage would otherwise leave the app stuck on a
    // blank screen — this is the exact failure mode that caused PR #5025 to
    // revert the earlier eager-401 lockdown.
    localStorage.setItem(TOKEN_KEY, "expired-token");
    const pending = firstValueFrom(service.load());

    http.expectOne(`${API}/config/pre-login`).flush(PRE_LOGIN_PAYLOAD);
    const guiReq = http.expectOne(`${API}/config/gui`);
    const userSystemReq = http.expectOne(`${API}/config/user-system`);
    guiReq.flush({}, { status: 403, statusText: "Forbidden" });
    // forkJoin tears down the sibling observable on first error, so the
    // user-system request is already cancelled by the time we get here.
    // Flushing a cancelled TestRequest throws.
    if (!userSystemReq.cancelled) {
      userSystemReq.flush({}, { status: 403, statusText: "Forbidden" });
    }

    await pending;
    // Pre-login fields stay, post-login fields stay unset; the app degrades
    // rather than failing to start.
    expect(service.env.localLogin).toBe(true);
    expect(service.env.copilotEnabled).toBeUndefined();
    expect(service.env.inviteOnly).toBeUndefined();
  });

  it("load() rejects when /config/pre-login itself fails so the bootstrap error is surfaced", async () => {
    localStorage.setItem(TOKEN_KEY, "stored-token");
    const pending = firstValueFrom(service.load());
    http.expectOne(`${API}/config/pre-login`).error(new ProgressEvent("offline"));
    // /config/gui must NOT be attempted if pre-login fails — the catchError on
    // the inner pipe must not catch the outer pre-login rejection.
    http.expectNone(`${API}/config/gui`);
    http.expectNone(`${API}/config/user-system`);
    await expect(pending).rejects.toThrow(/pre-login configuration/);
  });
});
