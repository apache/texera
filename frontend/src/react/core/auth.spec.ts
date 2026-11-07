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

import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "./api-client";
import { AuthStore, decodeSessionToken, safeReturnUrl } from "./auth";
import { RuntimeConfig } from "./runtime-config";

function token(claims: Record<string, unknown> = {}): string {
  return ["header", btoa(unescape(encodeURIComponent(JSON.stringify({
    userId: 7, sub: "日本語", email: "user@example.com", role: "REGULAR", exp: 2000, ...claims,
  })))), "signature"].join(".");
}
function memoryStorage() {
  const data = new Map<string, string>();
  return { getItem: (key: string) => data.get(key) ?? null, setItem: (key: string, value: string) => { data.set(key, value); }, removeItem: (key: string) => { data.delete(key); } };
}
function setup(respond: (path: string, init?: RequestInit) => Response = () => Response.json({})) {
  const storage = memoryStorage();
  const fetch = vi.fn(async (path: string, init?: RequestInit) => respond(path, init));
  const api = new ApiClient({ fetch, getToken: () => storage.getItem("access_token") });
  const config = new RuntimeConfig(api);
  const auth = new AuthStore(api, config, { storage, now: () => 1000000 });
  return { auth, api, storage, config, fetch };
}

describe("session token and return URL validation", () => {
  it("decodes unicode identities while refusing expired, malformed or incomplete claims", () => {
    expect(decodeSessionToken(token(), 1000000)).toMatchObject({ uid: 7, name: "日本語", role: "REGULAR" });
    for (const invalid of ["", "broken", token({ exp: 1000 }), token({ userId: null }), token({ role: "OWNER" })]) {
      expect(decodeSessionToken(invalid, 1000000)).toBeUndefined();
    }
  });
  it("accepts internal return URLs only", () => {
    expect(safeReturnUrl("/user/workflow/4?view=1")).toBe("/user/workflow/4?view=1");
    for (const invalid of ["https://evil.example", "//evil.example", "/\\evil", "", "/login"]) {
      expect(safeReturnUrl(invalid)).toBe("/user/workflow");
    }
  });
});

describe("AuthStore", () => {
  it("restores a valid stored token and clears invalid tokens without protected config requests", async () => {
    const { auth, storage, fetch } = setup();
    storage.setItem("access_token", token());
    await auth.bootstrap();
    expect(auth.getSnapshot()).toMatchObject({ status: "authenticated", user: { uid: 7 } });
    auth.logout();
    storage.setItem("access_token", "malformed");
    fetch.mockClear();
    await auth.bootstrap();
    expect(auth.getSnapshot().status).toBe("anonymous");
    expect(storage.getItem("access_token")).toBeNull();
    expect(fetch.mock.calls.map(([path]) => path)).toEqual(["/api/config/pre-login"]);
    auth.dispose();
  });

  it("signs in only after post-login config and emits a stable external-store snapshot", async () => {
    const { auth, fetch, storage } = setup(path => Response.json(path.endsWith("auth/login") ? { accessToken: token() } : {}));
    const changed = vi.fn();
    auth.subscribe(changed);
    await auth.login(" user ", "secret");
    expect(storage.getItem("access_token")).toBe(token());
    expect(fetch.mock.calls[0][0]).toBe("/api/auth/login");
    expect(JSON.parse(String(fetch.mock.calls[0][1]?.body))).toEqual({ username: "user", password: "secret" });
    expect(auth.getSnapshot()).toBe(auth.getSnapshot());
    expect(auth.getSnapshot().status).toBe("authenticated");
    expect(changed).toHaveBeenCalled();
    auth.dispose();
  });

  it("keeps code-mail registration pending and validates bad credentials before requests", async () => {
    const { auth, fetch, storage } = setup(() => Response.json({ accessToken: null }));
    await expect(auth.register("name", "user@example.com", "secret")).resolves.toEqual({ verificationRequired: true });
    expect(storage.getItem("access_token")).toBeNull();
    expect(auth.getSnapshot().status).toBe("anonymous");
    await expect(auth.login(" ", "x")).rejects.toThrow("Username");
    await expect(auth.register("name", "invalid", "secret")).rejects.toThrow("Email");
    expect(fetch).toHaveBeenCalledTimes(1);
    auth.dispose();
  });

  it("gates missing email and completes the reissued session through the server contract", async () => {
    const { auth, fetch } = setup(path => Response.json(
      path.endsWith("auth/orcid/login") ? { accessToken: token({ email: null }) }
      : path.endsWith("auth/email") ? { accessToken: token() } : {}
    ));
    await auth.socialLogin("orcid", "code");
    expect(auth.getSnapshot().status).toBe("email-required");
    expect(fetch.mock.calls[0][1]?.body).toBe("code");
    expect(new Headers(fetch.mock.calls[0][1]?.headers).get("Content-Type")).toBe("text/plain");
    await auth.completeEmail("user@example.com", "123456");
    expect(auth.getSnapshot().status).toBe("authenticated");
    auth.dispose();
  });

  it("keeps inactive invite-only accounts outside protected pages and submits the approval request", async () => {
    const { auth, fetch } = setup(path => Response.json(
      path.endsWith("auth/login") ? { accessToken: token({ role: "INACTIVE" }) }
      : path.endsWith("config/user-system") ? { inviteOnly: true }
      : path.includes("joining-reason/required") ? true : {}
    ));
    await auth.login("name", "secret");
    expect(auth.getSnapshot()).toMatchObject({ status: "approval-required", registrationRequired: true });
    await expect(auth.submitJoiningReason("UCI", "")).rejects.toThrow("Reason");
    await auth.submitJoiningReason("UCI", "Research");
    const request = fetch.mock.calls.find(([path]) => path.endsWith("/user/joining-reason"));
    expect(JSON.parse(String(request?.[1]?.body))).toEqual({ uid: 7, affiliation: "UCI", joiningReason: "Research" });
    expect(auth.getSnapshot().status).toBe("anonymous");
    auth.dispose();
  });
});

