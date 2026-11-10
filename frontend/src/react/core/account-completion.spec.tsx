/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { LoginPage } from "../login-page";
import { ApiClient } from "./api-client";
import { AuthStore } from "./auth";
import { RuntimeConfig } from "./runtime-config";

const stores: AuthStore[] = [];
afterEach(() => {
  cleanup();
  for (const store of stores) store.dispose();
  stores.length = 0;
});

function jwt(claims: Record<string, unknown> = {}) {
  return [
    "header",
    btoa(
      JSON.stringify({
        userId: 9,
        sub: "tester",
        email: "test@example.org",
        role: "REGULAR",
        exp: 9999999999,
        ...claims,
      })
    ),
    "signature",
  ].join(".");
}
async function mount(
  options: { token?: string; verification?: boolean; approvalRequired?: boolean; rejectEmail?: boolean } = {}
) {
  const values = new Map<string, string>();
  if (options.token) values.set("access_token", options.token);
  const storage = {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
    removeItem: (key: string) => {
      values.delete(key);
    },
  };
  const fetch = vi.fn(async (path: string, _init?: RequestInit) => {
    if (path.endsWith("/config/pre-login"))
      return Response.json({ localLogin: true, emailVerification: options.verification });
    if (path.endsWith("/config/user-system")) return Response.json({ inviteOnly: true });
    if (path.endsWith("/auth/register")) return Response.json({ accessToken: null });
    if (path.endsWith("/auth/register/verify")) return Response.json({ accessToken: jwt() });
    if (path.endsWith("/auth/email"))
      return options.rejectEmail
        ? Response.json({ message: "<a>Incorrect verification code</a>" }, { status: 400 })
        : Response.json({ accessToken: jwt() });
    if (path.includes("/joining-reason/required")) return Response.json(options.approvalRequired ?? false);
    return Response.json({});
  });
  const api = new ApiClient({ fetch, getToken: () => storage.getItem("access_token") });
  const auth = new AuthStore(api, new RuntimeConfig(api), { storage });
  stores.push(auth);
  await auth.bootstrap();
  const navigate = vi.fn();
  render(
    <LoginPage
      auth={auth}
      onNavigate={navigate}
      returnUrl="/user/workflow/42"
    />
  );
  return { auth, fetch, navigate, storage };
}
function fill(label: string, value: string) {
  fireEvent.change(screen.getByLabelText(label, { exact: true }), { target: { value } });
}

describe("account completion through the React login page", () => {
  it("sends registration mail only on explicit request and submits the mailed code separately", async () => {
    const { fetch, navigate } = await mount({ verification: true });
    fireEvent.click(screen.getByRole("button", { name: "Create account" }));
    fill("Username", "tester");
    fill("Email", "test@example.org");
    fill("Password", "secret");
    fill("Confirm password", "secret");
    fireEvent.click(screen.getByRole("button", { name: "Register", exact: true }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Enter the code"));
    expect(fetch.mock.calls.some(([path]) => path.endsWith("/auth/register"))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Send verification code" }));
    await waitFor(() => expect(screen.getByRole("status").textContent).toContain("verification code has been sent"));
    expect(navigate).not.toHaveBeenCalled();
    fill("Verification code", "123456");
    fireEvent.click(screen.getByRole("button", { name: "Register", exact: true }));
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("/user/workflow/42"));
    const verification = fetch.mock.calls.find(([path]) => path.endsWith("/auth/register/verify"));
    expect(JSON.parse(String(verification?.[1]?.body))).toEqual({
      username: "tester",
      email: "test@example.org",
      password: "secret",
      code: "123456",
    });
  });

  it("collects and verifies a missing email before reaching a protected destination", async () => {
    const { fetch, navigate } = await mount({ token: jwt({ email: null }), verification: true });
    expect(screen.queryByLabelText("Username")).toBeNull();
    expect(screen.getByRole("heading", { name: "Add your email address" })).toBeTruthy();
    fill("Email", "new@example.org");
    fireEvent.click(screen.getByRole("button", { name: "Send verification code" }));
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path.endsWith("/auth/email/code"))).toBe(true));
    expect(navigate).not.toHaveBeenCalled();
    fill("Verification code", "123456");
    fireEvent.click(screen.getByRole("button", { name: "Save email" }));
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("/user/workflow/42"));
    const update = fetch.mock.calls.find(([path]) => path.endsWith("/auth/email"));
    expect(JSON.parse(String(update?.[1]?.body))).toEqual({ email: "new@example.org", code: "123456" });
  });

  it("keeps a rejected email correction open and renders the server reason as text", async () => {
    const { auth, navigate } = await mount({ token: jwt({ email: null }), verification: true, rejectEmail: true });
    fill("Email", "new@example.org");
    fill("Verification code", "wrong");
    fireEvent.click(screen.getByRole("button", { name: "Save email" }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toBe("<a>Incorrect verification code</a>"));
    expect(screen.getByRole("alert").querySelector("a")).toBeNull();
    expect(auth.getSnapshot().status).toBe("email-required");
    expect(navigate).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    expect(auth.getSnapshot().status).toBe("anonymous");
  });

  it("submits a new activation request but does not offer a duplicate request for an already pending account", async () => {
    const first = await mount({ token: jwt({ role: "INACTIVE" }), approvalRequired: true });
    fill("Affiliation", "UCI");
    fill("Reason for joining", "Research");
    fireEvent.click(screen.getByRole("button", { name: "Request activation" }));
    await waitFor(() => expect(first.auth.getSnapshot().status).toBe("anonymous"));
    expect(first.navigate).not.toHaveBeenCalled();
    expect(first.fetch.mock.calls.some(([path]) => path.endsWith("/gmail/notify-unauthorized"))).toBe(true);
    cleanup();
    const second = await mount({ token: jwt({ role: "INACTIVE" }), approvalRequired: false });
    expect(screen.getByText("Your account is awaiting approval from the Texera administrator.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Request activation" })).toBeNull();
    expect(second.navigate).not.toHaveBeenCalled();
  });
});
