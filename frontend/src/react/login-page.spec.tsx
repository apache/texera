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
import { ApiClient } from "./core/api-client";
import { AuthStore } from "./core/auth";
import { RuntimeConfig } from "./core/runtime-config";
import { LoginPage } from "./login-page";

afterEach(cleanup);

async function setup(options: { localLogin?: boolean; emailVerification?: boolean; fail?: boolean } = {}) {
  const values = new Map<string, string>();
  const storage = {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
    removeItem: (key: string) => {
      values.delete(key);
    },
  };
  const jwt = `header.${btoa(JSON.stringify({ userId: 7, sub: "tester", email: "test@example.org", role: "REGULAR", exp: 9999999999 }))}.signature`;
  const fetch = vi.fn(async (path: string) => {
    if (path.endsWith("/config/pre-login")) return Response.json({ localLogin: true, ...options });
    if (path.endsWith("/auth/login"))
      return options.fail
        ? Response.json({ message: "Incorrect credentials" }, { status: 401 })
        : Response.json({ accessToken: jwt });
    if (path.endsWith("/auth/register")) return Response.json({ accessToken: null });
    return Response.json({});
  });
  const api = new ApiClient({ fetch, getToken: () => storage.getItem("access_token") });
  const auth = new AuthStore(api, new RuntimeConfig(api), { storage });
  await auth.bootstrap();
  const onNavigate = vi.fn();
  render(
    <LoginPage
      auth={auth}
      onNavigate={onNavigate}
      returnUrl="/user/workflow/42"
    />
  );
  return { auth, fetch, onNavigate };
}

describe("React sign-in page", () => {
  it("retains the Apache project attribution and safe external link", async () => {
    const { auth } = await setup();
    expect(screen.getByText(/Apache Texera/)).toBeTruthy();
    const link = screen.getByRole("link", { name: "texera.apache.org" });
    expect(link.getAttribute("href")).toBe("https://texera.apache.org/");
    expect(link.getAttribute("rel")).toContain("noopener");
    auth.dispose();
  });
  it("signs in with the existing backend and preserves the requested deep link", async () => {
    const { auth, fetch, onNavigate } = await setup();
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "tester" } });
    fireEvent.change(screen.getByLabelText("Password", { exact: true }), { target: { value: "secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in", exact: true }));
    await waitFor(() => expect(onNavigate).toHaveBeenCalledWith("/user/workflow/42"));
    expect(fetch.mock.calls.some(([path]) => path.endsWith("/auth/login"))).toBe(true);
    auth.dispose();
  });

  it("keeps a failed sign-in on the form and displays the server reason", async () => {
    const { auth, onNavigate } = await setup({ fail: true });
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "tester" } });
    fireEvent.change(screen.getByLabelText("Password", { exact: true }), { target: { value: "secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in", exact: true }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("Incorrect credentials"));
    expect(onNavigate).not.toHaveBeenCalled();
    auth.dispose();
  });

  it("rejects mismatched registration passwords before sending a request", async () => {
    const { auth, fetch } = await setup();
    fireEvent.click(screen.getByRole("button", { name: "Create account", exact: true }));
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "tester" } });
    fireEvent.change(screen.getByLabelText("Email"), { target: { value: "test@example.org" } });
    fireEvent.change(screen.getByLabelText("Password", { exact: true }), { target: { value: "secret" } });
    fireEvent.change(screen.getByLabelText("Confirm password"), { target: { value: "different" } });
    fireEvent.click(screen.getByRole("button", { name: "Register", exact: true }));
    await waitFor(() => expect(screen.getByRole("alert").textContent).toContain("passwords"));
    expect(fetch.mock.calls.some(([path]) => path.endsWith("/auth/register"))).toBe(false);
    auth.dispose();
  });

  it("hides local authentication when disabled by deployment configuration", async () => {
    const { auth } = await setup({ localLogin: false });
    expect(screen.queryByLabelText("Password", { exact: true })).toBeNull();
    expect(screen.getByText("Local sign-in is disabled.")).toBeTruthy();
    auth.dispose();
  });
});
