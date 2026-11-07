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

import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "./api-client";
import { SocialAuth } from "./social-auth";

function storage(initial?: string) {
  const values = new Map(initial ? [["orcid_state", initial]] : []);
  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
    removeItem: (key: string) => {
      values.delete(key);
    },
  };
}
function setup() {
  const socialLogin = vi.fn().mockResolvedValue(undefined);
  const fetch = vi.fn(async (url: string) =>
    url.endsWith("/config")
      ? Response.json({
          clientId: "client",
          authorizeUrl: "https://orcid.org/oauth/authorize",
          redirectUri: "https://texera.example/callback/orcid",
        })
      : new Response("client")
  );
  const loadScript = vi.fn().mockResolvedValue(undefined);
  const assign = vi.fn();
  const sessionStorage = storage();
  return { api: new ApiClient({ fetch }), socialLogin, fetch, loadScript, assign, sessionStorage };
}

describe("SocialAuth", () => {
  it("uses server-owned ORCID parameters and fresh one-time state", async () => {
    const test = setup();
    const social = new SocialAuth(test.api, test, {
      ...test,
      location: { origin: "https://texera.example", assign: test.assign },
      randomUUID: () => "fresh-state",
    });
    await social.signInWithOrcid();
    const url = new URL(test.assign.mock.calls[0][0]);
    expect(url.origin).toBe("https://orcid.org");
    expect(url.searchParams.get("redirect_uri")).toBe("https://texera.example/callback/orcid");
    expect(url.searchParams.get("scope")).toBe("/authenticate");
    expect(url.searchParams.get("state")).toBe("fresh-state");
    expect(test.sessionStorage.getItem("orcid_state")).toBe("fresh-state");
  });

  it("checks ORCID state before errors or exchanges and refuses callback replay", async () => {
    const test = setup();
    const social = new SocialAuth(test.api, test, { ...test });
    test.sessionStorage.setItem("orcid_state", "expected");
    await expect(
      social.completeOrcid(new URLSearchParams("state=wrong&error=denied&error_description=attacker"))
    ).rejects.toThrow("could not be verified");
    expect(test.sessionStorage.getItem("orcid_state")).toBeNull();
    expect(test.socialLogin).not.toHaveBeenCalled();
    test.sessionStorage.setItem("orcid_state", "expected");
    const params = new URLSearchParams("state=expected&code=one-use");
    await social.completeOrcid(params);
    expect(test.socialLogin).toHaveBeenCalledWith("orcid", "one-use");
    await expect(social.completeOrcid(params)).rejects.toThrow("could not be verified");
    expect(test.socialLogin).toHaveBeenCalledTimes(1);
  });

  it("reports a correlated ORCID refusal and missing code without authenticating", async () => {
    const test = setup();
    const social = new SocialAuth(test.api, test, test);
    for (const query of ["state=expected&error=denied&error_description=Cancelled", "state=expected"]) {
      test.sessionStorage.setItem("orcid_state", "expected");
      await expect(social.completeOrcid(new URLSearchParams(query))).rejects.toThrow();
    }
    expect(test.socialLogin).not.toHaveBeenCalled();
  });

  it("loads Apple only on demand, uses popup mode and exchanges the identity token", async () => {
    const test = setup();
    const apple = {
      auth: { init: vi.fn(), signIn: vi.fn().mockResolvedValue({ authorization: { id_token: "identity" } }) },
    };
    const social = new SocialAuth(test.api, test, {
      ...test,
      getApple: () => apple,
      location: { origin: "https://texera.example", assign: test.assign },
    });
    expect(test.loadScript).not.toHaveBeenCalled();
    await social.signInWithApple();
    expect(apple.auth.init).toHaveBeenCalledWith({
      clientId: "client",
      scope: "email",
      redirectURI: "https://texera.example",
      usePopup: true,
    });
    expect(test.socialLogin).toHaveBeenCalledWith("apple", "identity");
  });

  it("treats Apple popup cancellation as cancellation, and permits retry after SDK failure", async () => {
    const test = setup();
    const apple = { auth: { init: vi.fn(), signIn: vi.fn().mockRejectedValue({ error: "popup_closed_by_user" }) } };
    test.loadScript.mockRejectedValueOnce(new Error("SDK unavailable")).mockResolvedValue(undefined);
    const social = new SocialAuth(test.api, test, { ...test, getApple: () => apple });
    await expect(social.signInWithApple()).rejects.toThrow("SDK unavailable");
    await expect(social.signInWithApple()).resolves.toBeUndefined();
    expect(test.socialLogin).not.toHaveBeenCalled();
    expect(test.loadScript).toHaveBeenCalledTimes(2);
  });

  it("renders Google's own button and stops accepting its callback after disposal", async () => {
    const test = setup();
    let callback!: (response: { credential?: string }) => void;
    const google = {
      accounts: {
        id: {
          initialize: vi.fn(options => {
            callback = options.callback;
          }),
          renderButton: vi.fn(),
        },
      },
    };
    const social = new SocialAuth(test.api, test, { ...test, getGoogle: () => google });
    const button = document.createElement("div");
    const dispose = await social.mountGoogle(button);
    expect(google.accounts.id.renderButton).toHaveBeenCalledWith(button, expect.any(Object));
    callback({ credential: "google-token" });
    expect(test.socialLogin).toHaveBeenCalledWith("google", "google-token");
    dispose();
    callback({ credential: "late-token" });
    expect(test.socialLogin).toHaveBeenCalledTimes(1);
  });

  it("surfaces Google exchange failures through the supplied plain-text error callback", async () => {
    const test = setup();
    test.socialLogin.mockRejectedValue(new Error("Rejected"));
    let callback!: (response: { credential?: string }) => void;
    const google = {
      accounts: {
        id: {
          initialize: (options: { callback: typeof callback }) => {
            callback = options.callback;
          },
          renderButton: vi.fn(),
        },
      },
    };
    const social = new SocialAuth(test.api, test, { ...test, getGoogle: () => google });
    const error = vi.fn();
    await social.mountGoogle(document.createElement("div"), error);
    callback({ credential: "bad" });
    await vi.waitFor(() => expect(error).toHaveBeenCalledWith(expect.objectContaining({ message: "Rejected" })));
  });
});
