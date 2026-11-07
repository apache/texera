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

import { ApiClient } from "./api-client";
import { v4 as uuid } from "uuid";
import type { AuthStore } from "./auth";

export const ORCID_STATE_KEY = "orcid_state";
const GOOGLE_SCRIPT = "https://accounts.google.com/gsi/client";
const APPLE_SCRIPT = "https://appleid.cdn-apple.com/appleauth/static/jsapi/appleid/1/en_US/appleid.auth.js";

interface GoogleIdentity {
  accounts: {
    id: {
      initialize(options: {
        client_id: string;
        callback: (response: { credential?: string }) => void;
        auto_select: boolean;
      }): void;
      renderButton(element: HTMLElement, options: { theme: string; size: string; width: number }): void;
    };
  };
}
interface AppleIdentity {
  auth: {
    init(options: { clientId: string; scope: string; redirectURI: string; usePopup: boolean }): void;
    signIn(): Promise<{ authorization?: { id_token?: string } }>;
  };
}
export interface SocialAuthOptions {
  sessionStorage?: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  loadScript?: (url: string) => Promise<void>;
  location?: { origin: string; assign(url: string): void };
  randomUUID?: () => string;
  getGoogle?: () => GoogleIdentity | undefined;
  getApple?: () => AppleIdentity | undefined;
}

const scripts = new Map<string, Promise<void>>();
function loadBrowserScript(url: string): Promise<void> {
  const existing = scripts.get(url);
  if (existing) return existing;
  const loading = new Promise<void>((resolve, reject) => {
    const script = document.createElement("script");
    script.src = url;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => {
      script.remove();
      reject(new Error("The sign-in provider could not be loaded. Please try again."));
    };
    document.head.appendChild(script);
  }).catch(error => {
    scripts.delete(url);
    throw error;
  });
  scripts.set(url, loading);
  return loading;
}

/** Provider handshakes stay separate from the Texera session and its account-completion gates. */
export class SocialAuth {
  private readonly storage: Pick<Storage, "getItem" | "setItem" | "removeItem">;
  private readonly loadScript: (url: string) => Promise<void>;
  private readonly location: { origin: string; assign(url: string): void };
  private readonly randomUUID: () => string;
  private readonly getGoogle: () => GoogleIdentity | undefined;
  private readonly getApple: () => AppleIdentity | undefined;
  private appleReady?: Promise<AppleIdentity>;

  constructor(
    private readonly api: ApiClient,
    private readonly auth: Pick<AuthStore, "socialLogin">,
    options: SocialAuthOptions = {}
  ) {
    this.storage = options.sessionStorage ?? globalThis.sessionStorage;
    this.loadScript = options.loadScript ?? loadBrowserScript;
    this.location = options.location ?? globalThis.location;
    this.randomUUID = options.randomUUID ?? (() => uuid());
    this.getGoogle =
      options.getGoogle ?? (() => (globalThis as typeof globalThis & { google?: GoogleIdentity }).google);
    this.getApple = options.getApple ?? (() => (globalThis as typeof globalThis & { AppleID?: AppleIdentity }).AppleID);
  }

  async mountGoogle(element: HTMLElement, onError?: (error: Error) => void): Promise<() => void> {
    const clientId = await this.api.get<string>("auth/google/clientid", { responseType: "text" });
    await this.loadScript(GOOGLE_SCRIPT);
    const google = this.getGoogle();
    if (!google) throw new Error("Google sign-in is unavailable.");
    let active = true;
    google.accounts.id.initialize({
      client_id: clientId,
      auto_select: false,
      callback: response => {
        if (!active || !response.credential) return;
        void this.auth.socialLogin("google", response.credential).catch(error => {
          if (active) onError?.(error instanceof Error ? error : new Error("Google sign-in failed."));
        });
      },
    });
    google.accounts.id.renderButton(element, { theme: "outline", size: "large", width: 320 });
    return () => {
      active = false;
      element.replaceChildren();
    };
  }

  async signInWithApple(): Promise<void> {
    const apple = await this.configureApple();
    try {
      const response = await apple.auth.signIn();
      const token = response?.authorization?.id_token;
      if (token) await this.auth.socialLogin("apple", token);
    } catch (error) {
      if ((error as { error?: string })?.error === "popup_closed_by_user") return;
      throw error instanceof Error ? error : new Error("Apple sign-in failed.");
    }
  }

  async signInWithOrcid(): Promise<void> {
    const config = await this.api.get<{ clientId: string; authorizeUrl: string; redirectUri: string }>(
      "auth/orcid/config"
    );
    if (!config.clientId || !config.authorizeUrl || !config.redirectUri)
      throw new Error("ORCID sign-in is unavailable.");
    const url = new URL(config.authorizeUrl);
    if (!["https:", "http:"].includes(url.protocol)) throw new Error("ORCID sign-in is unavailable.");
    const state = this.randomUUID();
    this.storage.setItem(ORCID_STATE_KEY, state);
    url.searchParams.set("client_id", config.clientId);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("scope", "/authenticate");
    url.searchParams.set("redirect_uri", config.redirectUri);
    url.searchParams.set("state", state);
    this.location.assign(url.toString());
  }

  async completeOrcid(params: URLSearchParams): Promise<void> {
    const expectedState = this.storage.getItem(ORCID_STATE_KEY);
    this.storage.removeItem(ORCID_STATE_KEY);
    if (expectedState === null || params.get("state") !== expectedState) {
      throw new Error("ORCID sign-in could not be verified. Please try again.");
    }
    if (params.has("error")) throw new Error(params.get("error_description") || "ORCID sign-in was not completed.");
    const code = params.get("code");
    if (!code) throw new Error("ORCID sign-in was not completed.");
    await this.auth.socialLogin("orcid", code);
  }

  private configureApple(): Promise<AppleIdentity> {
    if (!this.appleReady) {
      this.appleReady = (async () => {
        const clientId = await this.api.get<string>("auth/apple/clientid", { responseType: "text" });
        await this.loadScript(APPLE_SCRIPT);
        const apple = this.getApple();
        if (!apple) throw new Error("Apple sign-in is unavailable.");
        apple.auth.init({ clientId, scope: "email", redirectURI: this.location.origin, usePopup: true });
        return apple;
      })().catch(error => {
        this.appleReady = undefined;
        throw error;
      });
    }
    return this.appleReady;
  }
}
