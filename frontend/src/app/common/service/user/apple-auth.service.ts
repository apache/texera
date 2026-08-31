/*
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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { firstValueFrom, Observable } from "rxjs";
import { AppSettings } from "../../app-setting";

/** The subset of Apple's JS SDK this service uses. */
interface AppleIdSignInResponse {
  authorization?: { id_token?: string };
}

declare const AppleID: {
  auth: {
    init(config: { clientId: string; scope: string; redirectURI: string; usePopup: boolean }): void;
    signIn(): Promise<AppleIdSignInResponse>;
  };
};

const APPLE_SDK_URL = "https://appleid.cdn-apple.com/appleauth/static/jsapi/appleid/1/en_US/appleid.auth.js";

/**
 * Sign in with Apple, driven through Apple's own SDK because
 * `@abacritt/angularx-social-login` ships no Apple provider. The script is fetched on first use, so
 * a deployment with `appleLogin` off never calls Apple at all.
 *
 * `usePopup` keeps the identity token in the page — Apple posts to `redirectURI` otherwise — which
 * is what lets it go straight to `/auth/apple/login`. Apple still requires `redirectURI` to be
 * registered against the Services ID and HTTPS on a verified domain; it rejects `http://localhost`,
 * so a local click-through needs a tunnel.
 */
@Injectable({
  providedIn: "root",
})
export class AppleAuthService {
  private sdkLoading?: Promise<void>;

  constructor(private http: HttpClient) {}

  getClientId(): Observable<string> {
    return this.http.get(`${AppSettings.getApiEndpoint()}/auth/apple/clientid`, { responseType: "text" });
  }

  /**
   * Run Apple's popup flow and resolve with the identity token to hand to the backend, or
   * `undefined` when the user dismisses it — a cancelled sign-in is not an error to report.
   */
  async signIn(): Promise<string | undefined> {
    const clientId = await firstValueFrom(this.getClientId());
    await this.loadSdk();

    AppleID.auth.init({
      clientId,
      scope: "email",
      redirectURI: window.location.origin,
      usePopup: true,
    });

    try {
      const response = await AppleID.auth.signIn();
      return response?.authorization?.id_token;
    } catch {
      // Apple rejects with `{ error: "popup_closed_by_user" }` on dismissal, and with the same
      // shape for a genuine failure; neither carries anything worth surfacing to the user.
      return undefined;
    }
  }

  private loadSdk(): Promise<void> {
    if (typeof AppleID !== "undefined") return Promise.resolve();
    if (this.sdkLoading) return this.sdkLoading;

    this.sdkLoading = new Promise<void>((resolve, reject) => {
      const script = document.createElement("script");
      script.src = APPLE_SDK_URL;
      script.async = true;
      script.onload = () => resolve();
      script.onerror = () => {
        // Let a later click retry rather than caching the failure forever.
        this.sdkLoading = undefined;
        reject(new Error("Failed to load Apple's sign-in SDK"));
      };
      document.head.appendChild(script);
    });
    return this.sdkLoading;
  }
}
