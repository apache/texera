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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { GuiConfig } from "../type/gui-config";
import { catchError, forkJoin, map, Observable, of, switchMap, tap, throwError } from "rxjs";
import { AppSettings } from "../app-setting";

// AuthService also owns this key but injects GuiConfigService, so importing it
// back here would create a dependency cycle. Duplicating the constant is the
// least invasive fix.
const ACCESS_TOKEN_KEY = "access_token";

type PreLoginConfig = Pick<GuiConfig, "localLogin" | "googleLogin" | "defaultLocalUser" | "attributionEnabled">;
type GuiOnlyConfig = Omit<GuiConfig, keyof PreLoginConfig | "inviteOnly">;
type UserSystemConfig = Pick<GuiConfig, "inviteOnly">;

@Injectable({ providedIn: "root" })
export class GuiConfigService {
  private config: Partial<GuiConfig> = {};

  constructor(private http: HttpClient) {}

  /**
   * APP_INITIALIZER entry point. Always loads /config/pre-login (anonymous). If
   * a JWT is already in localStorage (browser reload while logged in), chains
   * /config/gui + /config/user-system in the same await so the full config is
   * available before any post-login component mounts.
   */
  load(): Observable<Partial<GuiConfig>> {
    return this.loadPreLogin().pipe(
      switchMap(() => {
        if (!GuiConfigService.hasStoredAccessToken()) {
          return of(this.config);
        }
        return this.loadPostLogin().pipe(
          // Expired or malformed token → /config/gui returns 403. Continue
          // with pre-login only; UserService.loginWithExistingToken detects
          // expiry on its own, so we shouldn't block bootstrap on it.
          catchError((err: unknown) => {
            console.warn("Failed to load authenticated config; continuing with pre-login only.", err);
            return of(this.config);
          })
        );
      })
    );
  }

  loadPreLogin(): Observable<Partial<GuiConfig>> {
    return this.http.get<PreLoginConfig>(`${AppSettings.getApiEndpoint()}/config/pre-login`).pipe(
      tap(preLogin => {
        this.config = { ...this.config, ...preLogin };
      }),
      map(() => this.config),
      catchError((error: unknown) => {
        console.error("Failed to load pre-login configuration:", error);
        return throwError(() => new Error(`Failed to load pre-login configuration from backend: ${error}`));
      })
    );
  }

  /**
   * Fetches the authenticated portion of the configuration. Runs after the
   * frontend has a valid JWT — called from APP_INITIALIZER on bootstrap when a
   * stored token exists, and from UserService.handleAccessToken on fresh login.
   */
  loadPostLogin(): Observable<Partial<GuiConfig>> {
    const guiConfig$ = this.http.get<GuiOnlyConfig>(`${AppSettings.getApiEndpoint()}/config/gui`);
    const userSystemConfig$ = this.http.get<UserSystemConfig>(`${AppSettings.getApiEndpoint()}/config/user-system`);
    return forkJoin([guiConfig$, userSystemConfig$]).pipe(
      tap(([guiConfig, userSystemConfig]) => {
        this.config = { ...this.config, ...guiConfig, ...userSystemConfig };
      }),
      map(() => this.config)
    );
  }

  get env(): GuiConfig {
    return this.config as GuiConfig;
  }

  private static hasStoredAccessToken(): boolean {
    return typeof localStorage !== "undefined" && localStorage.getItem(ACCESS_TOKEN_KEY) != null;
  }
}
