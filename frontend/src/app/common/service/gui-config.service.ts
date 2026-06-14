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
// Fields served by /config/amber.
type AmberConfig = Pick<GuiConfig, "defaultDataTransferBatchSize">;
type GuiOnlyConfig = Omit<GuiConfig, keyof PreLoginConfig | keyof AmberConfig | "inviteOnly">;
type UserSystemConfig = Pick<GuiConfig, "inviteOnly">;

// One entry per backend config endpoint.
export type ConfigSource = "preLogin" | "gui" | "amber" | "userSystem";

@Injectable({ providedIn: "root" })
export class GuiConfigService {
  // Each endpoint's payload, kept under its own key.
  private configBySource: Partial<Record<ConfigSource, Partial<GuiConfig>>> = {};

  // Memoized flat merge. Rebuilt only when a source is written so that env
  // returns a stable reference: callers read env on every change-detection
  // cycle, and one call site mutates env through a two-way [(ngModel)] binding,
  // which only persists when the object identity is held across reads.
  private mergedCache: GuiConfig | null = null;

  // Merge precedence when a key appears in multiple sources (later wins).
  private static readonly MERGE_ORDER: ConfigSource[] = ["preLogin", "gui", "amber", "userSystem"];

  constructor(private http: HttpClient) {}

  /**
   * APP_INITIALIZER entry point. Always loads /config/pre-login (anonymous). If
   * a JWT is already in localStorage (browser reload while logged in), chains
   * /config/gui + /config/amber + /config/user-system in the same await so the
   * full config is available before any post-login component mounts.
   */
  load(): Observable<Partial<GuiConfig>> {
    return this.loadPreLogin().pipe(
      switchMap(() => {
        if (!GuiConfigService.hasStoredAccessToken()) {
          return of(this.env);
        }
        return this.loadPostLogin().pipe(
          // Expired or malformed token → /config/gui returns 403. Continue
          // with pre-login only; UserService.loginWithExistingToken detects
          // expiry on its own, so we shouldn't block bootstrap on it.
          catchError((err: unknown) => {
            console.warn("Failed to load authenticated config; continuing with pre-login only.", err);
            return of(this.env);
          })
        );
      })
    );
  }

  loadPreLogin(): Observable<Partial<GuiConfig>> {
    return this.http.get<PreLoginConfig>(`${AppSettings.getApiEndpoint()}/config/pre-login`).pipe(
      tap(preLogin => {
        this.setSource("preLogin", preLogin);
      }),
      map(() => this.env),
      catchError((error: unknown) => {
        console.error("Failed to load pre-login configuration:", error);
        return throwError(() => new Error(`Failed to load pre-login configuration from backend: ${error}`));
      })
    );
  }

  /**
   * Fetches the authenticated portion of the configuration. Runs after the
   * frontend has a valid JWT, called from APP_INITIALIZER on bootstrap when a
   * stored token exists, and from UserService.handleAccessToken on fresh login.
   */
  loadPostLogin(): Observable<Partial<GuiConfig>> {
    const guiConfig$ = this.http.get<GuiOnlyConfig>(`${AppSettings.getApiEndpoint()}/config/gui`);
    const amberConfig$ = this.http.get<AmberConfig>(`${AppSettings.getApiEndpoint()}/config/amber`);
    const userSystemConfig$ = this.http.get<UserSystemConfig>(`${AppSettings.getApiEndpoint()}/config/user-system`);
    return forkJoin([guiConfig$, amberConfig$, userSystemConfig$]).pipe(
      tap(([guiConfig, amberConfig, userSystemConfig]) => {
        this.setSource("gui", guiConfig);
        this.setSource("amber", amberConfig);
        this.setSource("userSystem", userSystemConfig);
      }),
      map(() => this.env)
    );
  }

  // Flat merge of all sources, memoized so reads return a stable reference.
  get env(): GuiConfig {
    if (this.mergedCache === null) {
      this.mergedCache = GuiConfigService.MERGE_ORDER.reduce(
        (merged, source) => ({ ...merged, ...this.configBySource[source] }),
        {} as Partial<GuiConfig>
      ) as GuiConfig;
    }
    return this.mergedCache;
  }

  // One endpoint's payload, in isolation. Returns a shallow copy so callers
  // cannot mutate the stored source and desync it from the memoized env view.
  source(name: ConfigSource): Partial<GuiConfig> {
    return { ...(this.configBySource[name] ?? {}) };
  }

  // Store a source payload and invalidate the merged view.
  private setSource(name: ConfigSource, payload: Partial<GuiConfig>): void {
    this.configBySource[name] = payload;
    this.mergedCache = null;
  }

  private static hasStoredAccessToken(): boolean {
    return typeof localStorage !== "undefined" && localStorage.getItem(ACCESS_TOKEN_KEY) != null;
  }
}
