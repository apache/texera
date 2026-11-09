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

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { catchError, map, shareReplay, tap } from "rxjs/operators";

/**
 * Service for managing site-wide settings (key-value pairs) via REST API.
 * All values are stored and retrieved as plain strings.
 */

@Injectable({
  providedIn: "root",
})
export class AdminSettingsService {
  private readonly BASE_URL = "/api/config/settings";

  // One request for all user-visible settings, shared by every consumer.
  // Invalidated by the write methods below, so a save through this service
  // never leaves the cache stale.
  private publicSettings$?: Observable<Record<string, string>>;

  constructor(private http: HttpClient) {}

  /**
   * Reads one of the user-visible settings (branding, sidebar tabs, upload
   * limits) through the aggregated anonymous endpoint. Emits null when the
   * key is absent from the payload, so callers must apply their own default.
   */
  getPublicSetting(key: string): Observable<string | null> {
    if (!this.publicSettings$) {
      this.publicSettings$ = this.http.get<Record<string, string>>(`${this.BASE_URL}/public`).pipe(
        // shareReplay would otherwise cache a failed fetch and replay the
        // error to every consumer forever; drop the cached observable so the
        // next getPublicSetting call retries the request.
        catchError((err: unknown) => {
          this.publicSettings$ = undefined;
          return throwError(() => err);
        }),
        shareReplay(1)
      );
    }
    return this.publicSettings$.pipe(map(settings => settings[key] ?? null));
  }

  /**
   * Reads every stored setting (including management-only keys) in one
   * payload. ADMIN-only; used by the admin settings page.
   */
  getAllSettings(): Observable<Record<string, string>> {
    return this.http.get<Record<string, string>>(this.BASE_URL);
  }

  updateSetting(key: string, value: string): Observable<void> {
    return this.http
      .put<void>(`${this.BASE_URL}/${key}`, { value }, { withCredentials: true })
      .pipe(tap(() => (this.publicSettings$ = undefined)));
  }

  resetSetting(key: string): Observable<void> {
    return this.http
      .post<void>(`${this.BASE_URL}/reset/${key}`, {})
      .pipe(tap(() => (this.publicSettings$ = undefined)));
  }
}
