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
import { Observable } from "rxjs";
import { map } from "rxjs/operators";

/**
 * Service for managing site-wide settings (key-value pairs) via REST API.
 * All values are stored and retrieved as plain strings.
 */

@Injectable({
  providedIn: "root",
})
export class AdminSettingsService {
  private readonly BASE_URL = "/api/admin/settings";
  private readonly DEFAULT_LOGO_PATH = "assets/logos/logo.png";
  private readonly DEFAULT_MINI_LOGO_PATH = "assets/logos/full_logo_small.png";
  private readonly DEFAULT_FAVICON_PATH = "assets/logos/favicon-32x32.png";

  constructor(private http: HttpClient) {}

  getSetting(key: string): Observable<string> {
    return this.http
      .get<{ key: string; value: string }>(`${this.BASE_URL}/${key}`)
      .pipe(map(resp => resp?.value ?? null));
  }

  getLogoPath(): Observable<string> {
    return this.getSetting("logo").pipe(map(url => url || this.DEFAULT_LOGO_PATH));
  }

  getMiniLogoPath(): Observable<string> {
    return this.getSetting("mini_logo").pipe(map(url => url || this.DEFAULT_MINI_LOGO_PATH));
  }

  getFaviconPath(): Observable<string> {
    return this.getSetting("favicon").pipe(
      map(url => {
        const path = url || this.DEFAULT_FAVICON_PATH;
        document.querySelectorAll("link[rel*='icon']").forEach(link => ((link as HTMLLinkElement).href = path));
        return path;
      })
    );
  }

  updateSetting(key: string, value: string): Observable<void> {
    return this.http.put<void>(`${this.BASE_URL}/${key}`, { value }, { withCredentials: true });
  }

  deleteSetting(key: string): Observable<void> {
    return this.http.post<void>(`${this.BASE_URL}/delete/${key}`, {});
  }
}
