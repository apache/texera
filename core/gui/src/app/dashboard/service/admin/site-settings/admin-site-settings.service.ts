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

/**
 * Represents a single site setting as a key/value pair.
 */
export interface SiteSetting {
  key: string;
  value: string;
}

@Injectable({
  providedIn: "root",
})
export class SiteSettingsService {
  constructor(private http: HttpClient) {}

  /** Fetch all site settings from the backend API */
  getAllSettings(): Observable<SiteSetting[]> {
    return this.http.get<SiteSetting[]>("/api/admin/site-settings", { withCredentials: true });
  }

  /** Update (save) all site settings via the backend API */
  updateSettings(settings: SiteSetting[]): Observable<void> {
    return this.http.put<void>("/api/admin/site-settings", settings, { withCredentials: true });
  }
}
