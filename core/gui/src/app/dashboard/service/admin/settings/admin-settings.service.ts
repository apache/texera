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
export class AdminSettingsService {
  private readonly BASE_URL = "/api/admin/settings";
  constructor(private http: HttpClient) {}

  getSetting(key: string): Observable<SiteSetting> {
    return this.http.get<SiteSetting>(`${this.BASE_URL}/${key}`, {
      withCredentials: true,
    });
  }

  updateSetting(setting: SiteSetting): Observable<void> {
    return this.http.put<void>(`${this.BASE_URL}/${setting.key}`, setting, {
      withCredentials: true,
    });
  }

  deleteSetting(key: string): Observable<void> {
    return this.http.post<void>(
      `${this.BASE_URL}/delete/${key}`,
      {},
      {
        withCredentials: true,
      }
    );
  }
}
