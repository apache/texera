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
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { ShareAccess } from "../../../type/share-access.interface";
export const BASE = `${AppSettings.getApiEndpoint()}/access`;
@Injectable({
  providedIn: "root",
})
export class ShareAccessService {
  constructor(private http: HttpClient) {}

  public grantAccess(type: string, id: number, email: string, privilege: string): Observable<void> {
    // TODO: find better way to handle computing unit access url difference
    let url = ""
    if (type === "computing-unit") {
      url = `${AppSettings.getApiEndpoint()}/computing-unit/access/grant/${id}/${email}/${privilege}`
    } else {
      url = `${BASE}/${type}/grant/${id}/${email}/${privilege}`;
    }

    return this.http.put<void>(url, null);
  }

  public revokeAccess(type: string, id: number, username: string): Observable<void> {
    return this.http.delete<void>(`${BASE}/${type}/revoke/${id}/${username}`);
  }

  public getOwner(type: string, id: number): Observable<string> {
    return this.http.get(`${BASE}/${type}/owner/${id}`, { responseType: "text" });
  }

  public getAccessList(type: string, id: number | undefined): Observable<ReadonlyArray<ShareAccess>> {
    // TODO: find better way to handle computing unit access url difference
    let url = ""
    if (type === "computing-unit") {
      url = `${AppSettings.getApiEndpoint()}/computing-unit/access/list/${id}`
    } else {
      url = `${BASE}/${type}/list/${id}`;
    }

    return this.http.get<ReadonlyArray<ShareAccess>>(url);
  }
}
