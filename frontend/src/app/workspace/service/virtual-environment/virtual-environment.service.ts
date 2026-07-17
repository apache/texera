/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { map } from "rxjs/operators";
import { HttpClient, HttpParams } from "@angular/common/http";
import { AuthService } from "../../../common/service/user/auth.service";
import { AppSettings } from "../../../common/app-setting";

export interface PackageResponse {
  system: string[];
}

export interface PvePackageResponse {
  pveName: string;
  userPackages: string[];
}

export interface UserPveRecord {
  veid: number;
  name: string;
  packages: Record<string, string>;
}

@Injectable({ providedIn: "root" })
export class WorkflowPveService {
  constructor(private http: HttpClient) {}

  savePve(name: string, packages: Record<string, string>): Observable<{ veid: number }> {
    return this.http.post<{ veid: number }>(`${AppSettings.getApiEndpoint()}/pve/db`, { name, packages });
  }

  updateUserPve(veid: number, name: string, packages: Record<string, string>): Observable<{ veid: number }> {
    return this.http.put<{ veid: number }>(`${AppSettings.getApiEndpoint()}/pve/db/${veid}`, { name, packages });
  }

  listUserPves(): Observable<UserPveRecord[]> {
    return this.http.get<UserPveRecord[]>(`${AppSettings.getApiEndpoint()}/pve/db`);
  }

  deleteUserPve(veid: number): Observable<void> {
    return this.http.delete<void>(`${AppSettings.getApiEndpoint()}/pve/db/${veid}`);
  }

  getAccessToken(): string | null {
    const token = AuthService.getAccessToken();
    return token && token.trim().length > 0 ? token : null;
  }

  private buildBaseParams(): HttpParams {
    let params = new HttpParams();
    const token = this.getAccessToken();
    if (token) {
      params = params.set("access-token", token);
    }
    return params;
  }

  getSystemPackages(cuid: number): Observable<PackageResponse> {
    const params = this.buildBaseParams().set("cuid", cuid.toString());
    return this.http.get<PackageResponse>(`${AppSettings.getApiEndpoint()}/pve/system`, { params });
  }

  fetchPVEs(cuid: number): Observable<PvePackageResponse[]> {
    const params = this.buildBaseParams().set("cuid", cuid.toString());
    return this.http.get<PvePackageResponse[]>(`${AppSettings.getApiEndpoint()}/pve/pves`, { params });
  }

  getUserPackages(cuid: number, pveName: string): Observable<string[]> {
    return this.fetchPVEs(cuid).pipe(map(pves => pves.find(pve => pve.pveName === pveName)?.userPackages ?? []));
  }

  deleteEnvironments(cuid: number) {
    return this.http.delete(`${AppSettings.getApiEndpoint()}/pve/pves/${cuid}`);
  }

  deletePackage(cuid: number, pveName: string, packageName: string) {
    const params = this.buildBaseParams();

    return this.http.delete<string[]>(
      `${AppSettings.getApiEndpoint()}/pve/${cuid}/${encodeURIComponent(pveName)}/packages/${encodeURIComponent(packageName)}`,
      { params }
    );
  }

  getPveWebSocketUrl(cuid: number, pveName: string, action: string, packages: string[] = []): string {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const query = encodeURIComponent(JSON.stringify(packages));

    const token = this.getAccessToken();
    const tokenParam = token ? `&access-token=${encodeURIComponent(token)}` : "";

    return (
      `${protocol}//${window.location.host}/wsapi/pve` +
      `?packages=${query}` +
      `&cuid=${cuid}` +
      `&pveName=${encodeURIComponent(pveName)}` +
      `&action=${action}` +
      tokenParam
    );
  }
}
