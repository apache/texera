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
import { HttpClient, HttpParams } from "@angular/common/http";
import { AuthService } from "../../../common/service/user/auth.service";

export interface PackageResponse {
  system: string[];
  user: string[];
}

export interface PvePackageResponse {
  pveName: string;
  userPackages: string[];
}

@Injectable({ providedIn: "root" })
export class WorkflowPveService {
  constructor(private http: HttpClient) {}

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

  getSystemPackages(): Observable<PackageResponse> {
    const params = this.buildBaseParams();
    return this.http.get<PackageResponse>("/pve/system", { params });
  }

  fetchPVEs(cuid: number): Observable<PvePackageResponse[]> {
    const params = this.buildBaseParams().set("cuid", cuid.toString());
    return this.http.get<PvePackageResponse[]>("/pve/pves", { params });
  }

  deleteEnvironments(cuid: number) {
    return this.http.delete(`/pve/pves/${cuid}`);
  }

  createPveWebSocketUrl(cuid: number, pveName: string, isLocal: boolean, packages: string[] = []): string {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const query = encodeURIComponent(JSON.stringify(packages));

    const token = this.getAccessToken();
    const tokenParam = token ? `&access-token=${encodeURIComponent(token)}` : "";

    return (
      `${protocol}//${window.location.host}/wsapi/pve` +
      `?packages=${query}` +
      `&cuid=${cuid}` +
      `&pveName=${encodeURIComponent(pveName)}` +
      `&isLocal=${isLocal}` +
      tokenParam
    );
  }
}
