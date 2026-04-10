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
import { BehaviorSubject, Observable } from "rxjs";
import {HttpClient, HttpParams} from "@angular/common/http";


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
  // private pveSubject = new BehaviorSubject<string | null>(null);
  // private workflowIdSubject = new BehaviorSubject<string | null>(null);

  private cuidSubject = new BehaviorSubject<number | null>(null);

  private pveNameSubject = new BehaviorSubject<string | null>(null);

  constructor(private http: HttpClient) {}

  setCuid(cuid: number): void {
    this.cuidSubject.next(cuid);
  }

  setPveName(pveName: string): void {
    this.pveNameSubject.next(pveName);
  }

  private requireCuid(): number {
    const cuid = this.cuidSubject.value;
    if (cuid === null) {
      throw new Error("cuid is not set");
    }
    return cuid;
  }

  private requirePveName(): string {
    const pveName = this.pveNameSubject.value;
    if (pveName === null){
      throw new Error("Environment Name is not set");
    }

    return pveName;
  }

  private getAccessToken(): string | null {
    const token = localStorage.getItem("access_token");
    return token && token.trim().length > 0 ? token : null;
  }

  private buildAuthParams(): HttpParams {
    let params = new HttpParams().set("cuid", this.requireCuid().toString());
    const token = this.getAccessToken();
    if (token) {
      params = params.set("access-token", token);
    }
    const pveName = this.requirePveName();
    params = params.set("pveName", pveName);
    return params;
  }

  private buildBaseParams(): HttpParams {
    let params = new HttpParams();
    const token = this.getAccessToken();
    if (token) {
      params = params.set("access-token", token);
    }
    return params;
  }

  getInstalledPackages(): Observable<PackageResponse> {
    const params = this.buildAuthParams();
    return this.http.get<PackageResponse>("/pve/packages", { params });
  }

  fetchPVEs(cuid: number): Observable<PvePackageResponse[]> {
    const params = this.buildBaseParams().set("cuid", cuid.toString());
    return this.http.get<PvePackageResponse[]>("/pve/pves", { params });
  }

  deletePackage(packageName: string): Observable<string[]> {
    const params = this.buildAuthParams();
    const url = `/pve/uninstall/${packageName}`;
    return this.http.post<string[]>(url, {}, { params });
  }

  getEnvironments(): Observable<string[]>{
    let params = new HttpParams().set("cuid", this.requireCuid().toString());
    const token = this.getAccessToken();
    if (token) {
      params = params.set("access-token", token);
    }

    return this.http.get<string[]>("/pve/environments", { params });
  }
}
