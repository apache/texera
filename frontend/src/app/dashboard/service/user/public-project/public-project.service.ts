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
import { PublicProject } from "../../../type/dashboard-project.interface";

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/public/project`;

@Injectable({
  providedIn: "root",
})
export class PublicProjectService {
  constructor(private http: HttpClient) {}

  public getType(pid: number): Observable<string> {
    return this.http.get(`${USER_BASE_URL}/type/${pid}`, { responseType: "text" });
  }

  public makePublic(pid: number): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/public/${pid}`, null);
  }

  public makePrivate(pid: number): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/private/${pid}`, null);
  }

  public getPublicProjects(): Observable<PublicProject[]> {
    return this.http.get<PublicProject[]>(`${USER_BASE_URL}/list`);
  }

  public addPublicProjects(CheckedId: number[]): Observable<void> {
    return this.http.put<void>(`${USER_BASE_URL}/add`, CheckedId);
  }
}
