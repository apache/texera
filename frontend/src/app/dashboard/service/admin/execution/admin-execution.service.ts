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

import { HttpClient, HttpParams } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { Execution } from "../../../../common/type/execution";

export const WORKFLOW_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/execution`;

@Injectable({
  providedIn: "root",
})
export class AdminExecutionService {
  constructor(private http: HttpClient) {}

  public getExecutionList(
    pageSize: number,
    pageIndex: number,
    sortField: string,
    sortDirection: string,
    filter: string[]
  ): Observable<ReadonlyArray<Execution>> {
    const params = new HttpParams().set("filter", filter.join(","));
    return this.http.get<ReadonlyArray<Execution>>(
      `${WORKFLOW_BASE_URL}/executionList/${pageSize}/${pageIndex}/${sortField}/${sortDirection}`,
      { params }
    );
  }

  public getTotalWorkflows(): Observable<number> {
    return this.http.get<number>(`${WORKFLOW_BASE_URL}/totalWorkflow`);
  }
}
