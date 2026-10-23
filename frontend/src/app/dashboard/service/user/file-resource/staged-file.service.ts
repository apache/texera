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
import { HttpClient, HttpParams } from "@angular/common/http";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { FileResourceEndpoint } from "./file-resource-endpoint";

/**
 * The uncommitted-change operations every versioned resource shares: list what is staged, revert one
 * staged path, and stage a deletion. Addressing comes from the caller's FileResourceEndpoint.
 */
@Injectable({
  providedIn: "root",
})
export class StagedFileService {
  constructor(private http: HttpClient) {}

  /** Uncommitted changes, i.e. what a new version would consume. */
  public getDiff(endpoint: FileResourceEndpoint, resourceId: number): Observable<DatasetStagedObject[]> {
    return this.http.get<DatasetStagedObject[]>(
      `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/${resourceId}/diff`
    );
  }

  public resetFileDiff(endpoint: FileResourceEndpoint, resourceId: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.put<Response>(
      `${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/${resourceId}/diff`,
      {},
      { params }
    );
  }

  /** Stages a deletion of an already-committed file; the next version applies it. */
  public deleteFile(endpoint: FileResourceEndpoint, resourceId: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));

    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${endpoint.baseUrl}/${resourceId}/file`, {
      params,
    });
  }
}
