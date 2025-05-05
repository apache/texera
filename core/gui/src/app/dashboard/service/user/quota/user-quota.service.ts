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
import { Workflow, MongoExecution } from "../../../../common/type/user";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/quota`;
export const USER_CREATED_DATASETS = `${USER_BASE_URL}/created_datasets`;
export const USER_CREATED_WORKFLOWS = `${USER_BASE_URL}/created_workflows`;
export const USER_ACCESS_WORKFLOWS = `${USER_BASE_URL}/access_workflows`;
export const USER_MONGODB_SIZE = `${USER_BASE_URL}/mongodb_size`;
export const USER_DELETE_MONGODB_COLLECTION_NAME = `${USER_BASE_URL}/deleteCollection`;

@Injectable({
  providedIn: "root",
})
export class UserQuotaService {
  constructor(private http: HttpClient) {}

  public getCreatedDatasets(uid: number): Observable<ReadonlyArray<DatasetQuota>> {
    return this.http.get<ReadonlyArray<DatasetQuota>>(`${USER_CREATED_DATASETS}`);
  }

  public getCreatedWorkflows(uid: number): Observable<ReadonlyArray<Workflow>> {
    return this.http.get<ReadonlyArray<Workflow>>(`${USER_CREATED_WORKFLOWS}`);
  }

  public getAccessWorkflows(uid: number): Observable<ReadonlyArray<number>> {
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_WORKFLOWS}`);
  }

  public getMongoDBs(uid: number): Observable<ReadonlyArray<MongoExecution>> {
    return this.http.get<ReadonlyArray<MongoExecution>>(`${USER_MONGODB_SIZE}`);
  }

  public deleteMongoDBCollection(collectionName: string): Observable<void> {
    return this.http.delete<void>(`${USER_DELETE_MONGODB_COLLECTION_NAME}/${collectionName}`);
  }
}
