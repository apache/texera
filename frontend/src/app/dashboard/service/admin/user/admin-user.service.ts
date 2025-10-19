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
import { ExecutionQuota, File, Role, User, Workflow } from "../../../../common/type/user";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";

// Permission field schema definition
export interface PermissionFieldSchema {
  fieldType: string; // "boolean", "number", or "string"
  possibleValues: any[]; // List of possible values, empty list if not a category field
  defaultValue: any; // Default value for this permission
  description: string; // Human-readable description of what this permission does
}

// Permission template containing all available permissions
export interface PermissionTemplate {
  permissions: { [key: string]: PermissionFieldSchema };
}

export const USER_BASE_URL = `${AppSettings.getApiEndpoint()}/admin/user`;
export const USER_LIST_URL = `${USER_BASE_URL}/list`;
export const USER_UPDATE_URL = `${USER_BASE_URL}/update`;
export const USER_ADD_URL = `${USER_BASE_URL}/add`;
export const USER_PERMISSION_TEMPLATE_URL = `${USER_BASE_URL}/permission`;
export const USER_CREATED_FILES = `${USER_BASE_URL}/uploaded_files`;
export const USER_UPLOADED_DATASE_SIZE = `${USER_BASE_URL}/dataset_size`;
export const USER_UPLOADED_DATASET_COUNT = `${USER_BASE_URL}/uploaded_dataset`;
export const USER_CREATED_DATASETS = `${USER_BASE_URL}/created_datasets`;
export const USER_CREATED_WORKFLOWS = `${USER_BASE_URL}/created_workflows`;
export const USER_ACCESS_WORKFLOWS = `${USER_BASE_URL}/access_workflows`;
export const USER_ACCESS_FILES = `${USER_BASE_URL}/access_files`;
export const USER_QUOTA_SIZE = `${USER_BASE_URL}/user_quota_size`;
export const USER_DELETE_EXECUTION_COLLECTION = `${USER_BASE_URL}/deleteCollection`;

@Injectable({
  providedIn: "root",
})
export class AdminUserService {
  constructor(private http: HttpClient) {}

  public getUserList(): Observable<ReadonlyArray<User>> {
    return this.http.get<ReadonlyArray<User>>(`${USER_LIST_URL}`);
  }

  public updateUser(
    uid: number,
    name: string,
    email: string,
    role: Role,
    comment: string,
    permission?: string
  ): Observable<void> {
    return this.http.put<void>(`${USER_UPDATE_URL}`, {
      uid: uid,
      name: name,
      email: email,
      role: role,
      comment: comment,
      permission: permission,
    });
  }

  public getPermissionTemplate(): Observable<PermissionTemplate> {
    return this.http.get<PermissionTemplate>(`${USER_PERMISSION_TEMPLATE_URL}`);
  }

  public addUser(): Observable<Response> {
    return this.http.post<Response>(`${USER_ADD_URL}/`, {});
  }

  public getUploadedFiles(uid: number): Observable<ReadonlyArray<File>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<File>>(`${USER_CREATED_FILES}`, { params: params });
  }

  public getCreatedDatasets(uid: number): Observable<ReadonlyArray<DatasetQuota>> {
    return this.http.get<ReadonlyArray<DatasetQuota>>(`${USER_CREATED_DATASETS}`);
  }

  public getCreatedWorkflows(uid: number): Observable<ReadonlyArray<Workflow>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<Workflow>>(`${USER_CREATED_WORKFLOWS}`, { params: params });
  }

  public getAccessFiles(uid: number): Observable<ReadonlyArray<number>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_FILES}`, { params: params });
  }

  public getAccessWorkflows(uid: number): Observable<ReadonlyArray<number>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<number>>(`${USER_ACCESS_WORKFLOWS}`, { params: params });
  }

  public getExecutionQuota(uid: number): Observable<ReadonlyArray<ExecutionQuota>> {
    let params = new HttpParams().set("user_id", uid.toString());
    return this.http.get<ReadonlyArray<ExecutionQuota>>(`${USER_QUOTA_SIZE}`, { params: params });
  }

  public deleteExecutionCollection(eid: number): Observable<void> {
    return this.http.delete<void>(`${USER_DELETE_EXECUTION_COLLECTION}/${eid.toString()}`);
  }
}
