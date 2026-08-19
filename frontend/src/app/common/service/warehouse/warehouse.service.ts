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
import { BehaviorSubject, Observable } from "rxjs";
import { AppSettings } from "../../app-setting";
import { DashboardWarehouse, WarehouseStatus } from "../../type/warehouse";

export const WAREHOUSE_BASE_URL = "warehouse";
export const WAREHOUSE_STATUS_URL = `${WAREHOUSE_BASE_URL}/status`;

/**
 * Client of the per-user warehouse API (#6870): feature status + the caller's
 * warehouses, create, and delete. Also holds the workflow-level "which warehouse
 * does the next execution write to" pick, so the on-canvas picker (writer) and
 * ExecuteWorkflowService (reader) don't need to know each other.
 */
@Injectable({
  providedIn: "root",
})
export class WarehouseService {
  // The warehouse the next execution writes to; undefined = the shared default
  // storage (the backend treats an absent warehouseId the same way).
  private selectedWarehouseIdSubject = new BehaviorSubject<number | undefined>(undefined);

  constructor(private http: HttpClient) {}

  public getStatus(): Observable<WarehouseStatus> {
    return this.http.get<WarehouseStatus>(`${AppSettings.getApiEndpoint()}/${WAREHOUSE_STATUS_URL}`);
  }

  public createWarehouse(name: string): Observable<DashboardWarehouse> {
    return this.http.post<DashboardWarehouse>(`${AppSettings.getApiEndpoint()}/${WAREHOUSE_BASE_URL}`, { name });
  }

  public deleteWarehouse(whid: number): Observable<void> {
    return this.http.delete<void>(`${AppSettings.getApiEndpoint()}/${WAREHOUSE_BASE_URL}/${whid}`);
  }

  public selectWarehouse(whid: number | undefined): void {
    this.selectedWarehouseIdSubject.next(whid);
  }

  public getSelectedWarehouseId(): Observable<number | undefined> {
    return this.selectedWarehouseIdSubject.asObservable();
  }

  public getSelectedWarehouseIdValue(): number | undefined {
    return this.selectedWarehouseIdSubject.value;
  }
}
