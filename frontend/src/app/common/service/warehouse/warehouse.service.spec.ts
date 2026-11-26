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

import { HttpTestingController } from "@angular/common/http/testing";
import { flushHttpRequest, setupHttpServiceTest } from "../../testing/http-service-test-utils";
import { AppSettings } from "../../app-setting";
import { WAREHOUSE_BASE_URL, WAREHOUSE_STATUS_URL, WarehouseService } from "./warehouse.service";
import { DashboardWarehouse, WarehouseStatus } from "../../type/warehouse";

describe("WarehouseService", () => {
  let service: WarehouseService;
  let httpMock: HttpTestingController;

  const warehouse: DashboardWarehouse = {
    whid: 7,
    name: "mybucket",
    lakekeeperWarehouseName: "user-3-7",
    flavor: "local",
    createdAtMillis: 1723300000000,
    ownerName: "Alice",
    ownerAvatar: null,
  };

  beforeEach(() => {
    ({ service, httpMock } = setupHttpServiceTest(WarehouseService));
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("fetches the warehouse status", () => {
    let received: WarehouseStatus | undefined;
    service.getStatus().subscribe(status => (received = status));

    flushHttpRequest(httpMock, `${AppSettings.getApiEndpoint()}/${WAREHOUSE_STATUS_URL}`, "GET", {
      enabled: true,
      warehouses: [warehouse],
    });

    expect(received).toEqual({ enabled: true, warehouses: [warehouse] });
  });

  it("creates a warehouse with the given name", () => {
    let received: DashboardWarehouse | undefined;
    service.createWarehouse("mybucket").subscribe(created => (received = created));

    flushHttpRequest(httpMock, `${AppSettings.getApiEndpoint()}/${WAREHOUSE_BASE_URL}`, "POST", warehouse, {
      body: { name: "mybucket" },
    });

    expect(received).toEqual(warehouse);
  });

  it("deletes a warehouse by id", () => {
    let completed = false;
    service.deleteWarehouse(7).subscribe({ complete: () => (completed = true) });

    flushHttpRequest(httpMock, `${AppSettings.getApiEndpoint()}/${WAREHOUSE_BASE_URL}/7`, "DELETE", null);

    expect(completed).toBe(true);
  });

  it("holds the per-execution warehouse pick; undefined means no pick", () => {
    expect(service.getSelectedWarehouseIdValue()).toBeUndefined();

    const seen: (number | undefined)[] = [];
    service.getSelectedWarehouseId().subscribe(whid => seen.push(whid));

    service.selectWarehouse(7);
    expect(service.getSelectedWarehouseIdValue()).toBe(7);

    service.selectWarehouse(undefined);
    expect(service.getSelectedWarehouseIdValue()).toBeUndefined();

    expect(seen).toEqual([undefined, 7, undefined]);
  });
});
