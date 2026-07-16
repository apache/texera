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

import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import {
  UserQuotaService,
  USER_CREATED_DATASETS,
  USER_CREATED_WORKFLOWS,
  USER_ACCESS_WORKFLOWS,
  USER_QUOTA_SIZE,
  USER_DELETE_EXECUTION_COLLECTION,
} from "./user-quota.service";

describe("UserQuotaService", () => {
  let service: UserQuotaService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [UserQuotaService],
    });
    service = TestBed.inject(UserQuotaService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("getCreatedDatasets() GETs the created-datasets endpoint", () => {
    const datasets = [{ datasetId: 1 } as any];
    let result: readonly any[] | undefined;

    service.getCreatedDatasets(1).subscribe(r => (result = r));

    const req = httpMock.expectOne(USER_CREATED_DATASETS);
    expect(req.request.method).toEqual("GET");
    req.flush(datasets);

    expect(result).toEqual(datasets);
  });

  it("getCreatedWorkflows() GETs the created-workflows endpoint", () => {
    service.getCreatedWorkflows(1).subscribe();

    const req = httpMock.expectOne(USER_CREATED_WORKFLOWS);
    expect(req.request.method).toEqual("GET");
    req.flush([]);
  });

  it("getAccessWorkflows() GETs the access-workflows endpoint", () => {
    let result: readonly number[] | undefined;
    service.getAccessWorkflows(1).subscribe(r => (result = r));

    const req = httpMock.expectOne(USER_ACCESS_WORKFLOWS);
    expect(req.request.method).toEqual("GET");
    req.flush([10, 20]);

    expect(result).toEqual([10, 20]);
  });

  it("getExecutionQuota() GETs the quota-size endpoint", () => {
    service.getExecutionQuota(1).subscribe();

    const req = httpMock.expectOne(USER_QUOTA_SIZE);
    expect(req.request.method).toEqual("GET");
    req.flush([]);
  });

  it("deleteExecutionCollection() DELETEs the per-execution endpoint", () => {
    service.deleteExecutionCollection(55).subscribe();

    const req = httpMock.expectOne(`${USER_DELETE_EXECUTION_COLLECTION}/55`);
    expect(req.request.method).toEqual("DELETE");
    req.flush(null);
  });

  it("deleteExecutionCollection() handles a server error response", () => {
    let errored = false;
    service.deleteExecutionCollection(7).subscribe({ error: () => (errored = true) });

    const req = httpMock.expectOne(`${USER_DELETE_EXECUTION_COLLECTION}/7`);
    req.flush("boom", { status: 500, statusText: "Server Error" });

    expect(errored).toBe(true);
  });
});
