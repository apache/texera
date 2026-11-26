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

import { HttpClientTestingModule, HttpTestingController, TestRequest } from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { Observable } from "rxjs";
import {
  UserQuotaService,
  USER_CREATED_DATASETS,
  USER_CREATED_WORKFLOWS,
  USER_ACCESS_WORKFLOWS,
  USER_QUOTA_SIZE,
  USER_DELETE_EXECUTION_COLLECTION,
} from "./user-quota.service";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";
import { ExecutionQuota } from "../../../../common/type/user";

describe("UserQuotaService", () => {
  let service: UserQuotaService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
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

  it.each<[string, string, Parameters<TestRequest["flush"]>[0], (svc: UserQuotaService) => Observable<unknown>]>([
    [
      "getCreatedDatasets() GETs the created-datasets endpoint",
      USER_CREATED_DATASETS,
      [{ did: 1, name: "dataset-a", creationTime: 1700000000000, size: 2048 }] satisfies ReadonlyArray<DatasetQuota>,
      svc => svc.getCreatedDatasets(1),
    ],
    [
      "getCreatedWorkflows() GETs the created-workflows endpoint",
      USER_CREATED_WORKFLOWS,
      [{ wid: 1 } as any],
      svc => svc.getCreatedWorkflows(1),
    ],
    [
      "getAccessWorkflows() GETs the access-workflows endpoint",
      USER_ACCESS_WORKFLOWS,
      [10, 20],
      svc => svc.getAccessWorkflows(1),
    ],
    [
      "getExecutionQuota() GETs the quota-size endpoint",
      USER_QUOTA_SIZE,
      [
        { eid: 1, workflowId: 2, workflowName: "wf", resultBytes: 10, runTimeStatsBytes: 20, logBytes: 30 },
      ] satisfies ReadonlyArray<ExecutionQuota>,
      svc => svc.getExecutionQuota(1),
    ],
  ])("%s", (_name, endpoint, payload, call) => {
    let result: unknown;

    call(service).subscribe(r => (result = r));

    const req = httpMock.expectOne(endpoint);
    expect(req.request.method).toEqual("GET");
    req.flush(payload);

    expect(result).toEqual(payload);
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
