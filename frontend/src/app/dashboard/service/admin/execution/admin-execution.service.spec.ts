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
import { AdminExecutionService, WORKFLOW_BASE_URL } from "./admin-execution.service";

describe("AdminExecutionService", () => {
  let service: AdminExecutionService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [AdminExecutionService],
    });
    service = TestBed.inject(AdminExecutionService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("getExecutionList", () => {
    it("GETs the paginated endpoint and joins filters into a comma-separated query param", () => {
      const payload = [{ workflowId: 1 } as any];
      let result: readonly any[] | undefined;

      service.getExecutionList(10, 2, "name", "asc", ["running", "failed"]).subscribe(r => (result = r));

      const req = httpMock.expectOne(r => r.url === `${WORKFLOW_BASE_URL}/executionList/10/2/name/asc`);
      expect(req.request.method).toEqual("GET");
      expect(req.request.params.get("filter")).toEqual("running,failed");

      req.flush(payload);
      expect(result).toEqual(payload);
    });

    it("sends an empty filter param when no filters are supplied", () => {
      service.getExecutionList(5, 0, "id", "desc", []).subscribe();

      const req = httpMock.expectOne(r => r.url === `${WORKFLOW_BASE_URL}/executionList/5/0/id/desc`);
      expect(req.request.params.get("filter")).toEqual("");
      req.flush([]);
    });
  });

  describe("getTotalWorkflows", () => {
    it("GETs the total-workflow count endpoint", () => {
      let count: number | undefined;

      service.getTotalWorkflows().subscribe(c => (count = c));

      const req = httpMock.expectOne(`${WORKFLOW_BASE_URL}/totalWorkflow`);
      expect(req.request.method).toEqual("GET");
      req.flush(123);

      expect(count).toEqual(123);
    });
  });
});
