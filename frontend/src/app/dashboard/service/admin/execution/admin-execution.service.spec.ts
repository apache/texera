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
import { flushHttpRequest, setupHttpServiceTest } from "../../../../common/testing/http-service-test-utils";
import { AdminExecutionService, WORKFLOW_BASE_URL } from "./admin-execution.service";

describe("AdminExecutionService", () => {
  let service: AdminExecutionService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    ({ service, httpMock } = setupHttpServiceTest(AdminExecutionService));
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

      flushHttpRequest(httpMock, r => r.url === `${WORKFLOW_BASE_URL}/executionList/10/2/name/asc`, "GET", payload, {
        params: { filter: "running,failed" },
      });
      expect(result).toEqual(payload);
    });

    it("sends an empty filter param when no filters are supplied", () => {
      service.getExecutionList(5, 0, "id", "desc", []).subscribe();

      flushHttpRequest(httpMock, r => r.url === `${WORKFLOW_BASE_URL}/executionList/5/0/id/desc`, "GET", [], {
        params: { filter: "" },
      });
    });
  });

  describe("getTotalWorkflows", () => {
    it("GETs the total-workflow count endpoint", () => {
      let count: number | undefined;

      service.getTotalWorkflows().subscribe(c => (count = c));

      flushHttpRequest(httpMock, `${WORKFLOW_BASE_URL}/totalWorkflow`, "GET", 123);
      expect(count).toEqual(123);
    });
  });
});
