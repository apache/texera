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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { WORKFLOW_EXECUTIONS_API_BASE_URL, WorkflowExecutionsService } from "./workflow-executions.service";
import { ExecutionState } from "../../../../workspace/types/execute-workflow.interface";
import { WorkflowExecutionsEntry } from "../../../type/workflow-executions-entry";
import { WorkflowRuntimeStatistics } from "../../../type/workflow-runtime-statistics";

describe("WorkflowExecutionsService", () => {
  let service: WorkflowExecutionsService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowExecutionsService],
    });
    service = TestBed.inject(WorkflowExecutionsService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("retrieveLatestWorkflowExecution issues GET to the latest url", () => {
    let result: WorkflowExecutionsEntry | undefined;
    service.retrieveLatestWorkflowExecution(1).subscribe(res => (result = res));

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/1/latest`);
    expect(req.request.method).toBe("GET");
    const entry = { eId: 7 } as unknown as WorkflowExecutionsEntry;
    req.flush(entry);
    expect(result).toEqual(entry);
  });

  it("retrieveWorkflowExecutions issues GET without a status param when no statuses are given", () => {
    service.retrieveWorkflowExecutions(1).subscribe();

    const req = httpMock.expectOne(r => r.url === `${WORKFLOW_EXECUTIONS_API_BASE_URL}/1`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.has("status")).toBe(false);
    req.flush([]);
  });

  it("retrieveWorkflowExecutions sets the status param when statuses are given", () => {
    service.retrieveWorkflowExecutions(1, [ExecutionState.Running, ExecutionState.Completed]).subscribe();

    const req = httpMock.expectOne(r => r.url === `${WORKFLOW_EXECUTIONS_API_BASE_URL}/1`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.get("status")).toBe([ExecutionState.Running, ExecutionState.Completed].join(","));
    req.flush([]);
  });

  it("groupSetIsBookmarked issues PUT with the bookmark payload", () => {
    service.groupSetIsBookmarked(1, [10, 11], true).subscribe();

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/set_execution_bookmarks`);
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ wid: 1, eIds: [10, 11], isBookmarked: true });
    req.flush({});
  });

  it("groupDeleteWorkflowExecutions issues PUT with the delete payload", () => {
    service.groupDeleteWorkflowExecutions(1, [10, 11]).subscribe();

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/delete_executions`);
    expect(req.request.method).toBe("PUT");
    expect(req.request.body).toEqual({ wid: 1, eIds: [10, 11] });
    req.flush({});
  });

  it("updateWorkflowExecutionsName issues POST with the rename payload", () => {
    service.updateWorkflowExecutionsName(1, 10, "renamed").subscribe();

    const req = httpMock.expectOne(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/update_execution_name`);
    expect(req.request.method).toBe("POST");
    expect(req.request.body).toEqual({ wid: 1, eId: 10, executionName: "renamed" });
    req.flush({});
  });

  it("retrieveWorkflowRuntimeStatistics issues GET with the cuid param", () => {
    let result: WorkflowRuntimeStatistics[] | undefined;
    service.retrieveWorkflowRuntimeStatistics(1, 10, 5).subscribe(res => (result = res));

    const req = httpMock.expectOne(r => r.url === `${WORKFLOW_EXECUTIONS_API_BASE_URL}/1/stats/10`);
    expect(req.request.method).toBe("GET");
    expect(req.request.params.get("cuid")).toBe("5");
    const stats: WorkflowRuntimeStatistics[] = [];
    req.flush(stats);
    expect(result).toEqual(stats);
  });
});
