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
import { HttpErrorResponse } from "@angular/common/http";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { LogicalPlan } from "../../../../workspace/types/execute-workflow.interface";
import {
  WORKFLOW_TO_PYTHON_ENDPOINT,
  WorkflowToPythonResponse,
  WorkflowToPythonService,
} from "./workflow-to-python.service";

const URL = `api/${WORKFLOW_TO_PYTHON_ENDPOINT}`;

const plan: LogicalPlan = {
  operators: [
    { operatorID: "op1", operatorType: "CSVFileScan", fileName: "/u/ds/v1/a.csv" },
    { operatorID: "op2", operatorType: "Limit", limit: 10 },
  ],
  links: [
    {
      fromOpId: "op1",
      fromPortId: { id: 0, internal: false },
      toOpId: "op2",
      toPortId: { id: 0, internal: false },
    },
  ],
};

describe("WorkflowToPythonService", () => {
  let service: WorkflowToPythonService;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowToPythonService],
    });
    httpTestingController = TestBed.inject(HttpTestingController);
    service = TestBed.inject(WorkflowToPythonService);
  });

  afterEach(() => {
    httpTestingController.verify();
  });

  it("posts the plan's operators and links as JSON to the translator endpoint", () => {
    service.convertToPython(plan).subscribe();

    const req = httpTestingController.expectOne(URL);
    expect(req.request.method).toEqual("POST");
    expect(req.request.headers.get("Content-Type")).toEqual("application/json");
    expect(req.request.body).toEqual({
      operators: plan.operators,
      links: plan.links,
      opsToReuseResult: [],
      opsToViewResult: [],
    });
    req.flush({ type: "success", pythonCode: "" });
  });

  it("never forwards the plan's own result-viewing or reuse lists", () => {
    service.convertToPython({ ...plan, opsToViewResult: ["op2"], opsToReuseResult: ["op1"] }).subscribe();

    const req = httpTestingController.expectOne(URL);
    expect(req.request.body.opsToViewResult).toEqual([]);
    expect(req.request.body.opsToReuseResult).toEqual([]);
    expect(Object.keys(req.request.body).sort()).toEqual(
      ["links", "operators", "opsToReuseResult", "opsToViewResult"].sort()
    );
    req.flush({ type: "success", pythonCode: "" });
  });

  it("still posts an empty plan", () => {
    service.convertToPython({ operators: [], links: [] }).subscribe();

    const req = httpTestingController.expectOne(URL);
    expect(req.request.body.operators).toEqual([]);
    expect(req.request.body.links).toEqual([]);
    req.flush({ type: "success", pythonCode: "" });
  });

  it("passes a success response through as returned", () => {
    const response: WorkflowToPythonResponse = {
      type: "success",
      pythonCode: "import pandas as pd\n",
      helpersModule: "def helper(): ...\n",
      renamedSources: [{ from: "/u/ds/v1/a.csv", to: "a.csv" }],
    };
    let received: WorkflowToPythonResponse | undefined;
    service.convertToPython(plan).subscribe(value => (received = value));

    httpTestingController.expectOne(URL).flush(response);

    expect(received).toEqual(response);
  });

  it("passes a failure body through to the subscriber rather than raising it", () => {
    const response: WorkflowToPythonResponse = { type: "failure", errorMessage: "Unsupported operator: Sklearn" };
    let received: WorkflowToPythonResponse | undefined;
    let errored = false;
    service.convertToPython(plan).subscribe({ next: value => (received = value), error: () => (errored = true) });

    httpTestingController.expectOne(URL).flush(response);

    expect(errored).toBe(false);
    expect(received).toEqual(response);
  });

  it("hands an HTTP error to the subscriber's error callback", () => {
    let status: number | undefined;
    service.convertToPython(plan).subscribe({ error: (err: unknown) => (status = (err as HttpErrorResponse).status) });

    httpTestingController.expectOne(URL).flush("boom", { status: 500, statusText: "Server Error" });

    expect(status).toEqual(500);
  });
});
