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
import {
  WorkflowPersistService,
  WORKFLOW_BASE_URL,
  WORKFLOW_ID_URL,
  WORKFLOW_OWNER_URL,
  WORKFLOW_SEARCH_URL,
} from "./workflow-persist.service";
import { jsonCast } from "../../util/storage";
import { Workflow, WorkflowContent } from "../../type/workflow";
import { AppSettings } from "../../app-setting";
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { SearchFilterParameters, toQueryStrings } from "../../../dashboard/type/search-filter-parameters";
import { last } from "rxjs/operators";

describe("WorkflowPersistService", () => {
  let service: WorkflowPersistService;
  let httpTestingController: HttpTestingController;
  const testContent =
    '{"operators":[{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9",' +
    '"operatorType":"Limit","operatorProperties":{"limit":2},"inputPorts":[{"portID":"input-0","displayName":""}],' +
    '"outputPorts":[{"portID":"output-0","displayName":null}],"showAdvanced":false},' +
    '{"operatorID":"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914","operatorType":"SimpleSink",' +
    '"operatorProperties":{},"inputPorts":[{"portID":"input-0","displayName":""}],"outputPorts":[],' +
    '"showAdvanced":false},{"operatorID":"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50",' +
    '"operatorType":"MySQLSource","operatorProperties":{"port":"default","search":false,"progressive":false,' +
    '"min":"auto","max":"auto","interval":1000000000,"host":"localhost"},"inputPorts":[],' +
    '"outputPorts":[{"portID":"output-0","displayName":""}],"showAdvanced":false}],' +
    '"operatorPositions":{"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9":{"x":200,"y":212},' +
    '"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914":{"x":392,"y":218},' +
    '"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50":{"x":36,"y":214}},' +
    '"links":[{"linkID":"link-ea977a06-3ef5-4c80-b31a-4013cfb8321d",' +
    '"source":{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9","portID":"output-0"},' +
    '"target":{"operatorID":"SimpleSink-operator-e4a77a32-e3c9-4c40-a26d-a1aa103cc914","portID":"input-0"}},' +
    '{"linkID":"link-c94e24a6-2c77-40cf-ba22-1a7ffba64b7d","source":{"operatorID":' +
    '"MySQLSource-operator-1ee619b1-8884-4564-a136-29ef77dfcc50","portID":"output-0"},"target":' +
    '{"operatorID":"Limit-operator-a11370eb-940a-4f10-8b36-8b413b2396c9","portID":"input-0"}}],"breakpoints":{}}';
  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
    });
    service = TestBed.inject(WorkflowPersistService);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should send http post request once", () => {
    service
      .createWorkflow(jsonCast<WorkflowContent>(testContent), "testname")
      .pipe(last())
      .subscribe(value => {
        expect(value).toBeTruthy();
      });
    httpTestingController.expectOne(request => request.method === "POST");
  });

  it("should check if workflow content and name returned correctly", () => {
    service
      .createWorkflow(jsonCast<WorkflowContent>(testContent), "testname")
      .pipe(last())
      .subscribe(value => {
        expect(value.workflow.name).toEqual("testname_copy");
        expect(value.workflow.content).toEqual(jsonCast<WorkflowContent>(testContent));
      });
  });

  const API = AppSettings.getApiEndpoint();

  it("retrieveWorkflow issues a GET and parses the workflow content", () => {
    const raw = { wid: 5, name: "wf", content: '{"operators":[]}' } as unknown as Workflow;
    let result: Workflow | undefined;
    service.retrieveWorkflow(5).subscribe(w => (result = w));

    const req = httpTestingController.expectOne(`${API}/${WORKFLOW_BASE_URL}/5`);
    expect(req.request.method).toBe("GET");
    req.flush(raw);

    // parseWorkflowInfo turns the string content into a parsed object
    expect(result?.content).toEqual({ operators: [] });
  });

  it("retrieveWorkflowIDs issues a GET to the workflow-ids url", () => {
    let result: number[] | undefined;
    service.retrieveWorkflowIDs().subscribe(ids => (result = ids));

    const req = httpTestingController.expectOne(`${API}/${WORKFLOW_ID_URL}`);
    expect(req.request.method).toBe("GET");
    req.flush([1, 2, 3]);

    expect(result).toEqual([1, 2, 3]);
  });

  it("retrieveOwners issues a GET to the owners url", () => {
    let result: string[] | undefined;
    service.retrieveOwners().subscribe(owners => (result = owners));

    const req = httpTestingController.expectOne(`${API}/${WORKFLOW_OWNER_URL}`);
    expect(req.request.method).toBe("GET");
    req.flush(["alice@example.com", "bob@example.com"]);

    expect(result).toEqual(["alice@example.com", "bob@example.com"]);
  });

  it("searchWorkflows issues a GET to the search url and maps each entry", () => {
    const params: SearchFilterParameters = {
      createDateStart: null,
      createDateEnd: null,
      modifiedDateStart: null,
      modifiedDateEnd: null,
      owners: [],
      ids: [],
      operators: [],
      projectIds: [],
    };
    const keywords = ["test"];
    const entry = { workflow: { wid: 1, name: "w", content: '{"operators":[]}' } } as unknown as DashboardWorkflow;

    let result: DashboardWorkflow[] | undefined;
    service.searchWorkflows(keywords, params).subscribe(r => (result = r));

    const req = httpTestingController.expectOne(`${API}/${WORKFLOW_SEARCH_URL}?${toQueryStrings(keywords, params)}`);
    expect(req.request.method).toBe("GET");
    req.flush([entry]);

    expect(result?.length).toBe(1);
    // the mapping adds a `dashboardWorkflowEntry`; parseWorkflowInfo mutates the shared workflow
    // in place, so the entry's own `workflow.content` is parsed too.
    expect(result?.[0]).toHaveProperty("dashboardWorkflowEntry");
    expect(result?.[0]?.workflow?.content).toEqual({ operators: [] });
  });
});
