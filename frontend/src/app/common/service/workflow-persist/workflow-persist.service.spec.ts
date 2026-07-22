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
  WORKFLOW_PERSIST_URL,
  WORKFLOW_CREATE_URL,
  WORKFLOW_DUPLICATE_URL,
  WORKFLOW_DELETE_URL,
  WORKFLOW_LIST_URL,
  WORKFLOW_UPDATENAME_URL,
  WORKFLOW_UPDATEDESCRIPTION_URL,
  WORKFLOW_OWNER_NAME,
  WORKFLOW_NAME,
  WORKFLOW_PUBLIC_WORKFLOW,
  WORKFLOW_DESCRIPTION,
  WORKFLOW_SIZE,
} from "./workflow-persist.service";
import { jsonCast } from "../../util/storage";
import { Workflow, WorkflowContent } from "../../type/workflow";
import { AppSettings } from "../../app-setting";
import { DashboardWorkflow } from "../../../dashboard/type/dashboard-workflow.interface";
import { SearchFilterParameters, toQueryStrings } from "../../../dashboard/type/search-filter-parameters";
import { NotificationService } from "../notification/notification.service";
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

  describe("uncovered persist/retrieve/duplicate/delete endpoints", () => {
    let notificationService: NotificationService;

    // content with no links -> checkIfWorkflowBroken returns false
    const validContent = { operators: [], operatorPositions: {}, links: [] } as unknown as WorkflowContent;
    // a link that references operators that do not exist -> checkIfWorkflowBroken returns true
    const brokenContent = {
      operators: [],
      operatorPositions: {},
      links: [{ source: { operatorID: "does-not-exist" }, target: { operatorID: "also-missing" } }],
    } as unknown as WorkflowContent;

    beforeEach(() => {
      notificationService = TestBed.inject(NotificationService);
    });

    afterEach(() => {
      httpTestingController.verify();
      vi.restoreAllMocks();
    });

    it("persistWorkflow POSTs the serialized body and parses the response content", () => {
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const workflow = {
        wid: 9,
        name: "my wf",
        description: "a description",
        content: validContent,
        isPublished: true,
      } as unknown as Workflow;

      let result: Workflow | undefined;
      service.persistWorkflow(workflow).subscribe(w => (result = w));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_PERSIST_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({
        wid: 9,
        name: "my wf",
        description: "a description",
        content: JSON.stringify(validContent),
        isPublic: true,
      });

      req.flush({ wid: 9, name: "my wf", content: '{"operators":[]}' });

      // valid workflow -> no error notification, and string content is parsed
      expect(errorSpy).not.toHaveBeenCalled();
      expect(result?.content).toEqual({ operators: [] });
    });

    it("persistWorkflow notifies the user when the workflow is broken but still POSTs", () => {
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      const workflow = {
        wid: 1,
        name: "broken",
        description: "",
        content: brokenContent,
        isPublished: false,
      } as unknown as Workflow;

      service.persistWorkflow(workflow).subscribe();

      expect(errorSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).toHaveBeenCalledWith(
        "Sorry! The workflow is broken and cannot be persisted. Please contact the system admin."
      );

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_PERSIST_URL}`);
      expect(req.request.body.isPublic).toBe(false);
      req.flush({ wid: 1, name: "broken", content: '{"operators":[]}' });
    });

    it("persistWorkflow filters out a null response so no value is emitted", () => {
      const workflow = {
        wid: 2,
        name: "n",
        description: "",
        content: validContent,
        isPublished: false,
      } as unknown as Workflow;

      let emitted = false;
      service.persistWorkflow(workflow).subscribe(() => (emitted = true));

      httpTestingController.expectOne(`${API}/${WORKFLOW_PERSIST_URL}`).flush(null);
      expect(emitted).toBe(false);
    });

    it("createWorkflow POSTs the name and serialized content and emits the created workflow", () => {
      const content = jsonCast<WorkflowContent>(testContent);

      let result: DashboardWorkflow | undefined;
      service.createWorkflow(content, "brand new").subscribe(r => (result = r));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_CREATE_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ name: "brand new", content: JSON.stringify(content) });

      const created = { workflow: { wid: 99, name: "brand new" } } as unknown as DashboardWorkflow;
      req.flush(created);
      expect(result).toEqual(created);
    });

    it("createWorkflow filters out a null response so no value is emitted", () => {
      let emitted = false;
      service.createWorkflow(jsonCast<WorkflowContent>(testContent)).subscribe(() => (emitted = true));

      httpTestingController.expectOne(`${API}/${WORKFLOW_CREATE_URL}`).flush(null);
      expect(emitted).toBe(false);
    });

    it("duplicateWorkflow POSTs only wids when no pid is provided", () => {
      let result: DashboardWorkflow[] | undefined;
      service.duplicateWorkflow([3, 4]).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_DUPLICATE_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ wids: [3, 4] });
      expect(req.request.body).not.toHaveProperty("pid");

      const dup = [{ workflow: { wid: 10 } }] as unknown as DashboardWorkflow[];
      req.flush(dup);
      expect(result).toEqual(dup);
    });

    it("duplicateWorkflow includes pid in the body when provided", () => {
      service.duplicateWorkflow([5], 42).subscribe();

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_DUPLICATE_URL}`);
      expect(req.request.body).toEqual({ wids: [5], pid: 42 });
      req.flush([{ workflow: { wid: 11 } }]);
    });

    it("duplicateWorkflow filters out an empty-array response", () => {
      let emitted = false;
      service.duplicateWorkflow([6]).subscribe(() => (emitted = true));

      httpTestingController.expectOne(`${API}/${WORKFLOW_DUPLICATE_URL}`).flush([]);
      expect(emitted).toBe(false);
    });

    it("retrieveWorkflowsBySessionUser GETs the list url and maps each entry", () => {
      const entry = { workflow: { wid: 1, name: "w", content: '{"operators":[]}' } } as unknown as DashboardWorkflow;

      let result: DashboardWorkflow[] | undefined;
      service.retrieveWorkflowsBySessionUser().subscribe(r => (result = r));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_LIST_URL}`);
      expect(req.request.method).toBe("GET");
      req.flush([entry]);

      expect(result?.length).toBe(1);
      expect(result?.[0]).toHaveProperty("dashboardWorkflowEntry");
      expect(result?.[0]?.workflow?.content).toEqual({ operators: [] });
    });

    it("deleteWorkflow POSTs the wids to the delete url", () => {
      let responded = false;
      service.deleteWorkflow([7, 8]).subscribe(() => (responded = true));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_DELETE_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ wids: [7, 8] });
      req.flush({});
      expect(responded).toBe(true);
    });

    it("updateWorkflowName POSTs wid and name on success", () => {
      let responded = false;
      service.updateWorkflowName(12, "renamed").subscribe(() => (responded = true));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_UPDATENAME_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ wid: 12, name: "renamed" });
      req.flush({});
      expect(responded).toBe(true);
    });

    it("updateWorkflowName notifies with the server message and rethrows on error", () => {
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      let caught: unknown;
      service.updateWorkflowName(13, "bad").subscribe({
        next: () => {},
        error: (e: unknown) => (caught = e),
      });

      httpTestingController
        .expectOne(`${API}/${WORKFLOW_UPDATENAME_URL}`)
        .flush({ message: "name already taken" }, { status: 400, statusText: "Bad Request" });

      expect(errorSpy).toHaveBeenCalledWith("name already taken");
      expect(caught).toBeTruthy();
    });

    it("updateWorkflowDescription POSTs wid and description on success", () => {
      let responded = false;
      service.updateWorkflowDescription(14, "new desc").subscribe(() => (responded = true));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_UPDATEDESCRIPTION_URL}`);
      expect(req.request.method).toBe("POST");
      expect(req.request.body).toEqual({ wid: 14, description: "new desc" });
      req.flush({});
      expect(responded).toBe(true);
    });

    it("updateWorkflowDescription notifies with the server message and rethrows on error", () => {
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});

      let caught: unknown;
      service.updateWorkflowDescription(15, "bad").subscribe({
        next: () => {},
        error: (e: unknown) => (caught = e),
      });

      httpTestingController
        .expectOne(`${API}/${WORKFLOW_UPDATEDESCRIPTION_URL}`)
        .flush({ message: "description too long" }, { status: 500, statusText: "Server Error" });

      expect(errorSpy).toHaveBeenCalledWith("description too long");
      expect(caught).toBeTruthy();
    });

    it("getWorkflowIsPublished GETs the type url as text", () => {
      let result: string | undefined;
      service.getWorkflowIsPublished(16).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_BASE_URL}/type/16`);
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");
      req.flush("true");
      expect(result).toBe("true");
    });

    it("updateWorkflowIsPublished PUTs to the public url when publishing", () => {
      service.updateWorkflowIsPublished(17, true).subscribe();

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_BASE_URL}/public/17`);
      expect(req.request.method).toBe("PUT");
      expect(req.request.body).toBeNull();
      req.flush(null);
    });

    it("updateWorkflowIsPublished PUTs to the private url when unpublishing", () => {
      service.updateWorkflowIsPublished(18, false).subscribe();

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_BASE_URL}/private/18`);
      expect(req.request.method).toBe("PUT");
      expect(req.request.body).toBeNull();
      req.flush(null);
    });

    it("setWorkflowPersistFlag toggles the value read by isWorkflowPersistEnabled", () => {
      // defaults to enabled
      expect(service.isWorkflowPersistEnabled()).toBe(true);

      service.setWorkflowPersistFlag(false);
      expect(service.isWorkflowPersistEnabled()).toBe(false);

      service.setWorkflowPersistFlag(true);
      expect(service.isWorkflowPersistEnabled()).toBe(true);
    });

    it("getOwnerName GETs the owner-name url with a wid param as text", () => {
      let result: string | undefined;
      service.getOwnerName(19).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(
        r => r.url === `${API}/${WORKFLOW_OWNER_NAME}` && r.params.get("wid") === "19"
      );
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");
      req.flush("alice");
      expect(result).toBe("alice");
    });

    it("getWorkflowName GETs the workflow-name url with a wid param as text", () => {
      let result: string | undefined;
      service.getWorkflowName(20).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(
        r => r.url === `${API}/${WORKFLOW_NAME}` && r.params.get("wid") === "20"
      );
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");
      req.flush("some workflow");
      expect(result).toBe("some workflow");
    });

    it("retrievePublicWorkflow GETs the publicised url and parses the content", () => {
      let result: Workflow | undefined;
      service.retrievePublicWorkflow(21).subscribe(w => (result = w));

      const req = httpTestingController.expectOne(`${API}/${WORKFLOW_PUBLIC_WORKFLOW}/21`);
      expect(req.request.method).toBe("GET");
      req.flush({ wid: 21, name: "pub", content: '{"operators":[]}' });

      expect(result?.content).toEqual({ operators: [] });
    });

    it("retrievePublicWorkflow filters out a null response", () => {
      let emitted = false;
      service.retrievePublicWorkflow(22).subscribe(() => (emitted = true));

      httpTestingController.expectOne(`${API}/${WORKFLOW_PUBLIC_WORKFLOW}/22`).flush(null);
      expect(emitted).toBe(false);
    });

    it("getWorkflowDescription GETs the description url with a wid param as text", () => {
      let result: string | undefined;
      service.getWorkflowDescription(23).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(
        r => r.url === `${API}/${WORKFLOW_DESCRIPTION}` && r.params.get("wid") === "23"
      );
      expect(req.request.method).toBe("GET");
      expect(req.request.responseType).toBe("text");
      req.flush("the description");
      expect(result).toBe("the description");
    });

    it("getSizes GETs the size url appending every wid as a separate param", () => {
      let result: Record<number, number> | undefined;
      service.getSizes([24, 25, 26]).subscribe(r => (result = r));

      const req = httpTestingController.expectOne(
        r => r.url === `${API}/${WORKFLOW_SIZE}` && r.params.getAll("wid")?.join(",") === "24,25,26"
      );
      expect(req.request.method).toBe("GET");
      const sizes = { 24: 100, 25: 200, 26: 300 };
      req.flush(sizes);
      expect(result).toEqual(sizes);
    });
  });
});
