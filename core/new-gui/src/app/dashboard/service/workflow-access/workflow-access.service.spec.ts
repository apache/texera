import { TestBed } from "@angular/core/testing";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { jsonCast } from "../../../common/util/storage";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import {
  WORKFLOW_ACCESS_GRANT_URL,
  WORKFLOW_ACCESS_LIST_URL,
  WORKFLOW_ACCESS_REVOKE_URL,
  WorkflowAccessService,
} from "./workflow-access.service";
import { first } from "rxjs/operators";

describe("WorkflowAccessService", () => {
  const TestWorkflow: Workflow = {
    wid: 28,
    name: "project 1",
    description: "dummy description",
    content: jsonCast<WorkflowContent>(
      " {\"operators\":[],\"operatorPositions\":{},\"links\":[],\"groups\":[],\"breakpoints\":{}}"
    ),
    creationTime: 1,
    lastModifiedTime: 2,
  };

  const username = "Jim";
  const accessType = "read";

  let service: WorkflowAccessService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowAccessService],
      imports: [HttpClientTestingModule],
    });
    service = TestBed.get(WorkflowAccessService);
    httpMock = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
  it("grantUserWorkflowAccess works as expected", () => {
    service.grantUserWorkflowAccess(TestWorkflow, username, accessType).pipe(first()).subscribe();
    console.log(httpMock);
    const req = httpMock.expectOne(`${WORKFLOW_ACCESS_GRANT_URL}/${TestWorkflow.wid}/${username}/${accessType}`);
    console.log(req.request);
    expect(req.request.method).toEqual("POST");
    req.flush({ code: 0, message: "" });
  });

  it("retrieveGrantedWorkflowAccessList works as expected", () => {
    service.retrieveGrantedWorkflowAccessList(TestWorkflow).pipe(first()).subscribe();
    const req = httpMock.expectOne(`${WORKFLOW_ACCESS_LIST_URL}/${TestWorkflow.wid}`);
    expect(req.request.method).toEqual("GET");
    req.flush({ code: 0, message: "" });
  });

  it("revokeWorkflowAccess works as expected", () => {
    service.revokeWorkflowAccess(TestWorkflow, username).pipe(first()).subscribe();
    const req = httpMock.expectOne(`${WORKFLOW_ACCESS_REVOKE_URL}/${TestWorkflow.wid}/${username}`);
    expect(req.request.method).toEqual("DELETE");
    req.flush({ code: 0, message: "" });
  });
});
