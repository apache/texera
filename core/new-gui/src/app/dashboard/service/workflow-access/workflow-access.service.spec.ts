import { TestBed } from "@angular/core/testing";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { jsonCast } from "../../../common/util/storage";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { WorkflowAccessService } from "./workflow-access.service";

describe("WorkflowAccessService", () => {
  const TestWorkflow: Workflow = {
    wid: 28,
    name: "project 1",
    description: "dummy description",
    content: jsonCast<WorkflowContent>(
      ' {"operators":[],"operatorPositions":{},"links":[],"groups":[],"breakpoints":{}}'
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
});
