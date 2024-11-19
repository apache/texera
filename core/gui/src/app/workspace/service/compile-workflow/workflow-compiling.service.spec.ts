import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { TestBed, fakeAsync, tick, discardPeriodicTasks } from "@angular/core/testing";
import { environment } from "../../../../environments/environment";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import {
  WorkflowCompilingService,
  WORKFLOW_COMPILATION_ENDPOINT,
  WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS,
} from "./workflow-compiling.service";
import { AppSettings } from "../../../common/app-setting";
import {
  mockPoint,
  mockScanPredicate,
  mockScanSentimentLink,
  mockSentimentPredicate,
} from "../workflow-graph/model/mock-workflow-data";

describe("WorkflowCompilingService", () => {
  let httpTestingController: HttpTestingController;
  let workflowCompilingService: WorkflowCompilingService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [WorkflowCompilingService, WorkflowActionService, DynamicSchemaService, ExecuteWorkflowService],
    });

    httpTestingController = TestBed.inject(HttpTestingController);
    workflowCompilingService = TestBed.inject(WorkflowCompilingService);
    environment.schemaPropagationEnabled = true;
  });

  it("should be created", () => {
    expect(workflowCompilingService).toBeTruthy();
  });

  it("should invoke compilation API on workflow topology changes", fakeAsync(() => {
    const workflowActionService = TestBed.inject(WorkflowActionService);

    workflowActionService.addOperator(mockScanPredicate, mockPoint);
    workflowActionService.addOperator(mockSentimentPredicate, mockPoint);

    // add link
    workflowActionService.addLink(mockScanSentimentLink);

    tick(WORKFLOW_COMPILATION_DEBOUNCE_TIME_MS);

    const req = httpTestingController.expectOne(`${AppSettings.getApiEndpoint()}/${WORKFLOW_COMPILATION_ENDPOINT}`);
    expect(req.request.method).toEqual("POST");
    httpTestingController.verify();
    discardPeriodicTasks();
  }));
});
