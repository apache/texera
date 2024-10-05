import { ExecutionState, LogicalPlan } from "../../types/execute-workflow.interface";
import { fakeAsync, flush, inject, TestBed, tick } from "@angular/core/testing";

import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from "./execute-workflow.service";

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { Observable, of } from "rxjs";

import { mockLogicalPlan_scan_result, mockWorkflowPlan_scan_result } from "./mock-workflow-plan";
import { HttpClient } from "@angular/common/http";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowSnapshotService } from "../../../dashboard/service/user/workflow-snapshot/workflow-snapshot.service";
import { DOCUMENT } from "@angular/common";

class StubHttpClient {
  public post(): Observable<string> {
    return of("a");
  }
}

describe("ExecuteWorkflowService", () => {
  let service: ExecuteWorkflowService;
  let mockWorkflowSnapshotService: WorkflowSnapshotService;
  let mockDocument: Document;

  beforeEach(() => {
    mockDocument = {
      location: {
        origin: "https://texera.example.com",
      },
    } as Document;

    TestBed.configureTestingModule({
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        WorkflowUtilService,
        UndoRedoService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: HttpClient, useClass: StubHttpClient },
        { provide: DOCUMENT, useValue: mockDocument },
      ],
    });

    service = TestBed.inject(ExecuteWorkflowService);
    mockWorkflowSnapshotService = TestBed.inject(WorkflowSnapshotService);
  });

  it("should be created", inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("should generate a logical plan request based on the workflow graph that is passed to the function", () => {
    const newLogicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    expect(newLogicalPlan).toEqual(mockLogicalPlan_scan_result);
  });

  it("should msg backend when executing workflow", fakeAsync(() => {
    const logicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    const wsSendSpy = spyOn((service as any).workflowWebsocketService, "send");
    const settings = service["workflowActionService"].getWorkflowSettings();
    service.sendExecutionRequest("", logicalPlan, settings);
    tick(FORM_DEBOUNCE_TIME_MS + 1);
    flush();
    expect(wsSendSpy).toHaveBeenCalledTimes(1);
  }));

  it("it should raise an error when pauseWorkflow() is called without an execution state", () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.pauseWorkflow();
    }).toThrowError(
      new RegExp("cannot pause workflow, the current execution state is " + (service as any).currentState.state)
    );
  });

  it("it should raise an error when resumeWorkflow() is called without an execution state", () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(function () {
      service.resumeWorkflow();
    }).toThrowError(
      new RegExp("cannot resume workflow, the current execution state is " + (service as any).currentState.state)
    );
  });

  it("should generate correct email notification content for successful execution", () => {
    const workflow = { wid: "123", name: "Test Workflow" };
    const stateInfo = {
      state: ExecutionState.Completed,
      startTime: new Date("2023-04-01T10:00:00Z"),
      endTime: new Date("2023-04-01T10:30:00Z"),
    };

    const result = (service as any).getEmailNotificationContent(workflow, stateInfo);

    expect(result.subject).toBe("[Texera] Workflow Test Workflow (123) Status: Completed");
    expect(result.content).toContain("Your workflow Test Workflow (123) has finished execution.");
    expect(result.content).toContain("Status: Completed");
    expect(result.content).toContain("Start time: 2023-04-01 10:00:00 (UTC)");
    expect(result.content).toContain("End time: 2023-04-01 10:30:00 (UTC)");
    expect(result.content).toContain("https://texera.example.com/dashboard/user/workspace/123");
  });

  it("should generate correct email notification content for failed execution", () => {
    const workflow = { wid: "456", name: "Failed Workflow" };
    const stateInfo = {
      state: ExecutionState.Failed,
      startTime: new Date("2023-04-02T14:00:00Z"),
      endTime: new Date("2023-04-02T14:15:00Z"),
    };

    const result = (service as any).getEmailNotificationContent(workflow, stateInfo);

    expect(result.subject).toBe("[Texera] Workflow Failed Workflow (456) Status: Failed");
    expect(result.content).toContain("Your workflow Failed Workflow (456) has finished execution.");
    expect(result.content).toContain("Status: Failed");
    expect(result.content).toContain("Start time: 2023-04-02 14:00:00 (UTC)");
    expect(result.content).toContain("End time: 2023-04-02 14:15:00 (UTC)");
    expect(result.content).toContain("https://texera.example.com/dashboard/user/workspace/456");
  });
});
