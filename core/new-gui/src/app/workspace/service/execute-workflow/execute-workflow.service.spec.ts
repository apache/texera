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
import { environment } from "../../../../environments/environment";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowSnapshotService } from "../../../dashboard/user/service/workflow-snapshot/workflow-snapshot.service";

class StubHttpClient {
  constructor() {}

  public post(): Observable<string> {
    return of("a");
  }
}

/* eslint-disable @typescript-eslint/no-non-null-assertion */

describe("ExecuteWorkflowService", () => {
  let service: ExecuteWorkflowService;
  let mockWorkflowSnapshotService: WorkflowSnapshotService;

  beforeEach(() => {
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
    service.sendExecutionRequest("", logicalPlan);
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
});
