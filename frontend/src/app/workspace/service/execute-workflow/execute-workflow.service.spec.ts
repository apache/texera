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

import "zone.js/testing";

import { DOCUMENT } from "@angular/core";
import { ExecutionState, LogicalPlan } from "../../types/execute-workflow.interface";
import { fakeAsync, flush, inject, TestBed, tick } from "@angular/core/testing";

import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from "./execute-workflow.service";

import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../operator-metadata/stub-operator-metadata.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { of, Subject } from "rxjs";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";

import { mockLogicalPlan_scan_result, mockWorkflowPlan_scan_result } from "./mock-workflow-plan";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowSnapshotService } from "../../../dashboard/service/user/workflow-snapshot/workflow-snapshot.service";

import { WorkflowSettings } from "src/app/common/type/workflow";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { AuthService } from "src/app/common/service/user/auth.service";
import { StubAuthService } from "src/app/common/service/user/stub-auth.service";
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import {
  RegionStateEvent,
  RegionUpdateEvent,
  ReplayExecutionInfo,
  TexeraWebsocketEvent,
} from "../../types/workflow-websocket.interface";
import { mockScanPredicate } from "../workflow-graph/model/mock-workflow-data";
import { PAGINATION_INFO_STORAGE_KEY, ResultPaginationInfo } from "../../types/result-table.interface";
import { sessionGetObject, sessionSetObject } from "../../../common/util/storage";

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
      imports: [HttpClientTestingModule],
      providers: [
        ExecuteWorkflowService,
        WorkflowActionService,
        WorkflowUtilService,
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        UndoRedoService,
        JointUIService,
        {
          provide: OperatorMetadataService,
          useClass: StubOperatorMetadataService,
        },
        { provide: DOCUMENT, useValue: mockDocument },
        { provide: AuthService, useClass: StubAuthService },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    });

    service = TestBed.inject(ExecuteWorkflowService);
    mockWorkflowSnapshotService = TestBed.inject(WorkflowSnapshotService);
  });

  afterEach(() => {
    // sendExecutionRequest reads/writes sessionStorage; keep tests isolated
    sessionStorage.clear();
  });

  // Push an event through the stream the real WorkflowWebsocketService exposes, so the
  // constructor subscription in ExecuteWorkflowService runs exactly as it would in production.
  // websocketEvent() publicly returns the underlying Subject; casting to Subject for the test
  // avoids reaching into the service's private fields.
  const emitWsEvent = (event: TexeraWebsocketEvent): void => {
    (TestBed.inject(WorkflowWebsocketService).websocketEvent() as Subject<TexeraWebsocketEvent>).next(event);
  };

  it("should be created", inject([ExecuteWorkflowService], (injectedService: ExecuteWorkflowService) => {
    expect(injectedService).toBeTruthy();
  }));

  it("resetExecutionAndWorkers() clears the execution state and worker assignments", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    (service as any).assignedWorkerIds.set("op1", ["w1", "w2"]);

    const emittedStates: ExecutionState[] = [];
    service.getExecutionStateStream().subscribe(event => emittedStates.push(event.current.state));

    service.resetExecutionAndWorkers();

    expect(service.getExecutionState().state).toBe(ExecutionState.Uninitialized);
    expect(service.getWorkerIds("op1")).toEqual([]);
    // must broadcast on the stream so subscribers (menu, result panel) drop stale status
    expect(emittedStates).toContain(ExecutionState.Uninitialized);
  });

  it("should generate a logical plan request based on the workflow graph that is passed to the function", () => {
    const newLogicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    expect(newLogicalPlan).toEqual(mockLogicalPlan_scan_result);
  });

  it("should msg backend when executing workflow", fakeAsync(() => {
    const logicalPlan: LogicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(mockWorkflowPlan_scan_result);
    const wsSendSpy = vi.spyOn((service as any).workflowWebsocketService, "send");
    const settings = service["workflowActionService"].getWorkflowSettings();
    service.sendExecutionRequest("", logicalPlan, settings, false, undefined);
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

  it("should execute workflow with email notification successfully", () => {
    const executionName = "Test Execution";
    const emailNotificationEnabled = true;
    const targetOperatorId = "test-operator-id";

    const logicalPlanSpy = vi.spyOn(ExecuteWorkflowService, "getLogicalPlanRequest").mockReturnValue({} as LogicalPlan);
    const settingsSpy = vi
      .spyOn(service["workflowActionService"], "getWorkflowSettings")
      .mockReturnValue({} as WorkflowSettings);
    const resetExecutionStateSpy = vi.spyOn(service, "resetExecutionState");
    const resetStatusSpy = vi.spyOn(service["workflowStatusService"], "resetStatus");
    const sendExecutionRequestSpy = vi.spyOn(service, "sendExecutionRequest");

    service.executeWorkflowWithEmailNotification(executionName, emailNotificationEnabled, targetOperatorId);

    expect(logicalPlanSpy).toHaveBeenCalledWith(service["workflowActionService"].getTexeraGraph(), targetOperatorId);
    expect(settingsSpy).toHaveBeenCalled();
    expect(resetExecutionStateSpy).toHaveBeenCalled();
    expect(resetStatusSpy).toHaveBeenCalled();
    expect(sendExecutionRequestSpy).toHaveBeenCalledWith(
      executionName,
      expect.any(Object),
      expect.any(Object),
      emailNotificationEnabled
    );
  });

  it("should handle failure when executing workflow with email notification", () => {
    const executionName = "Test Execution";
    const emailNotificationEnabled = true;
    const targetOperatorId = "test-operator-id";

    const logicalPlanSpy = vi.spyOn(ExecuteWorkflowService, "getLogicalPlanRequest").mockImplementation(() => {
      throw "Logical plan error";
    });
    const resetExecutionStateSpy = vi.spyOn(service, "resetExecutionState");
    const resetStatusSpy = vi.spyOn(service["workflowStatusService"], "resetStatus");
    const sendExecutionRequestSpy = vi.spyOn(service, "sendExecutionRequest");

    expect(() => {
      service.executeWorkflowWithEmailNotification(executionName, emailNotificationEnabled, targetOperatorId);
    }).toThrowError("Logical plan error");

    expect(logicalPlanSpy).toHaveBeenCalledWith(service["workflowActionService"].getTexeraGraph(), targetOperatorId);
    expect(resetExecutionStateSpy).not.toHaveBeenCalled();
    expect(resetStatusSpy).not.toHaveBeenCalled();
    expect(sendExecutionRequestSpy).not.toHaveBeenCalled();
  });

  // ---- constructor: websocket-event routing --------------------------------------------------

  it("routes region and worker-assignment websocket events to their dedicated streams", () => {
    const regionUpdates: RegionUpdateEvent[] = [];
    const regionStates: RegionStateEvent[] = [];
    service.getRegionUpdateStream().subscribe(event => regionUpdates.push(event));
    service.getRegionStateStream().subscribe(event => regionStates.push(event));

    emitWsEvent({ type: "RegionUpdateEvent", regions: [] });
    emitWsEvent({ type: "RegionStateEvent", id: 7, state: "COMPLETED" });
    emitWsEvent({ type: "WorkerAssignmentUpdateEvent", operatorId: "opX", workerIds: ["w1", "w2"] });

    expect(regionUpdates).toEqual([{ type: "RegionUpdateEvent", regions: [] }]);
    expect(regionStates).toEqual([{ type: "RegionStateEvent", id: 7, state: "COMPLETED" }]);
    expect(service.getWorkerIds("opX")).toEqual(["w1", "w2"]);
  });

  it("routes execution-status websocket events into the execution-state stream", () => {
    const states: ExecutionState[] = [];
    service.getExecutionStateStream().subscribe(event => states.push(event.current.state));

    emitWsEvent({ type: "WorkflowStateEvent", state: ExecutionState.Running });

    expect(service.getExecutionState().state).toBe(ExecutionState.Running);
    expect(states).toEqual([ExecutionState.Running]);
  });

  it("does not re-emit when a websocket event maps to the unchanged execution state", () => {
    const states: ExecutionState[] = [];
    service.getExecutionStateStream().subscribe(event => states.push(event.current.state));

    emitWsEvent({ type: "WorkflowStateEvent", state: ExecutionState.Running });
    emitWsEvent({ type: "WorkflowStateEvent", state: ExecutionState.Running });

    // second identical event is deduped by updateExecutionState's isEqual guard
    expect(states).toEqual([ExecutionState.Running]);
  });

  // ---- handleReconfigurationEvent ------------------------------------------------------------

  it("handleReconfigurationEvent shows an error notification for an invalid ModifyLogicResponse", () => {
    const errorSpy = vi.spyOn(service["notificationService"], "error").mockImplementation(() => undefined as any);
    service.handleReconfigurationEvent({
      type: "ModifyLogicResponse",
      opId: "1",
      isValid: false,
      errorMessage: "boom",
    });
    expect(errorSpy).toHaveBeenCalledWith("boom");
  });

  it("handleReconfigurationEvent confirms a valid ModifyLogicResponse", () => {
    const infoSpy = vi.spyOn(service["notificationService"], "info").mockImplementation(() => undefined as any);
    service.handleReconfigurationEvent({ type: "ModifyLogicResponse", opId: "1", isValid: true, errorMessage: "" });
    expect(infoSpy).toHaveBeenCalledWith("reconfiguration registered");
  });

  it("handleReconfigurationEvent announces a ModifyLogicCompletedEvent with the operator ids", () => {
    const infoSpy = vi.spyOn(service["notificationService"], "info").mockImplementation(() => undefined as any);
    service.handleReconfigurationEvent({ type: "ModifyLogicCompletedEvent", opIds: ["a", "b"] });
    expect(infoSpy).toHaveBeenCalledWith("reconfiguration on operator(s) a,b complete");
  });

  // ---- handleExecutionEvent ------------------------------------------------------------------

  it("handleExecutionEvent enters Paused with empty tuples from a non-paused state", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    const result = service.handleExecutionEvent({ type: "WorkflowStateEvent", state: ExecutionState.Paused });
    expect(result).toEqual({ state: ExecutionState.Paused, currentTuples: {} });
  });

  it("handleExecutionEvent keeps the current state when already Paused", () => {
    const paused = { state: ExecutionState.Paused, currentTuples: { a: { operatorID: "a", tuples: [] } } };
    (service as any).currentState = paused;
    const result = service.handleExecutionEvent({ type: "WorkflowStateEvent", state: ExecutionState.Paused });
    expect(result).toBe(paused);
  });

  it("handleExecutionEvent defers the Failed state to the follow-up error event", () => {
    const result = service.handleExecutionEvent({ type: "WorkflowStateEvent", state: ExecutionState.Failed });
    expect(result).toBeUndefined();
  });

  it("handleExecutionEvent passes through other workflow states", () => {
    const result = service.handleExecutionEvent({ type: "WorkflowStateEvent", state: ExecutionState.Running });
    expect(result).toEqual({ state: ExecutionState.Running });
  });

  it("handleExecutionEvent maps RecoveryStartedEvent to the Recovering state", () => {
    const result = service.handleExecutionEvent({ type: "RecoveryStartedEvent" });
    expect(result).toEqual({ state: ExecutionState.Recovering });
  });

  it("handleExecutionEvent merges current tuples on top of the existing paused tuples", () => {
    const existing = { opA: { operatorID: "opA", tuples: [] } };
    (service as any).currentState = { state: ExecutionState.Paused, currentTuples: existing };
    const event: TexeraWebsocketEvent = { type: "OperatorCurrentTuplesUpdateEvent", operatorID: "opB", tuples: [] };
    const result = service.handleExecutionEvent(event);
    expect(result).toEqual({
      state: ExecutionState.Paused,
      currentTuples: { opB: event, opA: existing.opA },
    });
  });

  it("handleExecutionEvent starts a fresh paused-tuples map when not already paused", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    const event: TexeraWebsocketEvent = { type: "OperatorCurrentTuplesUpdateEvent", operatorID: "opB", tuples: [] };
    const result = service.handleExecutionEvent(event);
    expect(result).toEqual({ state: ExecutionState.Paused, currentTuples: { opB: event } });
  });

  it("handleExecutionEvent maps WorkflowErrorEvent to Failed and rewrites newline escapes", () => {
    const fatalError = {
      message: "line1\\nline2",
      details: "stack",
      operatorId: "op1",
      workerId: "w1",
      type: { name: "RuntimeError" },
      timestamp: { nanos: 1, seconds: 2 },
    };
    const result = service.handleExecutionEvent({ type: "WorkflowErrorEvent", fatalErrors: [fatalError] });
    expect(result).toEqual({
      state: ExecutionState.Failed,
      errorMessages: [{ ...fatalError, message: "line1<br>line2" }],
    });
  });

  it("handleExecutionEvent ignores unrelated events", () => {
    expect(service.handleExecutionEvent({ type: "HeartBeatResponse" })).toBeUndefined();
  });

  // ---- getErrorMessages ----------------------------------------------------------------------

  it("getErrorMessages returns the fatal errors while the execution is Failed", () => {
    const fatalError = {
      message: "boom",
      details: "stack",
      operatorId: "op1",
      workerId: "w1",
      type: { name: "RuntimeError" },
      timestamp: { nanos: 1, seconds: 2 },
    };
    (service as any).currentState = { state: ExecutionState.Failed, errorMessages: [fatalError] };
    expect(service.getErrorMessages()).toEqual([fatalError]);
  });

  // ---- execution entry points ----------------------------------------------------------------

  it("executeWorkflow delegates to executeWorkflowWithEmailNotification with email disabled", () => {
    const delegateSpy = vi.spyOn(service, "executeWorkflowWithEmailNotification").mockImplementation(() => {});
    service.executeWorkflow("run-1", "op-9");
    expect(delegateSpy).toHaveBeenCalledWith("run-1", false, "op-9");
  });

  it("executeWorkflowWithReplay builds a replay request and forwards the replay info", () => {
    const logicalPlanSpy = vi.spyOn(ExecuteWorkflowService, "getLogicalPlanRequest").mockReturnValue({} as LogicalPlan);
    const resetExecutionStateSpy = vi.spyOn(service, "resetExecutionState");
    const resetStatusSpy = vi.spyOn(service["workflowStatusService"], "resetStatus");
    const sendExecutionRequestSpy = vi.spyOn(service, "sendExecutionRequest").mockImplementation(() => {});
    const replayInfo: ReplayExecutionInfo = { eid: 42, interaction: "step-3" };

    service.executeWorkflowWithReplay(replayInfo);

    expect(logicalPlanSpy).toHaveBeenCalledWith(service["workflowActionService"].getTexeraGraph());
    expect(resetExecutionStateSpy).toHaveBeenCalled();
    expect(resetStatusSpy).toHaveBeenCalled();
    expect(sendExecutionRequestSpy).toHaveBeenCalledWith(
      "Replay run of 42 to step-3",
      expect.any(Object),
      expect.any(Object),
      false,
      replayInfo
    );
  });

  // ---- sendExecutionRequest branches ---------------------------------------------------------

  it("sendExecutionRequest includes the selected computing unit id in the request", fakeAsync(() => {
    vi.spyOn(service["computingUnitStatusService"], "getSelectedComputingUnitValue").mockReturnValue({
      computingUnit: { cuid: 99 },
    } as any);
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    const settings = service["workflowActionService"].getWorkflowSettings();

    service.sendExecutionRequest("exec", {} as LogicalPlan, settings, true, undefined);
    tick(FORM_DEBOUNCE_TIME_MS + 1);
    flush();

    expect(wsSendSpy).toHaveBeenCalledWith(
      "WorkflowExecuteRequest",
      expect.objectContaining({ computingUnitId: 99, emailNotificationEnabled: true, executionName: "exec" })
    );
  }));

  it("sendExecutionRequest flags stored pagination info as belonging to a new execution", fakeAsync(() => {
    sessionSetObject(PAGINATION_INFO_STORAGE_KEY, { newWorkflowExecuted: false });
    const settings = service["workflowActionService"].getWorkflowSettings();

    service.sendExecutionRequest("exec", {} as LogicalPlan, settings, false, undefined);
    tick(FORM_DEBOUNCE_TIME_MS + 1);
    flush();

    const stored = sessionGetObject<ResultPaginationInfo>(PAGINATION_INFO_STORAGE_KEY);
    expect(stored?.newWorkflowExecuted).toBe(true);
  }));

  // ---- control commands ----------------------------------------------------------------------

  it("pauseWorkflow sends a pause request while running", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.pauseWorkflow();
    expect(wsSendSpy).toHaveBeenCalledWith("WorkflowPauseRequest", {});
  });

  it("killWorkflow throws in a terminal state and sends a kill request otherwise", () => {
    (service as any).currentState = { state: ExecutionState.Uninitialized };
    expect(() => service.killWorkflow()).toThrowError(
      "cannot kill workflow, the current execution state is Uninitialized"
    );

    (service as any).currentState = { state: ExecutionState.Running };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.killWorkflow();
    expect(wsSendSpy).toHaveBeenCalledWith("WorkflowKillRequest", {});
  });

  it("takeGlobalCheckpoint throws when completed and sends a checkpoint request otherwise", () => {
    (service as any).currentState = { state: ExecutionState.Completed };
    expect(() => service.takeGlobalCheckpoint()).toThrowError(
      "cannot take checkpoint, the current execution state is Completed"
    );

    (service as any).currentState = { state: ExecutionState.Running };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.takeGlobalCheckpoint();
    expect(wsSendSpy).toHaveBeenCalledWith("WorkflowCheckpointRequest", {});
  });

  it("resumeWorkflow sends a resume request while paused", () => {
    (service as any).currentState = { state: ExecutionState.Paused };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.resumeWorkflow();
    expect(wsSendSpy).toHaveBeenCalledWith("WorkflowResumeRequest", {});
  });

  it("skipTuples requires a paused state and forwards the workers", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    expect(() => service.skipTuples(["w1"])).toThrowError("cannot skip tuples, the current execution state is Running");

    (service as any).currentState = { state: ExecutionState.Paused };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.skipTuples(["w1", "w2"]);
    expect(wsSendSpy).toHaveBeenCalledWith("SkipTupleRequest", { workers: ["w1", "w2"] });
  });

  it("retryExecution requires a paused state and forwards the workers", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    expect(() => service.retryExecution(["w1"])).toThrowError(
      "cannot retry the current tuple, the current execution state is Running"
    );

    (service as any).currentState = { state: ExecutionState.Paused };
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.retryExecution(["w9"]);
    expect(wsSendSpy).toHaveBeenCalledWith("RetryRequest", { workers: ["w9"] });
  });

  it("modifyOperatorLogic requires a paused state and sends the operator logic", () => {
    (service as any).currentState = { state: ExecutionState.Running };
    expect(() => service.modifyOperatorLogic("1")).toThrowError(
      "cannot modify logic, the current execution state is Running"
    );

    (service as any).currentState = { state: ExecutionState.Paused };
    vi.spyOn(service["workflowActionService"].getTexeraGraph(), "getOperator").mockReturnValue(mockScanPredicate);
    const wsSendSpy = vi.spyOn(service["workflowWebsocketService"], "send");
    service.modifyOperatorLogic("1");
    expect(wsSendSpy).toHaveBeenCalledWith("ModifyLogicRequest", {
      operator: { operatorID: "1", operatorType: "ScanSource" },
    });
  });
});
