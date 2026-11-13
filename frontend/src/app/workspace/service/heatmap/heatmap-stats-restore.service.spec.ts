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
import { Subject, of, throwError } from "rxjs";
import type { Mocked } from "vitest";
import { HeatmapStatsRestoreService } from "./heatmap-stats-restore.service";
import { savePersistedHeatmapView } from "./heatmap-overlay-persistence";
import { HeatmapView } from "./heatmap-scoring";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { WorkflowRuntimeStatistics } from "../../../dashboard/type/workflow-runtime-statistics";

function makeExecution(overrides: Partial<WorkflowExecutionsEntry>): WorkflowExecutionsEntry {
  return {
    eId: 1,
    vId: 1,
    cuId: 7,
    whId: null,
    sId: 0,
    userName: "user",
    avatar: "",
    name: "run",
    startingTime: 1_000,
    completionTime: 2_000,
    status: 3, // Completed
    result: "",
    bookmarked: false,
    logLocation: "",
    ...overrides,
  };
}

function makeStatsRow(overrides: Partial<WorkflowRuntimeStatistics>): WorkflowRuntimeStatistics {
  return {
    operatorId: "op1",
    timestamp: 1_000,
    inputTupleCount: 10,
    inputTupleSize: 100,
    outputTupleCount: 5,
    outputTupleSize: 50,
    totalDataProcessingTime: 1_000_000,
    totalControlProcessingTime: 2_000,
    totalIdleTime: 3_000,
    numberOfWorkers: 2,
    status: 3,
    ...overrides,
  };
}

describe("HeatmapStatsRestoreService", () => {
  let service: HeatmapStatsRestoreService;
  let executionsService: Mocked<WorkflowExecutionsService>;
  let statusService: Mocked<WorkflowStatusService>;
  let actionService: Mocked<WorkflowActionService>;
  let executeService: Mocked<ExecuteWorkflowService>;
  let statisticsUpdates: Subject<Record<string, never>>;

  beforeEach(() => {
    localStorage.clear();
    // Default arrangement: overlay persisted on, a saved workflow, no live run.
    savePersistedHeatmapView(HeatmapView.Runtime);

    executionsService = {
      retrieveWorkflowExecutions: vi.fn(() => of([makeExecution({})])),
      retrieveWorkflowRuntimeStatistics: vi.fn(() => of([makeStatsRow({})])),
    } as unknown as Mocked<WorkflowExecutionsService>;
    actionService = {
      getWorkflowMetadata: vi.fn(() => ({ wid: 42 })),
    } as unknown as Mocked<WorkflowActionService>;
    // A plain Subject, like the real one: subscribing does not emit, so it only cuts the
    // restore short when another producer actually writes statistics.
    statisticsUpdates = new Subject<Record<string, never>>();
    statusService = {
      setExternalStatus: vi.fn(),
      getStatisticsUpdateStream: vi.fn(() => statisticsUpdates.asObservable()),
    } as unknown as Mocked<WorkflowStatusService>;
    executeService = {
      getExecutionState: vi.fn(() => ({ state: ExecutionState.Uninitialized })),
    } as unknown as Mocked<ExecuteWorkflowService>;

    TestBed.configureTestingModule({
      providers: [
        HeatmapStatsRestoreService,
        { provide: WorkflowExecutionsService, useValue: executionsService },
        { provide: WorkflowActionService, useValue: actionService },
        { provide: WorkflowStatusService, useValue: statusService },
        { provide: ExecuteWorkflowService, useValue: executeService },
      ],
    });
    service = TestBed.inject(HeatmapStatsRestoreService);
  });

  afterEach(() => localStorage.clear());

  it("fetches the latest run and feeds the mapped statistics into WorkflowStatusService", () => {
    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowExecutions).toHaveBeenCalledWith(42);
    expect(executionsService.retrieveWorkflowRuntimeStatistics).toHaveBeenCalledWith(42, 1, 7);
    expect(statusService.setExternalStatus).toHaveBeenCalledWith({
      op1: expect.objectContaining({
        operatorState: OperatorState.Completed,
        aggregatedInputRowCount: 10,
        aggregatedOutputRowCount: 5,
        aggregatedDataProcessingTime: 1_000_000,
        numWorkers: 2,
      }),
    });
  });

  it("does nothing when the overlay is not persisted on", () => {
    savePersistedHeatmapView(null);

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowExecutions).not.toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("does nothing for an unsaved workflow (no wid)", () => {
    actionService.getWorkflowMetadata.mockReturnValue({ wid: undefined } as never);

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowExecutions).not.toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("skips the restore while an execution is in progress, so the live stream wins", () => {
    executeService.getExecutionState.mockReturnValue({ state: ExecutionState.Running } as never);

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowExecutions).not.toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("is cold: reads no gate and issues no request until subscribed", () => {
    const pending = service.restoreLatestRunStatistics();

    expect(executeService.getExecutionState).not.toHaveBeenCalled();
    expect(executionsService.retrieveWorkflowExecutions).not.toHaveBeenCalled();

    pending.subscribe();

    expect(executionsService.retrieveWorkflowExecutions).toHaveBeenCalledWith(42);
  });

  it("evaluates the gates at subscribe time, not at call time", () => {
    const pending = service.restoreLatestRunStatistics();
    savePersistedHeatmapView(null);

    pending.subscribe();

    expect(executionsService.retrieveWorkflowExecutions).not.toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("drops the restore when a run starts while the fetches are in flight", () => {
    // The entry guard passes on Uninitialized, which is also the state before the websocket
    // connects; a live run landing mid-flight must not be overwritten by the previous run.
    executionsService.retrieveWorkflowRuntimeStatistics.mockImplementation(() => {
      executeService.getExecutionState.mockReturnValue({ state: ExecutionState.Running } as never);
      return of([makeStatsRow({})]);
    });

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowRuntimeStatistics).toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("neither ingests nor throws when the workflow has no executions", () => {
    executionsService.retrieveWorkflowExecutions.mockReturnValue(of([]));

    expect(() => service.restoreLatestRunStatistics().subscribe()).not.toThrow();

    expect(executionsService.retrieveWorkflowRuntimeStatistics).not.toHaveBeenCalled();
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("prefers the most recent completed run over newer unfinished ones", () => {
    executionsService.retrieveWorkflowExecutions.mockReturnValue(
      of([
        makeExecution({ eId: 3, cuId: 9, startingTime: 3_000, status: 4 }), // newest, Failed
        makeExecution({ eId: 2, cuId: 8, startingTime: 2_000, status: 3 }), // newest Completed
        makeExecution({ eId: 1, cuId: 7, startingTime: 1_000, status: 3 }),
      ])
    );

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowRuntimeStatistics).toHaveBeenCalledWith(42, 2, 8);
  });

  it("still loads the latest run when no execution ever completed", () => {
    executionsService.retrieveWorkflowExecutions.mockReturnValue(
      of([
        makeExecution({ eId: 5, cuId: 11, startingTime: 5_000, status: 1 }), // Running
        makeExecution({ eId: 4, cuId: 10, startingTime: 4_000, status: 4 }), // Failed
      ])
    );

    service.restoreLatestRunStatistics().subscribe();

    expect(executionsService.retrieveWorkflowRuntimeStatistics).toHaveBeenCalledWith(42, 5, 11);
  });

  // The error path is pinned via an explicit { error, complete } observer. RxJS 7 reports an
  // unhandled error asynchronously instead of rethrowing from subscribe(), so a not.toThrow()
  // assertion would pass with catchError deleted.
  it("swallows an HTTP error from the executions fetch", () => {
    executionsService.retrieveWorkflowExecutions.mockReturnValue(throwError(() => new Error("500")));
    const onError = vi.fn();
    const onComplete = vi.fn();

    service.restoreLatestRunStatistics().subscribe({ error: onError, complete: onComplete });

    expect(onError).not.toHaveBeenCalled();
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("swallows an HTTP error from the statistics fetch", () => {
    executionsService.retrieveWorkflowRuntimeStatistics.mockReturnValue(throwError(() => new Error("500")));
    const onError = vi.fn();
    const onComplete = vi.fn();

    service.restoreLatestRunStatistics().subscribe({ error: onError, complete: onComplete });

    expect(onError).not.toHaveBeenCalled();
    expect(onComplete).toHaveBeenCalledTimes(1);
    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("drops the restore when Run clears the canvas before the fetches return", () => {
    // Run calls resetExecutionState() before the backend answers, so the execution state is
    // briefly Uninitialized and isExecuting() reads false. resetStatus() writes the cleared
    // statistics first, and that write is what the restore has to yield to.
    executionsService.retrieveWorkflowRuntimeStatistics.mockImplementation(() => {
      statisticsUpdates.next({});
      return of([makeStatsRow({})]);
    });

    service.restoreLatestRunStatistics().subscribe();

    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });

  it("still restores when nothing else writes statistics first", () => {
    service.restoreLatestRunStatistics().subscribe();

    expect(statusService.setExternalStatus).toHaveBeenCalledTimes(1);
  });

  it("surfaces an ingestion failure instead of swallowing it as a skipped restore", () => {
    statusService.setExternalStatus.mockImplementation(() => {
      throw new Error("ingest failed");
    });
    const onError = vi.fn();

    service.restoreLatestRunStatistics().subscribe({ error: onError });

    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: "ingest failed" }));
  });

  it("does not ingest an empty statistics payload", () => {
    executionsService.retrieveWorkflowRuntimeStatistics.mockReturnValue(of([]));

    service.restoreLatestRunStatistics().subscribe();

    expect(statusService.setExternalStatus).not.toHaveBeenCalled();
  });
});
