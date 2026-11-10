/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import { buildLogicalPlan } from "../../core/workspace/service/execute-workflow/logical-plan";
import {
  ExecutionState,
  type OperatorRuntimeStatus,
  type WorkflowResultUpdate,
  type WorkflowResultTableStats,
} from "../../core/workspace/types/execute-workflow.interface";
import type { WorkflowCompilationResponse } from "../../core/workspace/types/workflow-compiling.interface";
import type { ConsoleMessage } from "../../core/workspace/types/workflow-common.interface";
import type {
  OperatorCurrentTuples,
  ReplayExecutionInfo,
  TexeraWebsocketEvent,
  WorkflowFatalError,
} from "../../core/workspace/types/workflow-websocket.interface";
import { WorkflowSession } from "./workflow-session";
import { WorkflowSocket, type ConnectionSnapshot } from "./workflow-socket";

export interface RuntimeSnapshot {
  state: ExecutionState;
  duration: number;
  errors: readonly WorkflowFatalError[];
  console: Readonly<Record<string, readonly ConsoleMessage[]>>;
  operators: Readonly<Record<string, OperatorRuntimeStatus>>;
  currentTuples: Readonly<Record<string, OperatorCurrentTuples>>;
  results: WorkflowResultUpdate;
  materializedResults: Readonly<Record<string, readonly object[]>>;
  workers: Readonly<Record<string, readonly string[]>>;
  regions: readonly [number, string[]][];
  regionStates: Readonly<Record<number, string>>;
  tableStats: WorkflowResultTableStats;
  connection: ConnectionSnapshot;
  compilation?: WorkflowCompilationResponse;
  compiling: boolean;
  error?: string;
}
export interface RuntimeOptions {
  engineVersion: string;
  config: Readonly<GuiConfig>;
  getComputingUnit: () => { cuid: number; status: string; isOwner?: boolean; accessPrivilege?: string } | undefined;
  getWarehouseId: () => number | undefined;
}

/** Execution and compilation outlive either workspace view, just like the shared graph. */
export class WorkflowRuntime {
  private snapshot: RuntimeSnapshot;
  private readonly listeners = new Set<() => void>();
  private readonly cleanups: (() => void)[];
  private durationAnchor = { reported: 0, at: 0, running: false };
  private ticker?: ReturnType<typeof setInterval>;
  private runTimer?: ReturnType<typeof setTimeout>;
  private rejectRun?: (error: Error) => void;
  private compileTimer?: ReturnType<typeof setTimeout>;
  private compileController?: AbortController;
  private compileRevision = 0;
  private planKey: string;
  private disposed = false;

  constructor(
    readonly session: WorkflowSession,
    readonly socket: WorkflowSocket,
    private readonly api: ApiClient,
    private readonly options: RuntimeOptions
  ) {
    this.snapshot = {
      state: ExecutionState.Uninitialized,
      duration: 0,
      errors: [],
      console: {},
      operators: {},
      currentTuples: {},
      results: {},
      materializedResults: {},
      workers: {},
      regions: [],
      regionStates: {},
      tableStats: {},
      connection: socket.getSnapshot(),
      compiling: false,
    };
    this.planKey = JSON.stringify(buildLogicalPlan(session.graph));
    this.cleanups = [
      socket.subscribeEvents(event => this.receive(event)),
      socket.subscribe(() => this.update({ connection: socket.getSnapshot() })),
      session.subscribe(() => {
        const key = JSON.stringify(buildLogicalPlan(session.graph));
        if (key === this.planKey) return;
        this.planKey = key;
        if (this.compileTimer) clearTimeout(this.compileTimer);
        this.compileTimer = setTimeout(() => {
          void this.compile().catch(() => undefined);
        }, 500);
      }),
    ];
  }
  getSnapshot = (): RuntimeSnapshot => this.snapshot;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  canControl(): boolean {
    const unit = this.options.getComputingUnit();
    return !!unit && (unit.isOwner === true || !["READ", "NONE"].includes(unit.accessPrivilege ?? ""));
  }
  async compile(): Promise<WorkflowCompilationResponse> {
    const revision = ++this.compileRevision;
    this.compileController?.abort();
    const controller = new AbortController();
    this.compileController = controller;
    const plan = buildLogicalPlan(this.session.graph);
    this.update({ compiling: true, error: undefined });
    try {
      const response = await this.api.post<WorkflowCompilationResponse>(
        "compile",
        { operators: plan.operators, links: plan.links, opsToReuseResult: [], opsToViewResult: [] },
        { signal: controller.signal }
      );
      if (!this.disposed && revision === this.compileRevision) this.update({ compilation: response, compiling: false });
      return response;
    } catch (error) {
      if (!this.disposed && revision === this.compileRevision)
        this.update({ compiling: false, error: error instanceof Error ? error.message : "Compilation failed." });
      throw error;
    }
  }
  async run(
    options: {
      executionName?: string;
      emailNotificationEnabled?: boolean;
      targetOperatorId?: string;
      replayFromExecution?: ReplayExecutionInfo;
    } = {}
  ): Promise<void> {
    if (this.disposed) throw new Error("The workflow session is closed.");
    if (this.session.isAutosaveSuspended("replay") && !options.replayFromExecution)
      throw new Error("Exit the historical preview or choose a replay interaction before running.");
    const unit = this.options.getComputingUnit();
    if (!unit) throw new Error("Select a computing unit before running.");
    if (!this.canControl()) throw new Error("Write permission on the computing unit is required to run a workflow.");
    if (this.session.getSnapshot().workflow.readonly && !options.replayFromExecution)
      throw new Error("A read-only workflow cannot be run.");
    if (["Failed", "Unknown", "Terminating"].includes(unit.status))
      throw new Error(`The selected computing unit is ${unit.status.toLowerCase()}.`);
    const warehouseId = this.options.getWarehouseId();
    if (this.options.config.warehouseEnabled && warehouseId === undefined)
      throw new Error("Create or select a warehouse before running.");
    if (this.socket.getSnapshot().status !== "connected")
      throw new Error("The workflow is not connected to the selected computing unit.");
    if (
      [
        ExecutionState.Running,
        ExecutionState.Initializing,
        ExecutionState.Pausing,
        ExecutionState.Paused,
        ExecutionState.Resuming,
        ExecutionState.Recovering,
      ].includes(this.snapshot.state)
    )
      throw new Error("An execution is already active.");
    const workflow = this.session.getSnapshot().workflow;
    const request = {
      executionName: options.executionName ?? workflow.name,
      engineVersion: this.options.engineVersion,
      logicalPlan: buildLogicalPlan(this.session.graph, options.targetOperatorId),
      workflowSettings: workflow.content.settings,
      emailNotificationEnabled: options.emailNotificationEnabled ?? false,
      computingUnitId: unit.cuid,
      warehouseId,
      replayFromExecution: options.replayFromExecution,
    };
    this.update({
      errors: [],
      console: {},
      operators: {},
      currentTuples: {},
      results: {},
      materializedResults: {},
      workers: {},
      regions: [],
      regionStates: {},
      tableStats: {},
      duration: 0,
      error: undefined,
    });
    this.setState(ExecutionState.Initializing);
    await new Promise<void>((resolve, reject) => {
      this.rejectRun = reject;
      this.runTimer = setTimeout(() => {
        this.rejectRun = undefined;
        this.runTimer = undefined;
        try {
          this.socket.send("WorkflowExecuteRequest", request);
          resolve();
        } catch (error) {
          this.setState(ExecutionState.Uninitialized);
          reject(error);
        }
      }, 150);
    });
  }
  pause(): void {
    this.requireState("pause", [ExecutionState.Running]);
    this.requireWritePermission();
    this.socket.send("WorkflowPauseRequest", {});
  }
  resume(): void {
    this.requireState("resume", [ExecutionState.Paused]);
    this.requireWritePermission();
    this.socket.send("WorkflowResumeRequest", {});
  }
  kill(): void {
    this.requireActive("kill");
    this.requireWritePermission();
    this.socket.send("WorkflowKillRequest", {});
  }
  checkpoint(): void {
    this.requireActive("take checkpoint");
    this.requireWritePermission();
    this.socket.send("WorkflowCheckpointRequest", {});
  }
  skip(workers: readonly string[]): void {
    this.requireState("skip tuples", [ExecutionState.Paused]);
    this.requireWritePermission();
    this.socket.send("SkipTupleRequest", { workers });
  }
  retry(workers: readonly string[]): void {
    this.requireState("retry tuple", [ExecutionState.Paused]);
    this.requireWritePermission();
    this.socket.send("RetryRequest", { workers });
  }
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.cleanups.forEach(cleanup => cleanup());
    this.listeners.clear();
    if (this.ticker) clearInterval(this.ticker);
    if (this.runTimer) clearTimeout(this.runTimer);
    if (this.compileTimer) clearTimeout(this.compileTimer);
    this.rejectRun?.(new Error("The workflow session was closed."));
    this.compileController?.abort();
    this.socket.close();
    this.session.setExecutionLocked(false);
  }
  private receive(event: TexeraWebsocketEvent): void {
    if (this.disposed) return;
    switch (event.type) {
      case "WorkflowStateEvent":
        if (Object.values(ExecutionState).includes(event.state) && event.state !== ExecutionState.Failed)
          this.setState(event.state);
        break;
      case "RecoveryStartedEvent":
        this.setState(ExecutionState.Recovering);
        break;
      case "WorkflowErrorEvent":
        this.update({ errors: event.fatalErrors });
        this.setState(ExecutionState.Failed);
        break;
      case "OperatorStatisticsUpdateEvent":
        this.update({ operators: { ...this.snapshot.operators, ...event.operatorStatistics } });
        break;
      case "OperatorCurrentTuplesUpdateEvent":
        this.update({ currentTuples: { ...this.snapshot.currentTuples, [event.operatorID]: event } });
        break;
      case "ConsoleUpdateEvent": {
        const limit = Math.max(0, this.options.config.operatorConsoleMessageBufferSize ?? 1000);
        const messages = [...(this.snapshot.console[event.operatorId] ?? []), ...event.messages];
        this.update({ console: { ...this.snapshot.console, [event.operatorId]: limit ? messages.slice(-limit) : [] } });
        break;
      }
      case "WorkerAssignmentUpdateEvent":
        this.update({ workers: { ...this.snapshot.workers, [event.operatorId]: event.workerIds } });
        break;
      case "RegionUpdateEvent":
        this.update({ regions: event.regions });
        break;
      case "RegionStateEvent":
        this.update({ regionStates: { ...this.snapshot.regionStates, [event.id]: event.state } });
        break;
      case "WebResultUpdateEvent": {
        const materializedResults = { ...this.snapshot.materializedResults };
        for (const [id, update] of Object.entries(event.updates)) {
          if (update.mode.type === "SetSnapshotMode" && "table" in update) materializedResults[id] = update.table;
          else if (update.mode.type === "PaginationMode") delete materializedResults[id];
        }
        this.update({
          results: { ...this.snapshot.results, ...event.updates },
          materializedResults,
          tableStats: event.tableStats,
        });
        break;
      }
      case "ExecutionDurationUpdateEvent":
        this.durationAnchor = { reported: event.duration, at: Date.now(), running: event.isRunning };
        if (this.ticker) clearInterval(this.ticker);
        this.ticker = event.isRunning ? setInterval(() => this.tick(), 1000) : undefined;
        this.tick();
        break;
      case "ModifyLogicResponse":
        if (!event.isValid) this.update({ error: event.errorMessage });
        break;
    }
  }
  private setState(state: ExecutionState): void {
    this.update({ state });
    this.session.setExecutionLocked(
      [
        ExecutionState.Initializing,
        ExecutionState.Running,
        ExecutionState.Pausing,
        ExecutionState.Paused,
        ExecutionState.Resuming,
        ExecutionState.Recovering,
      ].includes(state)
    );
  }
  private tick(): void {
    const anchor = this.durationAnchor;
    this.update({ duration: anchor.reported + (anchor.running ? Date.now() - anchor.at : 0) });
  }
  private requireState(action: string, allowed: ExecutionState[]): void {
    if (!allowed.includes(this.snapshot.state))
      throw new Error(`Cannot ${action} workflow in state ${this.snapshot.state}.`);
  }
  private requireActive(action: string): void {
    if (
      [
        ExecutionState.Uninitialized,
        ExecutionState.Completed,
        ExecutionState.Failed,
        ExecutionState.Killed,
        ExecutionState.Terminated,
      ].includes(this.snapshot.state)
    )
      throw new Error(`Cannot ${action} workflow in state ${this.snapshot.state}.`);
  }
  private requireWritePermission(): void {
    if (!this.canControl()) throw new Error("Write permission on the computing unit is required to control execution.");
  }
  private update(patch: Partial<RuntimeSnapshot>): void {
    if (!this.disposed) {
      this.snapshot = { ...this.snapshot, ...patch };
      this.listeners.forEach(listener => listener());
    }
  }
}
