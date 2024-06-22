/**
 * This file contains some type declaration for the WorkflowGraph interface of the **backend**.
 * The API of the backend is (currently) not the same as the Graph representation in the frontend.
 * These interfaces confront to the backend API.
 */

import { WorkflowFatalError, OperatorCurrentTuples } from "./workflow-websocket.interface";
export interface PortIdentity
  extends Readonly<{
    id: number;
    internal: boolean;
  }> {}
export interface OutputPort extends Readonly<{ id: PortIdentity; displayName: string }> {}
export interface InputPort
  extends Readonly<{
    id: PortIdentity;
    displayName: string;
    allowMultiLinks: boolean;
    dependencies: ReadonlyArray<PortIdentity>;
  }> {}

export interface LogicalLink
  extends Readonly<{
    fromOpId: string;
    fromPortId: PortIdentity;
    toOpId: string;
    toPortId: PortIdentity;
  }> {}

export interface LogicalOperator
  extends Readonly<{
    operatorID: string;
    operatorType: string;
    [uniqueAttributes: string]: any;
  }> {}

/**
 * LogicalPlan is the backend interface equivalent of frontend interface WorkflowGraph,
 *  they represent the same thing - the backend term currently used is LogicalPlan.
 * However, the format and content of the backend interface is different.
 */
export interface LogicalPlan
  extends Readonly<{
    operators: LogicalOperator[];
    links: LogicalLink[];
    opsToViewResult?: string[];
    opsToReuseResult?: string[];
  }> {}
export enum OperatorState {
  Uninitialized = "Uninitialized",
  Initializing = "Initializing",
  Ready = "Ready",
  Running = "Running",
  Pausing = "Pausing",
  Paused = "Paused",
  Resuming = "Resuming",
  Completed = "Completed",
  Recovering = "Recovering",
}

export interface OperatorStatistics
  extends Readonly<{
    operatorState: OperatorState;
    aggregatedInputRowCount: number;
    aggregatedOutputRowCount: number;
  }> {}

export interface OperatorStatsUpdate
  extends Readonly<{
    operatorStatistics: Record<string, OperatorStatistics>;
  }> {}

export type PaginationMode = { type: "PaginationMode" };
export type SetSnapshotMode = { type: "SetSnapshotMode" };
export type SetDeltaMode = { type: "SetDeltaMode" };
export type WebOutputMode = PaginationMode | SetSnapshotMode | SetDeltaMode;

export interface WebPaginationUpdate
  extends Readonly<{
    mode: PaginationMode;
    totalNumTuples: number;
    dirtyPageIndices: ReadonlyArray<number>;
  }> {}

export interface WebDataUpdate
  extends Readonly<{
    mode: SetSnapshotMode | SetDeltaMode;
    table: ReadonlyArray<object>;
  }> {}

export type WebResultUpdate = WebPaginationUpdate | WebDataUpdate;

export type WorkflowResultUpdate = Record<string, WebResultUpdate>;
export type WorkflowResultTableStats = Record<string, Record<string, Record<string, number>>>;

export interface WorkflowResultUpdateEvent
  extends Readonly<{
    updates: WorkflowResultUpdate;
    tableStats: WorkflowResultTableStats;
    sinkStorageMode: string;
  }> {}

// user-defined type guards to check the type of the result update
// because TypeScript can't do Tagged Unions on nested data types https://github.com/microsoft/TypeScript/issues/18758
// and the unions have to be defined as nested because of JSON serialization options
export function isWebPaginationUpdate(update: WebResultUpdate): update is WebPaginationUpdate {
  return update !== undefined && update.mode.type === "PaginationMode";
}

export function isWebDataUpdate(update: WebResultUpdate): update is WebDataUpdate {
  return (update !== undefined && update.mode.type === "SetSnapshotMode") || update.mode.type === "SetDeltaMode";
}

export function isNotInExecution(state: ExecutionState) {
  return [
    ExecutionState.Uninitialized,
    ExecutionState.Failed,
    ExecutionState.Killed,
    ExecutionState.Completed,
  ].includes(state);
}

export enum ExecutionState {
  Uninitialized = "Uninitialized",
  Initializing = "Initializing",
  Running = "Running",
  Pausing = "Pausing",
  Paused = "Paused",
  Resuming = "Resuming",
  Recovering = "Recovering",
  Completed = "Completed",
  Failed = "Failed",
  Killed = "Killed",
}

export type ExecutionStateInfo = Readonly<
  | {
      state:
        | ExecutionState.Uninitialized
        | ExecutionState.Initializing
        | ExecutionState.Pausing
        | ExecutionState.Running
        | ExecutionState.Resuming
        | ExecutionState.Recovering;
    }
  | {
      state: ExecutionState.Paused;
      currentTuples: Readonly<Record<string, OperatorCurrentTuples>>;
    }
  | {
      state: ExecutionState.Completed | ExecutionState.Killed;
    }
  | {
      state: ExecutionState.Failed;
      errorMessages: ReadonlyArray<WorkflowFatalError>;
    }
>;
