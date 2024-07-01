import {
  ExecutionState,
  LogicalOperator,
  LogicalPlan,
  WebOutputMode,
  WorkflowResultUpdateEvent,
  OperatorStatsUpdate,
} from "./execute-workflow.interface";
import { IndexableObject } from "./result-table.interface";
import { ConsoleUpdateEvent } from "./workflow-common.interface";

/**
 *  @fileOverview Type Definitions of WebSocket (Ws) API
 * WebSocket API can be either
 * either "Request" types - messages from frontend to server
 * or     "Event" types - messages from server to frontend.
 *
 * Each type definition MUST follow the following rules:
 * in either TexeraWebsocketRequestTypeMap or TexeraWebsocketEventTypeMap
 * add a map entry:
 * 1. key is the 'type' string, it must be the same as corresponding backend class name
 * 2. value is the payload this request/event needs
 */

export interface WorkflowExecuteRequest
  extends Readonly<{
    executionName: string;
    engineVersion: string;
    logicalPlan: LogicalPlan;
  }> {}

export interface ReplayExecutionInfo
  extends Readonly<{
    eid: number;
    interaction: string;
  }> {}

export interface WorkflowFatalError
  extends Readonly<{
    message: string;
    details: string;
    operatorId: string;
    workerId: string;
    type: {
      name: string;
    };
    timestamp: {
      nanos: number;
      seconds: number;
    };
  }> {}

export interface WorkflowErrorEvent
  extends Readonly<{
    fatalErrors: ReadonlyArray<WorkflowFatalError>;
  }> {}

export type ModifyOperatorLogic = Readonly<{
  operator: LogicalOperator;
}>;

export type WorkerTuples = Readonly<{
  workerID: string;
  tuple: ReadonlyArray<string>;
}>;

export type OperatorCurrentTuples = Readonly<{
  operatorID: string;
  tuples: ReadonlyArray<WorkerTuples>;
}>;

export type PaginationRequest = Readonly<{
  requestID: string;
  operatorID: string;
  pageIndex: number;
  pageSize: number;
}>;

export type PaginatedResultEvent = Readonly<{
  requestID: string;
  operatorID: string;
  pageIndex: number;
  table: ReadonlyArray<IndexableObject>;
}>;

export type ResultExportRequest = Readonly<{
  exportType: string;
  workflowId: number;
  workflowName: string;
  operatorId: string;
  operatorName: string;
  datasetIds: ReadonlyArray<number>;
}>;

export type ResultExportResponse = Readonly<{
  status: "success" | "error";
  message: string;
}>;

export type OperatorAvailableResult = Readonly<{
  operatorID: string;
  cacheValid: boolean;
  outputMode: WebOutputMode;
}>;

export type WorkflowAvailableResultEvent = Readonly<{
  availableOperators: ReadonlyArray<OperatorAvailableResult>;
}>;

export type OperatorResultCacheStatus = "cache invalid" | "cache valid";

export interface CacheStatusUpdateEvent
  extends Readonly<{
    cacheStatusMap: Record<string, OperatorResultCacheStatus>;
  }> {}

export type PythonExpressionEvaluateRequest = Readonly<{
  expression: string;
  operatorId: string;
}>;
export type TypedValue = Readonly<{
  expression: string;
  valueRef: string;
  valueStr: string;
  valueType: string;
  expandable: boolean;
}>;
export type EvaluatedValue = Readonly<{
  value: TypedValue;
  attributes: TypedValue[];
}>;

export type PythonExpressionEvaluateResponse = Readonly<{
  expression: string;
  values: EvaluatedValue[];
}>;

export type WorkerAssignmentUpdateEvent = Readonly<{
  operatorId: string;
  workerIds: readonly string[];
}>;

export type ExecutionDurationUpdateEvent = Readonly<{
  duration: number;
  isRunning: boolean;
}>;

export type ClusterStatusUpdateEvent = Readonly<{
  numWorkers: number;
}>;

export type ModifyLogicResponse = Readonly<{
  opId: string;
  isValid: boolean;
  errorMessage: string;
}>;

export type ModifyLogicCompletedEvent = Readonly<{
  opIds: readonly string[];
}>;

export type DebugCommandRequest = Readonly<{
  operatorId: string;
  workerId: string;
  cmd: string;
}>;

export type WorkflowStateInfo = Readonly<{
  state: ExecutionState;
}>;

export type TexeraWebsocketRequestTypeMap = {
  EditingTimeCompilationRequest: LogicalPlan;
  HeartBeatRequest: {};
  ModifyLogicRequest: ModifyOperatorLogic;
  ResultExportRequest: ResultExportRequest;
  ResultPaginationRequest: PaginationRequest;
  RetryRequest: { workers: ReadonlyArray<string> };
  SkipTupleRequest: { workers: ReadonlyArray<string> };
  WorkflowExecuteRequest: WorkflowExecuteRequest;
  WorkflowKillRequest: {};
  WorkflowPauseRequest: {};
  WorkflowCheckpointRequest: {};
  WorkflowResumeRequest: {};
  PythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest;
  DebugCommandRequest: DebugCommandRequest;
};

export type TexeraWebsocketEventTypeMap = {
  HeartBeatResponse: {};
  WorkflowStateEvent: WorkflowStateInfo;
  OperatorStatisticsUpdateEvent: OperatorStatsUpdate;
  WebResultUpdateEvent: WorkflowResultUpdateEvent;
  RecoveryStartedEvent: {};
  WorkflowErrorEvent: WorkflowErrorEvent;
  ConsoleUpdateEvent: ConsoleUpdateEvent;
  OperatorCurrentTuplesUpdateEvent: OperatorCurrentTuples;
  PaginatedResultEvent: PaginatedResultEvent;
  ResultExportResponse: ResultExportResponse;
  WorkflowAvailableResultEvent: WorkflowAvailableResultEvent;
  CacheStatusUpdateEvent: CacheStatusUpdateEvent;
  PythonExpressionEvaluateResponse: PythonExpressionEvaluateResponse;
  WorkerAssignmentUpdateEvent: WorkerAssignmentUpdateEvent;
  ModifyLogicResponse: ModifyLogicResponse;
  ModifyLogicCompletedEvent: ModifyLogicCompletedEvent;
  ExecutionDurationUpdateEvent: ExecutionDurationUpdateEvent;
  ClusterStatusUpdateEvent: ClusterStatusUpdateEvent;
};

// helper type definitions to generate the request and event types
type ValueOf<T> = T[keyof T];
type CustomUnionType<T> = ValueOf<{
  [P in keyof T]: {
    type: P;
  } & T[P];
}>;

export type TexeraWebsocketRequestTypes = keyof TexeraWebsocketRequestTypeMap;
export type TexeraWebsocketRequest = CustomUnionType<TexeraWebsocketRequestTypeMap>;

export type TexeraWebsocketEventTypes = keyof TexeraWebsocketEventTypeMap;
export type TexeraWebsocketEvent = CustomUnionType<TexeraWebsocketEventTypeMap>;
