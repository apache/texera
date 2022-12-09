import {
  BreakpointInfo,
  ExecutionState,
  LogicalOperator,
  LogicalPlan,
  WebOutputMode,
  WorkflowResultUpdateEvent,
  OperatorStatsUpdate,
} from "./execute-workflow.interface";
import { IndexableObject } from "./result-table.interface";
import { BreakpointFaultedTuple, BreakpointTriggerInfo, ConsoleUpdateEvent } from "./workflow-common.interface";

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

export interface RegisterWIdRequest
  extends Readonly<{
    wId: number;
  }> {}

export interface WorkflowExecuteRequest
  extends Readonly<{
    executionName: string;
    engineVersion: string;
    logicalPlan: LogicalPlan;
  }> {}

export interface RegisterWIdEvent extends Readonly<{ message: string }> {}

export interface TexeraConstraintViolation
  extends Readonly<{
    message: string;
    propertyPath: string;
  }> {}

export interface WorkflowError
  extends Readonly<{
    operatorErrors: Record<string, TexeraConstraintViolation>;
    generalErrors: Record<string, string>;
  }> {}

export interface WorkflowExecutionError
  extends Readonly<{
    message: string;
  }> {}

export type ModifyOperatorLogic = Readonly<{
  operator: LogicalOperator;
}>;

export type SkipTuple = Readonly<{
  actorPath: string;
  faultedTuple: BreakpointFaultedTuple;
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
}>;

export type CacheStatusUpdateRequest = LogicalPlan;

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

export type OperatorResultCacheStatus = "cache invalid" | "cache valid" | "cache not enabled";

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

export type DebugCommandRequest = Readonly<{
  operatorId: string;
  workerId: string;
  cmd: string;
}>;

export type WorkflowStateInfo = Readonly<{
  state: ExecutionState;
}>;

export type TexeraWebsocketRequestTypeMap = {
  RegisterWIdRequest: RegisterWIdRequest;
  AddBreakpointRequest: BreakpointInfo;
  CacheStatusUpdateRequest: CacheStatusUpdateRequest;
  HeartBeatRequest: {};
  ModifyLogicRequest: ModifyOperatorLogic;
  ResultExportRequest: ResultExportRequest;
  ResultPaginationRequest: PaginationRequest;
  RetryRequest: {};
  SkipTupleRequest: SkipTuple;
  WorkflowExecuteRequest: WorkflowExecuteRequest;
  WorkflowKillRequest: {};
  WorkflowPauseRequest: {};
  WorkflowResumeRequest: {};
  PythonExpressionEvaluateRequest: PythonExpressionEvaluateRequest;
  DebugCommandRequest: DebugCommandRequest;
};

export type TexeraWebsocketEventTypeMap = {
  RegisterWIdResponse: RegisterWIdEvent;
  HeartBeatResponse: {};
  WorkflowStateEvent: WorkflowStateInfo;
  WorkflowErrorEvent: WorkflowError;
  OperatorStatisticsUpdateEvent: OperatorStatsUpdate;
  WebResultUpdateEvent: WorkflowResultUpdateEvent;
  RecoveryStartedEvent: {};
  BreakpointTriggeredEvent: BreakpointTriggerInfo;
  ConsoleUpdateEvent: ConsoleUpdateEvent;
  OperatorCurrentTuplesUpdateEvent: OperatorCurrentTuples;
  PaginatedResultEvent: PaginatedResultEvent;
  WorkflowExecutionErrorEvent: WorkflowExecutionError;
  ResultExportResponse: ResultExportResponse;
  WorkflowAvailableResultEvent: WorkflowAvailableResultEvent;
  CacheStatusUpdateEvent: CacheStatusUpdateEvent;
  PythonExpressionEvaluateResponse: PythonExpressionEvaluateResponse;
  WorkerAssignmentUpdateEvent: WorkerAssignmentUpdateEvent;
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
