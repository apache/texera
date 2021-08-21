import {
  BreakpointInfo,
  LogicalOperator,
  LogicalPlan,
  WorkflowResultUpdateEvent,
  WorkflowStatusUpdate
} from './execute-workflow.interface';
import { BreakpointFaultedTuple, BreakpointTriggerInfo, PythonPrintTriggerInfo } from './workflow-common.interface';


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

export interface WebSocketHelloWorld extends Readonly<{ message: string }> {}

export interface TexeraConstraintViolation extends Readonly<{
  message: string;
  propertyPath: string;
}> {}

export interface WorkflowError extends Readonly<{
  operatorErrors: Record<string, TexeraConstraintViolation>,
  generalErrors: Record<string, string>
}> {}

export interface WorkflowExecutionError extends Readonly<{
  message: string
}> {}

export type ModifyOperatorLogic = Readonly<{
  operator: LogicalOperator
}>;

export type SkipTuple = Readonly<{
  actorPath: string;
  faultedTuple: BreakpointFaultedTuple
}>;

export type WorkerTuples = Readonly<{
  workerID: string,
  tuple: ReadonlyArray<string>
}>;

export type OperatorCurrentTuples = Readonly<{
  operatorID: string,
  tuples: ReadonlyArray<WorkerTuples>
}>;

export type PaginationRequest = Readonly<{
  requestID: string,
  operatorID: string,
  pageIndex: number,
  pageSize: number
}>;

export type PaginatedResultEvent = Readonly<{
  requestID: string,
  operatorID: string,
  pageIndex: number,
  table: ReadonlyArray<object>,
}>;

export type ResultDownloadRequest = Readonly<{
  downloadType: string,
  workflowName: string,
  operatorId: string
}>;

export type ResultDownloadResponse = Readonly<{
  message: string
}>;

export type TexeraWebsocketRequestTypeMap = {
  'HelloWorldRequest': WebSocketHelloWorld,
  'HeartBeatRequest': {},
  'ExecuteWorkflowRequest': LogicalPlan,
  'PauseWorkflowRequest': {},
  'ResumeWorkflowRequest': {},
  'KillWorkflowRequest': {},
  'ModifyLogicRequest': ModifyOperatorLogic,
  'SkipTupleRequest': SkipTuple,
  'AddBreakpointRequest': BreakpointInfo,
  'ResultPaginationRequest': PaginationRequest,
  'ResultDownloadRequest': ResultDownloadRequest,
};

export type TexeraWebsocketEventTypeMap = {
  'HelloWorldResponse': WebSocketHelloWorld,
  'HeartBeatResponse': {},
  'WorkflowErrorEvent': WorkflowError,
  'WorkflowStartedEvent': {},
  'WorkflowCompletedEvent': {},
  'WebWorkflowStatusUpdateEvent': WorkflowStatusUpdate,
  'WebResultUpdateEvent': WorkflowResultUpdateEvent,
  'WorkflowPausedEvent': {},
  'WorkflowResumedEvent': {},
  'RecoveryStartedEvent': {},
  'BreakpointTriggeredEvent': BreakpointTriggerInfo,
  'PythonPrintTriggeredEvent': PythonPrintTriggerInfo,
  'ModifyLogicCompletedEvent': {},
  'OperatorCurrentTuplesUpdateEvent': OperatorCurrentTuples,
  'PaginatedResultEvent': PaginatedResultEvent,
  'WorkflowExecutionErrorEvent': WorkflowExecutionError,
  'ResultDownloadResponse': ResultDownloadResponse
};

// helper type definitions to generate the request and event types
type ValueOf<T> = T[keyof T];
type CustomUnionType<T> = ValueOf<{
  [P in keyof T]:
  {
    type: P;
  } &
  T[P]
}>;

export type TexeraWebsocketRequestTypes = keyof TexeraWebsocketRequestTypeMap;
export type TexeraWebsocketRequest = CustomUnionType<TexeraWebsocketRequestTypeMap>;

export type TexeraWebsocketEventTypes = keyof TexeraWebsocketEventTypeMap;
export type TexeraWebsocketEvent = CustomUnionType<TexeraWebsocketEventTypeMap>;


