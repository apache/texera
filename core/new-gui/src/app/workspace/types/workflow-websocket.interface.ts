import { LogicalPlan, WorkflowStatusUpdate, ResultObject, LogicalOperator } from './execute-workflow.interface';
import { BreakpointTriggerInfo } from './workflow-common.interface';


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

export interface WebSocketHelloWorld extends Readonly<{message: string}> { }

export interface TexeraConstraintViolation extends Readonly<{
  message: string;
  propertyPath: string;
}> {}

export interface WorkflowCompilationError extends Readonly<{
  violations: Record<string, TexeraConstraintViolation>
}> { }


export interface WorkflowRuntimeError extends Readonly<{
}> { }
export interface WorkflowRuntimeErrorEvent extends Readonly<{
  type: 'WorkflowRuntimeErrorEvent'
} & WorkflowRuntimeError> { }

export type ModifyOperatorLogic = Readonly<{
  operator: LogicalOperator
}>;

export type TexeraWebsocketRequestTypeMap = {
  'HelloWorldRequest': WebSocketHelloWorld,
  'ExecuteWorkflowRequest': LogicalPlan,
  'PauseWorkflowRequest': {},
  'ResumeWorkflowRequest': {},
  'ModifyLogicRequest': ModifyOperatorLogic
};

export type TexeraWebsocketEventTypeMap = {
  'HelloWorldResponse': WebSocketHelloWorld,
  'WorkflowCompilationErrorEvent': WorkflowCompilationError,
  'WorkflowStartedEvent': {},
  'WorkflowCompletedEvent': {result: ReadonlyArray<ResultObject>},
  'WorkflowStatusUpdateEvent': WorkflowStatusUpdate,
  'WorkflowPausedEvent': {},
  'WorkflowResumedEvent': {},
  'BreakpointTriggeredEvent': BreakpointTriggerInfo,
  'ModifyLogicCompletedEvent': {}
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


