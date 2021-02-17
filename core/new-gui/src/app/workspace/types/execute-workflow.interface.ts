/**
 * This file contains some type declaration for the WorkflowGraph interface of the **backend**.
 * The API of the backend is (currently) not the same as the Graph representation in the frontend.
 * These interfaces confronts to the backend API.
 */

import { ChartType } from './visualization.interface';
import { BreakpointRequest, BreakpointTriggerInfo } from './workflow-common.interface';
import { OperatorCurrentTuples } from './workflow-websocket.interface';

export interface LogicalLink extends Readonly<{
  origin: { operatorID: string, portOrdinal: number },
  destination: { operatorID: string, portOrdinal: number },
}> { }

export interface LogicalOperator extends Readonly<{
  operatorID: string,
  operatorType: string,
  // reason for not using `any` in this case is to
  //  prevent types such as `undefined` or `null`
  [uniqueAttributes: string]: string | number | boolean | object
}> { }

export interface BreakpointInfo extends Readonly<{
  operatorID: string,
  breakpoint: BreakpointRequest
}> { }

/**
 * LogicalPlan is the backend interface equivalent of frontend interface WorkflowGraph,
 *  they represent the same thing - the backend term currently used is LogicalPlan.
 * However, the format and content of the backend interface is different.
 */
export interface LogicalPlan extends Readonly<{
  operators: LogicalOperator[],
  links: LogicalLink[],
  breakpoints: BreakpointInfo[]
}> { }

/**
 * The backend interface of the return object of a successful execution
 */
export interface ResultObject extends Readonly<{
  operatorID: string,
  table: ReadonlyArray<object | string[]>,
  chartType: ChartType | undefined,
  totalRowCount: number
}> {

}

export interface SuccessExecutionResult extends Readonly<{
  code: 0,
  result: ReadonlyArray<ResultObject>,
  resultID: string
}> { }

/**
 * The backend interface of the return object of a failed execution
 */
export interface ErrorExecutionResult extends Readonly<{
  code: 1,
  message: string
}> { }

/**
 * Discriminated Union
 * http://www.typescriptlang.org/docs/handbook/advanced-types.html
 *
 * ExecutionResult type can be either SuccessExecutionResult or ErrorExecutionResult.
 *  but cannot contain both structures at the same time.
 * In this case:
 *  if the code value is 0, then the object type must be SuccessExecutionResult
 *  if the code value is 1, then the object type must be ErrorExecutionResult
 */
export type ExecutionResult = SuccessExecutionResult | ErrorExecutionResult;

export enum OperatorState {
  Uninitialized = 'Uninitialized',
  Initializing = 'Initializing',
  Ready = 'Ready',
  Running = 'Running',
  Pausing = 'Pausing',
  CollectingBreakpoints = 'CollectingBreakpoints',
  Paused = 'Paused',
  Resuming = 'Resuming',
  Completed = 'Completed',
  Recovering = 'Recovering',
}

export interface OperatorStatistics extends Readonly<{
  operatorState: OperatorState,
  aggregatedInputRowCount: number,
  aggregatedOutputRowCount: number
}> { }

export interface WorkflowStatusUpdate extends Readonly<{
  operatorStatistics: Record<string, OperatorStatistics>
}> { }

export enum ExecutionState {
  Uninitialized = 'Uninitialized',
  WaitingToRun = 'WaitingToRun',
  Running = 'Running',
  Pausing = 'Pausing',
  Paused = 'Paused',
  Resuming = 'Resuming',
  Recovering = 'Recovering',
  BreakpointTriggered = 'BreakpointTriggered',
  Completed = 'Completed',
  Failed = 'Failed'
}

export type ExecutionStateInfo = Readonly<{
  state: ExecutionState.Uninitialized | ExecutionState.WaitingToRun | ExecutionState.Running
  | ExecutionState.Pausing | ExecutionState.Resuming | ExecutionState.Recovering
} | {
  state: ExecutionState.Paused, currentTuples: Readonly<Record<string, OperatorCurrentTuples>>
} | {
  state: ExecutionState.BreakpointTriggered, breakpoint: BreakpointTriggerInfo
} | {
  state: ExecutionState.Completed, resultID: string | undefined, resultMap: ReadonlyMap<string, ResultObject>
} | {
  state: ExecutionState.Failed, errorMessages: Readonly<Record<string, string>>
}>;
