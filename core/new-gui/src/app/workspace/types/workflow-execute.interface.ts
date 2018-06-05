/**
 * This file contains some type declaration for what
 *  should be sent directly to the backend
*/

export interface LogicalLink extends Readonly<{
  origin: string,
  destination: string
}> { }

export interface LogicalOperator extends Readonly<{
  operatorID: string,
  operatorType: string,
  [uniqueAttributes: string]: any
}> { }

export interface LogicalPlan extends Readonly<{
  operators: LogicalOperator[],
  links: LogicalLink[]
}> { }


export interface SuccessExecutionResult extends Readonly<{
  code: 0,
  result: ReadonlyArray<object>,
  resultID: string
}> { }

export interface ErrorExecutionResult extends Readonly< {
  code: 1,
  message: string
}> { }

export type ExecutionResult = SuccessExecutionResult | ErrorExecutionResult;
