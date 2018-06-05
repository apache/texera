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


export interface ExecutionResult extends Readonly<{
  code: number,
  // show only when correct result
  result?: object[],
  resultID?: string,
  // show only when incorrect result
  message?: string
}> { }
