/**
 * This file contains some type declaration for the WorkflowGraph interface of the **backend**.
 * The API of the backend is (currently) not the same as the Graph representation in the frontend.
 * These interfaces confronts to the backend API.
*/

export interface LogicalLink extends Readonly<{
  origin: string,
  destination: string
}> { }

export interface LogicalOperator extends Readonly<{
  operatorID: string,
  operatorType: string,
  // reason for not using `any` in this case is to
  //  prevent types such as `undefined` or `null`
  [uniqueAttributes: string]: string | number | boolean | object
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

/**
 * Discriminated Union
 * http://www.typescriptlang.org/docs/handbook/advanced-types.html
 *
 * Execution Result type can be either SuccessExecutionResult type
 *  or ErrorExecutionResult type. But cannot contain both structures
 * at the same time
 */
export type ExecutionResult = SuccessExecutionResult | ErrorExecutionResult;
