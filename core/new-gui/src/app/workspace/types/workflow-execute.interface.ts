/**
 * This file contains some type declaration for what
 *  should be sent directly to the backend
*/

export interface LogicalLink extends Readonly<{
  origin: string,
  destination: string
}> { }

export interface LogicalPlan extends Readonly<{
  operators: Object,
  links: LogicalLink[]
}> { }
