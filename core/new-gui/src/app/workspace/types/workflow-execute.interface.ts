/**
 * This file contains some type declaration for what
 *  should be sent directly to the backend
*/

import { OperatorPort } from './workflow-common.interface';



export interface LogicalLink extends Readonly<{
  origin: OperatorPort,
  destination: OperatorPort
}> { }

export interface LogicalPlan extends Readonly<{
  operators: Object,
  links: LogicalLink[]
}> { }
