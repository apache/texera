import { OperatorPredicate, OperatorLink } from './workflow-graph';

/**
 * WorkflowGraphReadonly only exposes the read functions of a workflow graph.
 * It should contain the function signarture of all the getter functions that the WorkflowGraph has.
 */
export interface WorkflowGraphReadonly {

  hasOperator(operatorID: string): boolean;

  getOperator(operatorID: string): OperatorPredicate;

  getOperators(): OperatorPredicate[];

  hasLinkWithID(linkID: string): boolean;

  hasLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): boolean;

  getLinkWithID(linkID: string): OperatorLink;

  getLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): OperatorLink;

  getLinks(): OperatorLink[];

}
