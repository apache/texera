import { OperatorPredicate, OperatorLink } from './workflow-graph';

export interface WorkflowGraphReadonly {

  hasOperator(operatorID: string): boolean;

  getOperator(operatorID: string): OperatorPredicate;

  getOperators(): OperatorPredicate[];

  hasLinkWithID(linkID: string): boolean;

  hasLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): boolean;

  getLink(linkID: string): OperatorLink;

  getLinks(): OperatorLink[];

}
