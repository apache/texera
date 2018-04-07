import { OperatorPredicate, OperatorLink } from './workflow-graph';

export interface WorkflowGraphReadonly {

  hasOperator(operatorID: string): boolean;

  getOperator(operatorID: string): OperatorPredicate;

  getOperators(): OperatorPredicate[];

  hasLink(linkID: string): boolean;

  getLink(linkID: string): OperatorLink;

  getLinks(): OperatorLink[];

}
