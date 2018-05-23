import { Observable } from 'rxjs/Observable';
import { OperatorPredicate, OperatorLink, OperatorPort } from './common.interface';

/**
 * WorkflowGraphReadonly is an interface only exposing the read functions of a workflow graph.
 * It contains the function signarture of all the getter functions that the WorkflowGraph has.
 * The implementation class is WorkflowGraph.
 */
export interface WorkflowGraphReadonly {

  /**
   * Returns whether the operator exists in the graph.
   * @param operatorID operator ID
   */
  hasOperator(operatorID: string): boolean;

  /**
   * Gets the operator with the operatorID.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  getOperator(operatorID: string): OperatorPredicate | undefined;

  /**
   * Returns an array of all operators in the graph
   */
  getOperators(): OperatorPredicate[];

  /**
   * Returns whether the graph contains the link with the linkID
   * @param linkID link ID
   */
  hasLinkWithID(linkID: string): boolean;

  /**
   * Returns wheter the graph contains the link with the source and target
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  hasLink(source: OperatorPort, target: OperatorPort): boolean;

  /**
   * Returns a link with the linkID from operatorLinkMap.
   * Returns undefined if the link doesn't exist.
   * @param linkID link ID
   */
  getLinkWithID(linkID: string): OperatorLink | undefined;

  /**
   * Returns a link with the source and target from operatorLinkMap.
   * Returns undefined if the link doesn't exist.
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  getLink(source: OperatorPort, target: OperatorPort): OperatorLink | undefined;

  /**
   * Returns an array of all the links in the graph.
   */
  getLinks(): OperatorLink[];

  /**
   * Gets the observable event stream of an operator being added into the graph.
   */
  getOperatorAddStream(): Observable<OperatorPredicate>;

  /**
   * Gets the observable event stream of an operator being deleted from the graph.
   * The observable value is the deleted operator.
   */
  getOperatorDeleteStream(): Observable<{deletedOperator: OperatorPredicate}>;

  /**
   *ets the observable event stream of a link being added into the graph.
   */
  getLinkAddStream(): Observable<OperatorLink>;

  /**
   * Gets the observable event stream of a link being deleted from the graph.
   * The observable value is the deleted link.
   */
  getLinkDeleteStream(): Observable<{deletedLink: OperatorLink}>;

  /**
   * Gets the observable event stream of a link being deleted from the graph.
   * The observable value includes the old property that is replaced, and the operator with new property.
   */
  getOperatorPropertyChangeStream(): Observable<{oldProperty: Object, operator: OperatorPredicate}>;

}
