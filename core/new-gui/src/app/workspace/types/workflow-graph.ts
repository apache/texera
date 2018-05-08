import { OperatorPredicate, OperatorLink, OperatorPort } from './common.interface';
import { WorkflowGraphReadonly } from './workflow-graph-readonly.interface';
import { isEqual } from 'lodash-es';


/**
 * WorkflowGraph will save all the operators and links, and
 *  handle all the ground level events such as add and delete operators and links. This
 *  class will become a model that will be synchronous with JointJS model.
 *
 */
export class WorkflowGraph implements WorkflowGraphReadonly {

  private operatorIDMap = new Map<string, OperatorPredicate>();
  private operatorLinkMap = new Map<string, OperatorLink>();

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = []
  ) {
    operatorPredicates.forEach(op => this.operatorIDMap.set(op.operatorID, op));
    operatorLinks.forEach(link => this.operatorLinkMap.set(link.linkID, link));
  }

  /**
   * This function will first check if the operator passed in is valid. If not,
   *  it will throw an error immediately.
   * If no error, this method will add a key-value pair <ID, operator> to the
   *  operatorIDMap.
   * @param operator OperatorPredicate
   */
  public addOperator(operator: OperatorPredicate): void {
    WorkflowGraph.checkIsValidOperator(this, operator);
    this.operatorIDMap.set(operator.operatorID, operator);
  }

  /**
   * This method will first get the operator using the operatorID provided.
   *  If the operator does not exist in the operatorIDMap, it will throw
   *  an error immediately and exit the program.
   * If the operator is found, this function will delete the operator with
   *  this operatorID in the operatorIDMap
   * @param operatorID operator ID
   */
  public deleteOperator(operatorID: string): OperatorPredicate {
    const operator = this.getOperator(operatorID);
    this.operatorIDMap.delete(operatorID);
    return operator;
  }

  /**
   * This method will check if the operatorID exists in the operatorIDMap.
   * @param operatorID operator ID
   */
  public hasOperator(operatorID: string): boolean {
    return this.operatorIDMap.has(operatorID);
  }

  /**
   * This method will get an operator using the operatorID. If operatorID
   *  is not in the operatorIDMap, throw an error and exit immediately.
   *
   * @param operatorID operator ID
   */
  public getOperator(operatorID: string): OperatorPredicate {
    const operator = this.operatorIDMap.get(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator;
  }

  /**
   * This method will return an array of all operators in the operatorIDMap
   */
  public getOperators(): OperatorPredicate[] {
    return Array.from(this.operatorIDMap.values());
  }

  /**
   * This method will first check if the link is valid. If no
   *  error is thrown, add this link to the operatorLinkMap
   * @param link OpearatorLink
   */
  public addLink(link: OperatorLink): void {
    WorkflowGraph.checkIsValidLink(this, link);
    this.operatorLinkMap.set(link.linkID, link);
  }

  /**
   * This method will get the link using a linkID. If the link exists,
   *  delete the link from the operatorLinkMap using the linkID.
   * @param linkID link ID
   */
  public deleteLinkWithID(linkID: string): OperatorLink {
    const link = this.getLinkWithID(linkID);
    this.operatorLinkMap.delete(linkID);
    return link;
  }

  /**
   * Check if the operatorLinkMap contains a link where the source
   *  and target is identical to the source port and target port
   *  passed. If exist, get the link and use the link ID to delete
   *  the link from operatorLinkMap.
   *
   * @param source source port
   * @param target target port
   */
  public deleteLink(source: OperatorPort, target: OperatorPort): OperatorLink {
    if (!this.hasLink(source, target)) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} doesn't exist`);
    }
    const link = this.getLink(source, target);
    this.operatorLinkMap.delete(link.linkID);
    return link;
  }

  /**
   * return whether the operatorLinkMap contains the linkID passed
   *
   * @param linkID link ID
   */
  public hasLinkWithID(linkID: string): boolean {
    return this.operatorLinkMap.has(linkID);
  }

  /**
   * Check if the link with specified source and target
   *  exisits in the operatorLinkMap.
   *
   * @param source the OperatorPort of the source of a link
   * @param target the OperatorPort of the target of a link
   */
  public hasLink(source: OperatorPort, target: OperatorPort): boolean {
    const links = this.getLinks().filter(
      value => isEqual(value.source, source) && isEqual(value.target, target)
    );
    if (links.length === 0) {
      return false;
    }
    return true;
  }

  /**
   * return a link with specified ID from operatorLinkMap. If not exist,
   *  throw an error.
   * @param linkID link ID
   */
  public getLinkWithID(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    if (link === undefined) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    return link;
  }

  /**
   * return a link with specified source port and target port from operatorLinkMap.
   *  If not exist, throw an error.
   * @param source the OperatorPort of the source of a link
   * @param target the OperatorPort of the target of a link
   */
  public getLink(source: OperatorPort, target: OperatorPort): OperatorLink {
    const links = this.getLinks().filter(
      value => isEqual(value.source, source) && isEqual(value.target, target)
    );

    if (links.length === 0) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} doesn't exist`);
    }
    return links[0];
  }

  /**
   * return an array of all the links existing in operatorLinkMap.
   */
  public getLinks(): OperatorLink[] {
    return Array.from(this.operatorLinkMap.values());
  }

  /**
   * This method will first check if the operator exist in the operatorIDMap.
   *  If operator exist, set its property to the new property passed.
   *
   * @param operatorID operator ID
   * @param newProperty new property to use
   */
  public changeOperatorProperty(operatorID: string, newProperty: Object) {
    const operator = this.operatorIDMap.get(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    operator.operatorProperties = newProperty;
  }

  /**
   * Checks if it's valid to add the given operator to the graph.
   * Throws an Error if it's not a valid operator because of:
   *  - duplicate operator ID
   * @param graph
   * @param operator
   */
  public static checkIsValidOperator(graph: WorkflowGraphReadonly, operator: OperatorPredicate): void {
    if (graph.hasOperator(operator.operatorID)) {
      throw new Error(`operator with ID ${operator.operatorID} already exists`);
    }
  }

  /**
   * Checks if it's valid to add the given link to the graph.
   * Throws an Error if it's not a valid link because of:
   *  - duplicate link ID
   *  - duplicate link source and target
   *  - incorrect source or target operator
   *  - incorrect source or target port
   * @param graph
   * @param link
   */
  public static checkIsValidLink(graph: WorkflowGraphReadonly, link: OperatorLink): void {
    if (graph.hasLinkWithID(link.linkID)) {
      throw new Error(`link with ID ${link.linkID} already exists`);
    }
    if (graph.hasLink(link.source, link.target)) {
      throw new Error(`link from ${link.source.operatorID}.${link.source.portID}
        to ${link.target.operatorID}.${link.target.portID} already exists`);
    }
    if (! graph.hasOperator(link.source.operatorID)) {
      throw new Error(`link's source operator ${link.source.operatorID} doesn't exist`);
    }
    if (! graph.hasOperator(link.target.operatorID)) {
      throw new Error(`link's target operator ${link.target.operatorID} doesn't exist`);
    }
    if (graph.getOperator(link.source.operatorID).outputPorts.find(
      (port) => port === link.source.portID) === undefined) {
        throw new Error(`link's source port ${link.source.portID} doesn't exist
          on output ports of the source operator ${link.source.operatorID}`);
    }
    if (graph.getOperator(link.target.operatorID).inputPorts.find(
      (port) => port === link.target.portID) === undefined) {
        throw new Error(`link's target port ${link.target.portID} doesn't exist
          on input ports of the target operator ${link.target.operatorID}`);
    }
  }

}
