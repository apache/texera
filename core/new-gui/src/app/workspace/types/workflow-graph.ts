import { OperatorPredicate, OperatorLink, OperatorPort } from './common.interface';
import { WorkflowGraphReadonly } from './workflow-graph-readonly.interface';
import { isEqual } from 'lodash-es';



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

  public addOperator(operator: OperatorPredicate): void {
    WorkflowGraph.checkIsValidOperator(this, operator);
    this.operatorIDMap.set(operator.operatorID, operator);
  }

  public deleteOperator(operatorID: string): OperatorPredicate {
    const operator = this.operatorIDMap.get(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    this.operatorIDMap.delete(operatorID);
    return operator;
  }

  public hasOperator(operatorID: string): boolean {
    return this.operatorIDMap.has(operatorID);
  }

  public getOperator(operatorID: string): OperatorPredicate {
    const operator = this.operatorIDMap.get(operatorID);
    if (operator === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator;
  }

  public getOperators(): OperatorPredicate[] {
    return Array.from(this.operatorIDMap.values());
  }

  public addLink(link: OperatorLink): void {
    WorkflowGraph.checkIsValidLink(this, link);
    this.operatorLinkMap.set(link.linkID, link);
  }

  public deleteLinkWithID(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    if (link === undefined) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    this.operatorLinkMap.delete(linkID);
    return link;
  }

  public deleteLink(source: OperatorPort, target: OperatorPort): OperatorLink {
    if (!this.hasLink(source, target)) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} doesn't exist`);
    }
    const link = this.getLink(source, target);
    this.operatorLinkMap.delete(link.linkID);
    return link;
  }

  public hasLinkWithID(linkID: string): boolean {
    return this.operatorLinkMap.has(linkID);
  }

  public hasLink(source: OperatorPort, target: OperatorPort): boolean {
    const links = this.getLinks().filter(
      value => isEqual(value.source, source) && isEqual(value.target, target)
    );
    if (links.length === 0) {
      return false;
    }
    return true;
  }

  public getLinkWithID(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    if (link === undefined) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    return link;
  }

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

  public getLinks(): OperatorLink[] {
    return Array.from(this.operatorLinkMap.values());
  }

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
