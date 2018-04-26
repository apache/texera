import { WorkflowGraphReadonly } from './workflow-graph-readonly';
import { OperatorLink } from './workflow-graph';

export interface OperatorPredicate {
  operatorID: string;
  operatorType: string;
  operatorProperties: Object;
  inputPorts: string[];
  outputPorts: string[];
}

export interface OperatorLink {
  linkID: string;
  sourceOperator: string;
  sourcePort: string;
  targetOperator: string;
  targetPort: string;
}


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
    if (this.hasOperator(operator.operatorID)) {
      throw new Error(`operator with ID ${operator.operatorID} already exists`);
    }
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
    if (this.hasLinkWithID(link.linkID)) {
      throw new Error(`link with ID ${link.linkID} already exists`);
    }
    if (this.hasLink(link.sourceOperator, link.sourcePort, link.targetOperator, link.targetPort)) {
      throw new Error(`link from ${link.sourceOperator}.${link.sourcePort}
        to ${link.targetOperator}.${link.targetPort} already exists`);
    }
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

  public deleteLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): OperatorLink {
    if (!this.hasLink(sourceOperator, sourcePort, targetOperator, targetPort)) {
      throw new Error(`link from ${sourceOperator}.${sourcePort} to ${targetOperator}.${targetPort} doesn't exist`);
    }
    const link = this.getLink(sourceOperator, sourcePort, targetOperator, targetPort);
    this.operatorLinkMap.delete(link.linkID);
    return link;
  }

  public hasLinkWithID(linkID: string): boolean {
    return this.operatorLinkMap.has(linkID);
  }

  public hasLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): boolean {
    const links = this.getLinks().filter(
      value => value.sourceOperator === sourceOperator && value.sourcePort === sourcePort
        && value.targetOperator === targetOperator && value.targetPort === targetPort
    );

    if (links.length === 1) {
      return true;
    } else if (links.length === 0) {
      return false;
    } else {
      // duplicate links found, this should never happen
      throw new Error(`find multiple duplicate links
        from ${sourceOperator}.${sourcePort} to ${targetOperator}.${targetPort},
        workflow graph is in inconsistent state.`);
    }
  }

  public getLinkWithID(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    if (link === undefined) {
      throw new Error(`link with ID ${linkID} does not exist`);
    }
    return link;
  }

  public getLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): OperatorLink {
    const links = this.getLinks().filter(
      value => value.sourceOperator === sourceOperator && value.sourcePort === sourcePort
        && value.targetOperator === targetOperator && value.targetPort === targetPort
    );

    if (links.length === 1) {
      return links[0];
    } else if (links.length === 0) {
      throw new Error(`link from ${sourceOperator}.${sourcePort} to ${targetOperator}.${targetPort} doesn't exist`);
    } else {
      // duplicate links found, this should never happen
      throw new Error(`find multiple duplicate links
        from ${sourceOperator}.${sourcePort} to ${targetOperator}.${targetPort},
        workflow graph is in inconsistent state.`);
    }
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

}
