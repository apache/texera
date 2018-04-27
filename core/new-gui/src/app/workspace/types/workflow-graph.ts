import { WorkflowGraphReadonly } from './workflow-graph-readonly';
import { OperatorLink } from './workflow-graph';
import { OperatorPort } from './operator-port';

export interface OperatorPredicate {
  operatorID: string;
  operatorType: string;
  operatorProperties: Object;
  inputPorts: string[];
  outputPorts: string[];
}

export interface OperatorLink {
  linkID: string;
  source: OperatorPort;
  target: OperatorPort;
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
    if (this.hasLink(link.source, link.target)) {
        throw new Error(`link from ${link.source.operatorID}.${link.source.portID}
        to ${link.target.operatorID}.${link.target.portID} already exists`);
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
      value => value.source === source && value.target === target
    );
    if (links.length === 0) {
      return false;
    }
    return true;
  }

  public getLinkWithID(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    if (link === undefined) {
      throw new Error(`link with ID ${linkID} does not exist`);
    }
    return link;
  }

  public getLink(source: OperatorPort, target: OperatorPort): OperatorLink {
    const links = this.getLinks().filter(
      value => value.source === source && value.target === target
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

}
