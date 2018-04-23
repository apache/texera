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


export class WorkflowGraph {

  private operatorIDMap = new Map<string, OperatorPredicate>();
  private operatorLinkMap = new Map<string, OperatorLink>();

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = []
  ) {
    operatorPredicates.forEach(op => this.operatorIDMap.set(op.operatorID, op));
    operatorLinks.forEach(link => this.operatorLinkMap.set(link.linkID, link));
  }

  public hasOperator(operatorID: string): boolean {
    return this.operatorIDMap.has(operatorID);
  }

  public getOperator(operatorID: string): OperatorPredicate {
    return this.operatorIDMap.get(operatorID);
  }

  public getOperators(): OperatorPredicate[] {
    return Array.from(this.operatorIDMap.values());
  }

  public hasLinkWithID(linkID: string): boolean {
    return this.operatorLinkMap.has(linkID);
  }

  public hasLink(sourceOperator: string, sourcePort: string, targetOperator: string, targetPort: string): boolean {
    let linkFound = false;
    this.operatorLinkMap.forEach(
      (value, key, map) => {
        const isEqual = value.sourceOperator === sourceOperator
          && value.sourcePort === sourcePort
          && value.targetOperator === targetOperator
          && value.targetPort === targetPort;
        if (isEqual) {
          linkFound = true;
        }
      }
    );
    return linkFound;
  }

  public getLink(linkID: string): OperatorLink {
    return this.operatorLinkMap.get(linkID);
  }

  public getLinks(): OperatorLink[] {
    return Array.from(this.operatorLinkMap.values());
  }

  public addOperator(operator: OperatorPredicate): void {
    this.operatorIDMap.set(operator.operatorID, operator);
  }

  public deleteOperator(operatorID: string): OperatorPredicate {
    const operator = this.operatorIDMap.get(operatorID);
    this.operatorIDMap.delete(operatorID);
    return operator;
  }

  public changeOperatorProperty(operatorID: string, newProperty: Object) {
    this.operatorIDMap.get(operatorID).operatorProperties = newProperty;
  }

  public addLink(operatorLink: OperatorLink): void {
    this.operatorLinkMap.set(operatorLink.linkID, operatorLink);
  }

  public deleteLink(linkID: string): OperatorLink {
    const link = this.operatorLinkMap.get(linkID);
    this.operatorLinkMap.delete(linkID);
    return link;
  }

  public changeLink(link: OperatorLink): void {
    this.deleteLink(link.linkID);
    this.addLink(link);
  }

}
