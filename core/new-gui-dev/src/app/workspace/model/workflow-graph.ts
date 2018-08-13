export class WorkflowGraph {

    operatorIDMap = new Map<string, OperatorPredicate>();
    operatorLinkMap = new Map<string, OperatorLink>();

    constructor(
        operatorPredicates: OperatorPredicate[],
        operatorLinks: OperatorLink[]
    ) {
        operatorPredicates.forEach(op => this.operatorIDMap.set(op.operatorID, op));
        operatorLinks.forEach(link => this.operatorLinkMap.set(link.linkID, link));
    }

    hasOperator(operatorID: string): boolean {
        return this.operatorIDMap.has(operatorID);
    }

    getOperator(operatorID: string): OperatorPredicate {
        return this.operatorIDMap.get(operatorID);
    }

    getOperators(): OperatorPredicate[] {
        return Array.from(this.operatorIDMap.values());
    }

    addOperator(operator: OperatorPredicate): void {
        this.operatorIDMap.set(operator.operatorID, operator);
    }

    deleteOperator(operatorID: string): OperatorPredicate {
        const operator = this.operatorIDMap.get(operatorID);
        this.operatorIDMap.delete(operatorID);
        return operator;
    }

    changeOperatorProperty(operatorID: string, newProperty: Object) {
        this.operatorIDMap.get(operatorID).operatorProperties = newProperty;
    }

    hasLink(linkID: string): boolean {
        return this.operatorLinkMap.has(linkID);
    }

    getLinks(): OperatorLink[] {
        return Array.from(this.operatorLinkMap.values());
    }

    addLink(operatorLink: OperatorLink): void {
        this.operatorLinkMap.set(operatorLink.linkID, operatorLink);
    }

    deleteLink(linkID: string): OperatorLink {
        const link = this.operatorLinkMap.get(linkID);
        this.operatorLinkMap.delete(linkID);
        return link;
    }

    changeLink(operatorLink: OperatorLink): void {
        this.operatorLinkMap.set(operatorLink.linkID, operatorLink);
    }
}
