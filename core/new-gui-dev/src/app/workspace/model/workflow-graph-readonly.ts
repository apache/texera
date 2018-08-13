interface WorkflowGraphReadonly {

    hasOperator(operatorID: string): boolean;

    getOperator(operatorID: string): OperatorPredicate;

    getOperators(): OperatorPredicate[];

    hasLink(linkID: string): boolean;

    getLinks(): OperatorLink[];

}
