interface OperatorAdditionalMetadata {
    userFriendlyName: string;
    numInputPorts: number;
    numOutputPorts: number;
    advancedOptions?: string[];
    operatorGroupName?: string;
    operatorDescription?: string;
}

interface OperatorSchema {
    operatorType: string;
    jsonSchema: Object;
    additionalMetadata: OperatorAdditionalMetadata;
}

interface GroupOrder {
    groupName: string;
    groupOrder: number;
}

interface OperatorMetadata {
    operators: OperatorSchema[];
    groups: GroupOrder[];
}
