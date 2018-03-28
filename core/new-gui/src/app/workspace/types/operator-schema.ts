/**
 * This file contains multiple type declarations related to operator schema.
 * These type declarations should be the same with the backend API.
 */

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

interface GroupInfo {
    groupName: string;
    groupOrder: number;
}

interface OperatorMetadata {
    operators: OperatorSchema[];
    groups: GroupInfo[];
}
