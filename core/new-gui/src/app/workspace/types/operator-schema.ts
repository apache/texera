/**
 * This file contains multiple type declarations related to operator schema.
 * These type declarations should be the same with the backend API.
 */

export interface OperatorAdditionalMetadata {
    userFriendlyName: string;
    numInputPorts: number;
    numOutputPorts: number;
    advancedOptions?: string[];
    operatorGroupName?: string;
    operatorDescription?: string;
}

export interface OperatorSchema {
    operatorType: string;
    jsonSchema: Object;
    additionalMetadata: OperatorAdditionalMetadata;
}

export interface GroupInfo {
    groupName: string;
    groupOrder: number;
}

export interface OperatorMetadata {
    operators: OperatorSchema[];
    groups: GroupInfo[];
}
