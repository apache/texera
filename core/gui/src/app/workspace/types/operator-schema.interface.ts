import { CustomJSONSchema7 } from "./custom-json-schema.interface";

/**
 * This file contains multiple type declarations related to operator schema.
 * These type declarations should be the same with the backend API.
 *
 * This file include a sample mock data:
 *   workspace/service/operator-metadata/mock-operator-metadata.data.ts
 *
 */

export interface InputPortInfo
  extends Readonly<{
    displayName?: string;
    allowMultiLinks?: boolean;
    dependencies?: { id: number; internal: boolean }[];
  }> {}

export interface OutputPortInfo
  extends Readonly<{
    displayName?: string;
  }> {}

export interface OperatorAdditionalMetadata
  extends Readonly<{
    userFriendlyName: string;
    operatorGroupName: string;
    operatorDescription?: string;
    inputPorts: ReadonlyArray<InputPortInfo>;
    outputPorts: ReadonlyArray<OutputPortInfo>;
    dynamicInputPorts?: boolean;
    dynamicOutputPorts?: boolean;
    supportReconfiguration?: boolean;
    allowPortCustomization?: boolean;
  }> {}

export interface OperatorSchema
  extends Readonly<{
    operatorType: string;
    jsonSchema: Readonly<CustomJSONSchema7>;
    additionalMetadata: OperatorAdditionalMetadata;
    operatorVersion: string;
  }> {}

export interface GroupInfo
  extends Readonly<{
    groupName: string;
    children?: GroupInfo[] | null;
  }> {}

export interface OperatorMetadata
  extends Readonly<{
    operators: ReadonlyArray<OperatorSchema>;
    groups: ReadonlyArray<GroupInfo>;
  }> {}
