import { JSONSchema7 } from "json-schema";
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
    allowMultiInputs?: boolean;
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
    groupOrder: number;
  }> {}

export interface OperatorMetadata
  extends Readonly<{
    operators: ReadonlyArray<OperatorSchema>;
    groups: ReadonlyArray<GroupInfo>;
  }> {}
