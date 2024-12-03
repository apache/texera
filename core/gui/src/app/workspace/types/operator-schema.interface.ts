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

export function areOperatorSchemasEqual(schema1: OperatorSchema, schema2: OperatorSchema): boolean {
  if (schema1.operatorType !== schema2.operatorType || schema1.operatorVersion !== schema2.operatorVersion) {
    return false;
  }

  // Compare jsonSchema using a JSON string comparison
  if (JSON.stringify(schema1.jsonSchema) !== JSON.stringify(schema2.jsonSchema)) {
    return false;
  }

  // Compare additionalMetadata by checking fields explicitly
  const meta1 = schema1.additionalMetadata;
  const meta2 = schema2.additionalMetadata;

  if (
    meta1.userFriendlyName !== meta2.userFriendlyName ||
    meta1.operatorGroupName !== meta2.operatorGroupName ||
    meta1.operatorDescription !== meta2.operatorDescription ||
    meta1.supportReconfiguration !== meta2.supportReconfiguration ||
    meta1.allowPortCustomization !== meta2.allowPortCustomization
  ) {
    return false;
  }

  // Compare inputPorts and outputPorts
  if (meta1.inputPorts.length !== meta2.inputPorts.length || meta1.outputPorts.length !== meta2.outputPorts.length) {
    return false;
  }

  // Check each port info for equality
  for (let i = 0; i < meta1.inputPorts.length; i++) {
    const port1 = meta1.inputPorts[i];
    const port2 = meta2.inputPorts[i];

    if (port1.displayName !== port2.displayName || port1.allowMultiLinks !== port2.allowMultiLinks) {
      return false;
    }
  }

  for (let i = 0; i < meta1.outputPorts.length; i++) {
    const port1 = meta1.outputPorts[i];
    const port2 = meta2.outputPorts[i];

    if (port1.displayName !== port2.displayName) {
      return false;
    }
  }

  // If all checks pass, they are equal
  return true;
}
