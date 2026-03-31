/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * WorkflowUtilService - Utility class for creating operator predicates.
 * This mirrors the frontend's WorkflowUtilService to ensure operators
 * are created with the correct format.
 */

import Ajv from "ajv";
import type {
  OperatorPredicate,
  PortDescription,
  OperatorLink,
  PortSchema,
  OperatorPortSchemaMap,
} from "../types/workflow";
import type { OperatorMetadataStore } from "../tools/metadata-tools";
import type { WorkflowState } from "./workflow-state";

// ============================================================================
// Port Identity Serialization Utilities
// (Mirrors frontend's port-identity-serde.ts and logical-operator-port-serde.ts)
// ============================================================================

/**
 * Serializes a port identity to a string in the format "{id}_{internal}"
 * This is aligned with the backend serializer.
 * @param id The port index (0, 1, 2, etc.)
 * @param internal Whether this is an internal port (typically false)
 * @returns A string representation (e.g., "0_false", "1_false")
 */
export function serializePortIdentity(id: number, internal: boolean = false): string {
  return `${id}_${internal}`;
}

/**
 * Extracts the port index from a port ID string.
 * @param portId Port ID like "input-0", "output-1", etc.
 * @returns undefined if the portId is invalid; port number and the type of the port will be returned
 */
export function parseLogicalOperatorPortID(
  portId: string
): { portNumber: number; portType: "input" | "output" } | undefined {
  const match = portId.match(/^(input|output)-(\d+)$/);
  if (!match) {
    return undefined;
  }

  const portType = match[1] as "input" | "output";
  const portNumber = parseInt(match[2]);

  return { portNumber, portType };
}

// ============================================================================
// Schema Comparison Utilities
// (Mirrors frontend's workflow-compilation-utils.ts)
// ============================================================================

/**
 * Deep equality check for two values (simple implementation without lodash)
 */
function isEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;
  if (typeof a !== "object") return a === b;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((val, i) => isEqual(val, b[i]));
  }

  if (Array.isArray(a) || Array.isArray(b)) return false;

  const keysA = Object.keys(a as object);
  const keysB = Object.keys(b as object);
  if (keysA.length !== keysB.length) return false;

  return keysA.every(key => isEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]));
}

/**
 * Checks if all PortSchemas in an array are equal to each other.
 * Requires either all schemas to be undefined, or all to be defined and equal.
 *
 * @param schemas Array of PortSchemas to compare (can contain undefined values)
 * @returns true if all schemas are equal, false otherwise
 */
export function areAllPortSchemasEqual(schemas: (PortSchema | undefined)[]): boolean {
  if (schemas.length <= 1) {
    return true;
  }
  return schemas.every(schema => isEqual(schemas[0], schema));
}

// ============================================================================
// Schema Extraction Utilities
// (Mirrors frontend's WorkflowCompilingService.extractOperatorInputPortSchemaMap)
// ============================================================================

/**
 * Gets all links that have the given operator as their target (input links).
 */
export function getInputLinksByOperatorId(operatorId: string, links: OperatorLink[]): OperatorLink[] {
  return links.filter(link => link.target.operatorID === operatorId);
}

/**
 * Extracts input schema per port for an operator by looking at the output schemas
 * of operators that are connecting to it.
 *
 * @param operatorId The target operator's ID
 * @param operator The operator predicate (to know its input ports)
 * @param outputSchemas Map of operator IDs to their output schemas per output port
 * @param links All links in the workflow
 * @returns The extracted input schema per port or undefined
 */
export function extractOperatorInputPortSchemaMap(
  operatorId: string,
  operator: OperatorPredicate,
  outputSchemas: Record<string, OperatorPortSchemaMap>,
  links: OperatorLink[]
): OperatorPortSchemaMap | undefined {
  const inputLinks = getInputLinksByOperatorId(operatorId, links);
  if (!inputLinks.length) return undefined;

  const inputPortSchemaMap: Record<string, PortSchema | undefined> = {};

  // Initialize all input ports with undefined schema
  operator.inputPorts.forEach((_, portIndex) => {
    const portId = serializePortIdentity(portIndex, false);
    inputPortSchemaMap[portId] = undefined;

    // Find all links that connect to this input port
    const linksToThisPort = inputLinks.filter(link => {
      const parsedPort = parseLogicalOperatorPortID(link.target.portID);
      if (!parsedPort) return false;
      return parsedPort.portNumber === portIndex;
    });

    if (linksToThisPort.length > 0) {
      // Collect schemas from all links to this port
      const schemas: (PortSchema | undefined)[] = linksToThisPort.map(link => {
        const sourcePortSchemaMap = outputSchemas[link.source.operatorID];
        if (!sourcePortSchemaMap) {
          return undefined;
        }

        const outputPort = parseLogicalOperatorPortID(link.source.portID);
        if (!outputPort) {
          return undefined;
        }

        return sourcePortSchemaMap[serializePortIdentity(outputPort.portNumber, false)];
      });

      // Check if all schemas are the same
      // Note: Frontend sets compilation error if schemas differ; we skip that for now
      // and just use the first valid schema
      if (schemas.length > 0) {
        // Use the first defined schema, or undefined if all are undefined
        inputPortSchemaMap[portId] = schemas.find(s => s !== undefined);
      }
    }
  });

  // Return undefined if no schemas were set
  const hasAnySchema = Object.values(inputPortSchemaMap).some(s => s !== undefined);
  return hasAnySchema ? inputPortSchemaMap : undefined;
}

/**
 * Input port info from operator metadata
 */
interface InputPortInfo {
  displayName?: string;
  allowMultiLinks?: boolean;
  dependencies?: { id: number; internal: boolean }[];
}

/**
 * Output port info from operator metadata
 */
interface OutputPortInfo {
  displayName?: string;
}

/**
 * Convert input port info to port description
 */
function inputPortToPortDescription(portID: string, inputPortInfo: InputPortInfo): PortDescription {
  return {
    portID,
    displayName: inputPortInfo.displayName ?? "",
    allowMultiInputs: inputPortInfo.allowMultiLinks ?? false,
    isDynamicPort: false,
    dependencies: inputPortInfo.dependencies ?? [],
  };
}

/**
 * Convert output port info to port description
 */
function outputPortToPortDescription(portID: string, outputPortInfo: OutputPortInfo): PortDescription {
  return {
    portID,
    displayName: outputPortInfo.displayName ?? "",
    allowMultiInputs: false,
    isDynamicPort: false,
  };
}

/**
 * WorkflowUtilService provides utilities for creating operator predicates.
 * Mirrors the frontend's WorkflowUtilService to ensure consistent operator creation.
 */
export class WorkflowUtilService {
  private metadataStore: OperatorMetadataStore;
  private workflowState: WorkflowState;
  private ajv: Ajv;

  constructor(metadataStore: OperatorMetadataStore, workflowState: WorkflowState) {
    this.metadataStore = metadataStore;
    this.workflowState = workflowState;
    this.ajv = new Ajv({ useDefaults: true, strict: false });
  }

  /**
   * Create a new operator predicate with default properties.
   * This method mirrors the frontend's getNewOperatorPredicate() exactly.
   *
   * @param operatorType - The type of operator to create
   * @param customDisplayName - Optional custom display name for the operator
   * @returns A new OperatorPredicate with all required fields
   */
  public getNewOperatorPredicate(operatorType: string, customDisplayName?: string): OperatorPredicate {
    const jsonSchema = this.metadataStore.getSchema(operatorType);
    const additionalMetadata = this.metadataStore.getAdditionalMetadata(operatorType);

    if (!jsonSchema || !additionalMetadata) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    const operatorId = this.workflowState.generateOperatorId(operatorType);
    const operatorProperties: Record<string, any> = {};

    // Remove the ID field for the schema to prevent warning messages from Ajv
    const { $id, ...schemaWithoutId } = jsonSchema as any;

    // Value inserted in the data will be the deep clone of the default in the schema
    const validate = this.ajv.compile(schemaWithoutId);
    validate(operatorProperties);

    const inputPorts: PortDescription[] = [];
    const outputPorts: PortDescription[] = [];

    // By default, the operator will not show advanced option in the properties to the user
    const showAdvanced = false;

    // By default, the operator is not disabled
    const isDisabled = false;

    // Use provided customDisplayName or default to the user friendly name from schema
    const displayName = customDisplayName ?? additionalMetadata.userFriendlyName;

    const dynamicInputPorts = additionalMetadata.dynamicInputPorts ?? false;
    const dynamicOutputPorts = additionalMetadata.dynamicOutputPorts ?? false;

    // Build input ports
    const inputPortInfos = additionalMetadata.inputPorts || [];
    for (let i = 0; i < inputPortInfos.length; i++) {
      const portID = "input-" + i.toString();
      const portInfo = inputPortInfos[i] as InputPortInfo;
      inputPorts.push(inputPortToPortDescription(portID, portInfo));
    }

    // Build output ports
    const outputPortInfos = additionalMetadata.outputPorts || [];
    for (let i = 0; i < outputPortInfos.length; i++) {
      const portID = "output-" + i.toString();
      const portInfo = outputPortInfos[i] as OutputPortInfo;
      outputPorts.push(outputPortToPortDescription(portID, portInfo));
    }

    // Get operator version from metadata (or use "N/A" as fallback)
    const operatorVersion = (additionalMetadata as any).operatorVersion ?? "N/A";

    return {
      operatorID: operatorId,
      operatorType,
      operatorVersion,
      operatorProperties,
      inputPorts,
      outputPorts,
      showAdvanced,
      isDisabled,
      customDisplayName: displayName,
      dynamicInputPorts,
      dynamicOutputPorts,
    };
  }
}
