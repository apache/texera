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

import Ajv from "ajv";
import type {
  OperatorPredicate,
  PortDescription,
  OperatorLink,
  PortSchema,
  OperatorPortSchemaMap,
} from "../../types/workflow";
import type { WorkflowSystemMetadata } from "./workflow-system-metadata";
import type { WorkflowState } from "../workflow-state";

// Format "{id}_{internal}" must align with the backend port-identity serializer.
function serializePortIdentity(id: number, internal: boolean = false): string {
  return `${id}_${internal}`;
}

function parseLogicalOperatorPortID(portId: string): { portNumber: number; portType: "input" | "output" } | undefined {
  const match = portId.match(/^(input|output)-(\d+)$/);
  if (!match) {
    return undefined;
  }

  const portType = match[1] as "input" | "output";
  const portNumber = parseInt(match[2]);

  return { portNumber, portType };
}

function getInputLinksByOperatorId(operatorId: string, links: OperatorLink[]): OperatorLink[] {
  return links.filter(link => link.target.operatorID === operatorId);
}

export function extractOperatorInputPortSchemaMap(
  operatorId: string,
  operator: OperatorPredicate,
  outputSchemas: Record<string, OperatorPortSchemaMap>,
  links: OperatorLink[]
): OperatorPortSchemaMap | undefined {
  const inputLinks = getInputLinksByOperatorId(operatorId, links);
  if (!inputLinks.length) return undefined;

  const inputPortSchemaMap: Record<string, PortSchema | undefined> = {};

  operator.inputPorts.forEach((_, portIndex) => {
    const portId = serializePortIdentity(portIndex, false);
    inputPortSchemaMap[portId] = undefined;

    const linksToThisPort = inputLinks.filter(link => {
      const parsedPort = parseLogicalOperatorPortID(link.target.portID);
      if (!parsedPort) return false;
      return parsedPort.portNumber === portIndex;
    });

    if (linksToThisPort.length > 0) {
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

      // Unlike the frontend, we don't flag mismatched schemas as a compilation
      // error; we just pick the first defined one.
      if (schemas.length > 0) {
        inputPortSchemaMap[portId] = schemas.find(s => s !== undefined);
      }
    }
  });

  const hasAnySchema = Object.values(inputPortSchemaMap).some(s => s !== undefined);
  return hasAnySchema ? inputPortSchemaMap : undefined;
}

interface InputPortInfo {
  displayName?: string;
  disallowMultiLinks?: boolean;
  dependencies?: { id: number; internal: boolean }[];
}

interface OutputPortInfo {
  displayName?: string;
}

function inputPortToPortDescription(portID: string, inputPortInfo: InputPortInfo): PortDescription {
  return {
    portID,
    displayName: inputPortInfo.displayName ?? "",
    disallowMultiInputs: inputPortInfo.disallowMultiLinks ?? false,
    isDynamicPort: false,
    dependencies: inputPortInfo.dependencies ?? [],
  };
}

function outputPortToPortDescription(portID: string, outputPortInfo: OutputPortInfo): PortDescription {
  return {
    portID,
    displayName: outputPortInfo.displayName ?? "",
    disallowMultiInputs: false,
    isDynamicPort: false,
  };
}

/**
 * Builds new `OperatorPredicate` instances from operator metadata.
 *
 * Given an operator type, reads the JSON schema and additional metadata from
 * `WorkflowSystemMetadata`, materializes default properties via Ajv, and
 * synthesizes input/output port descriptions so the operator is ready to
 * drop into a `WorkflowState`.
 */
export class WorkflowUtilService {
  private metadataStore: WorkflowSystemMetadata;
  private workflowState: WorkflowState;
  private ajv: Ajv;

  constructor(metadataStore: WorkflowSystemMetadata, workflowState: WorkflowState) {
    this.metadataStore = metadataStore;
    this.workflowState = workflowState;
    this.ajv = new Ajv({ useDefaults: true, strict: false });
  }

  public getNewOperatorPredicate(operatorType: string, customDisplayName?: string): OperatorPredicate {
    const jsonSchema = this.metadataStore.getSchema(operatorType);
    const additionalMetadata = this.metadataStore.getAdditionalMetadata(operatorType);

    if (!jsonSchema || !additionalMetadata) {
      throw new Error(`operatorType ${operatorType} doesn't exist in operator metadata`);
    }

    const operatorId = this.workflowState.generateOperatorId(operatorType);
    const operatorProperties: Record<string, any> = {};

    // Strip $id so Ajv doesn't warn about a duplicate schema registration.
    const { $id, ...schemaWithoutId } = jsonSchema as any;

    // Calling validate() here populates operatorProperties with a deep clone of
    // the schema defaults via Ajv's useDefaults option.
    const validate = this.ajv.compile(schemaWithoutId);
    validate(operatorProperties);

    const inputPorts: PortDescription[] = [];
    const outputPorts: PortDescription[] = [];

    const showAdvanced = false;

    const isDisabled = false;

    const displayName = customDisplayName ?? additionalMetadata.userFriendlyName;

    const dynamicInputPorts = additionalMetadata.dynamicInputPorts ?? false;
    const dynamicOutputPorts = additionalMetadata.dynamicOutputPorts ?? false;

    const inputPortInfos = additionalMetadata.inputPorts || [];
    for (let i = 0; i < inputPortInfos.length; i++) {
      const portID = "input-" + i.toString();
      const portInfo = inputPortInfos[i] as InputPortInfo;
      inputPorts.push(inputPortToPortDescription(portID, portInfo));
    }

    const outputPortInfos = additionalMetadata.outputPorts || [];
    for (let i = 0; i < outputPortInfos.length; i++) {
      const portID = "output-" + i.toString();
      const portInfo = outputPortInfos[i] as OutputPortInfo;
      outputPorts.push(outputPortToPortDescription(portID, portInfo));
    }

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
