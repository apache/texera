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

import { z } from "zod";
import { tool } from "ai";
import { OperatorMetadataService } from "../../operator-metadata/operator-metadata.service";

// Tool name constants
export const TOOL_NAME_LIST_ALL_OPERATOR_TYPES_AND_SCHEMAS = "listAllOperatorTypesAndSchemas";
export const TOOL_NAME_GET_OPERATOR_PORTS_INFO = "getOperatorPortsInfo";
export const TOOL_NAME_GET_OPERATOR_METADATA = "getOperatorMetadata";

// Whitelist of allowed operator types for the copilot
export const ALLOWED_OPERATOR_TYPES = [
  "PythonUDFV2",
  "Aggregate",
  "Projection",
  "HashJoin",
  "Sort",
  "Union",
  "Intersect",
  "CartesianProduct",
  "CSVFileScan",
] as const;

export type AllowedOperatorType = (typeof ALLOWED_OPERATOR_TYPES)[number];

/**
 * Combined tool that returns all allowed operator types with their properties schemas.
 * This reduces the number of tool calls needed and saves network bandwidth/tokens.
 */
export function createListAllOperatorTypesAndSchemasTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: TOOL_NAME_LIST_ALL_OPERATOR_TYPES_AND_SCHEMAS,
    description:
      "Get all available operator types and their properties schemas. " +
      "Returns operator types with their full properties schema for configuring operators. " +
      "Use this to understand what operators are available and how to configure them.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operatorsWithSchemas: Record<
          string,
          {
            properties: any;
            required: any;
            definitions: any;
          }
        > = {};

        for (const operatorType of ALLOWED_OPERATOR_TYPES) {
          try {
            const schema = operatorMetadataService.getOperatorSchema(operatorType);
            operatorsWithSchemas[operatorType] = {
              properties: schema.jsonSchema.properties,
              required: schema.jsonSchema.required,
              definitions: schema.jsonSchema.definitions,
            };
          } catch {
            // Skip operators that don't exist in this installation
          }
        }

        const availableTypes = Object.keys(operatorsWithSchemas);

        return {
          success: true,
          operators: operatorsWithSchemas,
          operatorTypes: availableTypes,
          count: availableTypes.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorPortsInfoTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: TOOL_NAME_GET_OPERATOR_PORTS_INFO,
    description:
      "Get input and output port information for an operator type. This is more token-efficient than getOperatorSchema and returns only port details (display names, multi-input support, etc.).",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of the operator to get port information for"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);
        const portsInfo = {
          inputPorts: schema.additionalMetadata.inputPorts,
          outputPorts: schema.additionalMetadata.outputPorts,
          dynamicInputPorts: schema.additionalMetadata.dynamicInputPorts,
          dynamicOutputPorts: schema.additionalMetadata.dynamicOutputPorts,
        };

        return {
          success: true,
          portsInfo: portsInfo,
          operatorType: args.operatorType,
          message: `Retrieved port information for operator type ${args.operatorType}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorMetadataTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: TOOL_NAME_GET_OPERATOR_METADATA,
    description:
      "Get semantic metadata for an operator type, including user-friendly name, description, operator group, and capabilities. This is very useful to understand the semantics and purpose of each operator type - what it does, how it works, and what kind of data transformation it performs.",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of the operator to get metadata for"),
    }),
    execute: async (args: { operatorType: string; signal?: AbortSignal }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);

        const metadata = schema.additionalMetadata;
        return {
          success: true,
          metadata: metadata,
          operatorType: args.operatorType,
          operatorVersion: schema.operatorVersion,
          message: `Retrieved metadata for operator type ${args.operatorType}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
