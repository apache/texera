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
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";

const TOOL_TIMEOUT_MS = 120000;

export function toolWithTimeout(toolConfig: any): any {
  const originalExecute = toolConfig.execute;

  return {
    ...toolConfig,
    execute: async (args: any) => {
      const abortController = new AbortController();

      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          abortController.abort();
          reject(new Error("timeout"));
        }, TOOL_TIMEOUT_MS);
      });

      try {
        const argsWithSignal = { ...args, signal: abortController.signal };
        return await Promise.race([originalExecute(argsWithSignal), timeoutPromise]);
      } catch (error: any) {
        if (error.message === "timeout") {
          return {
            success: false,
            error: "Tool execution timeout - operation took longer than 2 minutes. Please try again later.",
          };
        }
        throw error;
      }
    },
  };
}

export function createListOperatorsInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listOperatorsInCurrentWorkflow",
    description: "Get all operator IDs, types and custom names in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        const operatorList = operators.map(op => ({
          operatorId: op.operatorID,
          operatorType: op.operatorType,
          customDisplayName: op.customDisplayName,
        }));

        return {
          success: true,
          operators: operatorList,
          count: operatorList.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createListLinksInCurrentWorkflowTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listLinksInCurrentWorkflow",
    description: "Get all links in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const links = workflowActionService.getTexeraGraph().getAllLinks();
        return {
          success: true,
          links: links,
          count: links.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createListAllOperatorTypesTool(workflowUtilService: WorkflowUtilService) {
  return tool({
    name: "listAllOperatorTypes",
    description: "Get all available operator types in the system",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operatorTypes = workflowUtilService.getOperatorTypeList();
        return {
          success: true,
          operatorTypes: operatorTypes,
          count: operatorTypes.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorInCurrentWorkflowTool(
  workflowActionService: WorkflowActionService,
  workflowCompilingService: WorkflowCompilingService
) {
  return tool({
    name: "getOperatorInCurrentWorkflow",
    description:
      "Get detailed information about a specific operator in the current workflow, including its input and output schemas",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);

        // Get input schema (empty map if not available)
        const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(args.operatorId);
        const inputSchema = inputSchemaMap || {};

        // Get output schema (empty map if not available)
        const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(args.operatorId);
        const outputSchema = outputSchemaMap || {};

        return {
          success: true,
          operator: operator,
          inputSchema: inputSchema,
          outputSchema: outputSchema,
          message: `Retrieved operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return {
          success: false,
          error: error.message || `Operator ${args.operatorId} not found`,
        };
      }
    },
  });
}

export function createGetOperatorPropertiesSchemaTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: "getOperatorPropertiesSchema",
    description: "Get only the properties schema for an operator type. Use this before setting operator properties.",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of the operator to get properties schema for"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);
        const propertiesSchema = {
          properties: schema.jsonSchema.properties,
          required: schema.jsonSchema.required,
          definitions: schema.jsonSchema.definitions,
        };

        return {
          success: true,
          propertiesSchema: propertiesSchema,
          operatorType: args.operatorType,
          message: `Retrieved properties schema for operator type ${args.operatorType}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorPortsInfoTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: "getOperatorPortsInfo",
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
    name: "getOperatorMetadata",
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
