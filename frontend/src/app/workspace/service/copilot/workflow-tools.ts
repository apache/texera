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

export function createListOperatorIdsTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listOperatorIds",
    description: "Get all operator IDs in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const operators = workflowActionService.getTexeraGraph().getAllOperators();
        const operatorIds = operators.map(op => op.operatorID);

        return {
          success: true,
          operatorIds: operatorIds,
          count: operatorIds.length,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createListLinksTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "listLinks",
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

export function createGetOperatorTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "getOperator",
    description: "Get detailed information about a specific operator in the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);

        return {
          success: true,
          operator: operator,
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

export function createGetOperatorPropertiesSchemaTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService
) {
  return tool({
    name: "getOperatorPropertiesSchema",
    description:
      "Get only the properties schema for an operator. Use this before setting operator properties.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get properties schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);
        const propertiesSchema = {
          properties: schema.jsonSchema.properties,
          required: schema.jsonSchema.required,
          definitions: schema.jsonSchema.definitions,
        };

        return {
          success: true,
          propertiesSchema: propertiesSchema,
          operatorType: operator.operatorType,
          message: `Retrieved properties schema for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorPortsInfoTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService
) {
  return tool({
    name: "getOperatorPortsInfo",
    description:
      "Get input and output port information for an operator. This is more token-efficient than getOperatorSchema and returns only port details (display names, multi-input support, etc.).",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get port information for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);
        const portsInfo = {
          inputPorts: schema.additionalMetadata.inputPorts,
          outputPorts: schema.additionalMetadata.outputPorts,
          dynamicInputPorts: schema.additionalMetadata.dynamicInputPorts,
          dynamicOutputPorts: schema.additionalMetadata.dynamicOutputPorts,
        };

        return {
          success: true,
          portsInfo: portsInfo,
          operatorType: operator.operatorType,
          message: `Retrieved port information for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorMetadataTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService
) {
  return tool({
    name: "getOperatorMetadata",
    description:
      "Get semantic metadata for an operator, including user-friendly name, description, operator group, and capabilities. This is very useful to understand the semantics and purpose of each operator - what it does, how it works, and what kind of data transformation it performs.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get metadata for"),
    }),
    execute: async (args: { operatorId: string; signal?: AbortSignal }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        const metadata = schema.additionalMetadata;
        return {
          success: true,
          metadata: metadata,
          operatorType: operator.operatorType,
          operatorVersion: schema.operatorVersion,
          message: `Retrieved metadata for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorInputSchemaTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: "getOperatorInputSchema",
    description:
      "Get the input schema for an operator, which shows what columns/attributes are available from upstream operators. This is determined by workflow compilation and schema propagation.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get input schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const inputSchemaMap = workflowCompilingService.getOperatorInputSchemaMap(args.operatorId);

        if (!inputSchemaMap) {
          return {
            success: true,
            inputSchema: null,
            message: `Operator ${args.operatorId} has no input schema (may be a source operator or not connected)`,
          };
        }

        return {
          success: true,
          inputSchema: inputSchemaMap,
          message: `Retrieved input schema for operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

export function createGetOperatorOutputSchemaTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: "getOperatorOutputSchema",
    description:
      "Get the output schema for an operator, which shows what columns/attributes this operator produces. This is determined by workflow compilation and shows the schema that will be available to downstream operators.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get output schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const outputSchemaMap = workflowCompilingService.getOperatorOutputSchemaMap(args.operatorId);

        if (!outputSchemaMap) {
          return {
            success: true,
            outputSchema: null,
            message: `Operator ${args.operatorId} has no output schema (workflow may not be compiled yet or operator has errors)`,
          };
        }

        return {
          success: true,
          outputSchema: outputSchemaMap,
          message: `Retrieved output schema for operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

