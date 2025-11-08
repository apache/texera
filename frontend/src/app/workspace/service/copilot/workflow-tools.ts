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

const TIMEOUT_MS = 120000;

export function toolWithTimeout(toolConfig: any): any {
  const originalExecute = toolConfig.execute;
  return {
    ...toolConfig,
    execute: async (args: any) => {
      const controller = new AbortController();
      const timeout = new Promise((_, reject) => {
        setTimeout(() => {
          controller.abort();
          reject(new Error("timeout"));
        }, TIMEOUT_MS);
      });

      try {
        return await Promise.race([originalExecute({ ...args, signal: controller.signal }), timeout]);
      } catch (error: any) {
        if (error.message === "timeout") {
          return { success: false, error: "Tool execution timeout - exceeded 2 minutes" };
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
        return {
          success: true,
          operators: operators.map(op => ({
            operatorId: op.operatorID,
            operatorType: op.operatorType,
            customDisplayName: op.customDisplayName,
          })),
          count: operators.length,
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
        return { success: true, links, count: links.length };
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
        return { success: true, operatorTypes, count: operatorTypes.length };
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
      "Get detailed information about a specific operator in the current workflow, including input/output schemas",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        const inputSchema = workflowCompilingService.getOperatorInputSchemaMap(args.operatorId) || {};
        const outputSchema = workflowCompilingService.getOperatorOutputSchemaMap(args.operatorId) || {};

        return {
          success: true,
          operator,
          inputSchema,
          outputSchema,
        };
      } catch (error: any) {
        return { success: false, error: error.message || `Operator ${args.operatorId} not found` };
      }
    },
  });
}

export function createGetOperatorPropertiesSchemaTool(operatorMetadataService: OperatorMetadataService) {
  return tool({
    name: "getOperatorPropertiesSchema",
    description: "Get properties schema for an operator type. Use before setting operator properties",
    inputSchema: z.object({
      operatorType: z.string().describe("Operator type"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);
        return {
          success: true,
          propertiesSchema: {
            properties: schema.jsonSchema.properties,
            required: schema.jsonSchema.required,
            definitions: schema.jsonSchema.definitions,
          },
          operatorType: args.operatorType,
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
    description: "Get input/output port information for an operator type",
    inputSchema: z.object({
      operatorType: z.string().describe("Operator type"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);
        return {
          success: true,
          portsInfo: {
            inputPorts: schema.additionalMetadata.inputPorts,
            outputPorts: schema.additionalMetadata.outputPorts,
            dynamicInputPorts: schema.additionalMetadata.dynamicInputPorts,
            dynamicOutputPorts: schema.additionalMetadata.dynamicOutputPorts,
          },
          operatorType: args.operatorType,
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
    description: "Get semantic metadata for an operator type (name, description, group, capabilities)",
    inputSchema: z.object({
      operatorType: z.string().describe("Operator type"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        const schema = operatorMetadataService.getOperatorSchema(args.operatorType);
        return {
          success: true,
          metadata: schema.additionalMetadata,
          operatorType: args.operatorType,
          operatorVersion: schema.operatorVersion,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
