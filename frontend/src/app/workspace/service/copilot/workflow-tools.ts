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
import { OperatorLink } from "../../types/workflow-common.interface";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { CopilotCoeditorService } from "./copilot-coeditor.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";

/**
 * Create addOperator tool for adding a new operator to the workflow
 */
export function createAddOperatorTool(
  workflowActionService: WorkflowActionService,
  workflowUtilService: WorkflowUtilService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "addOperator",
    description: "Add a new operator to the workflow",
    inputSchema: z.object({
      operatorType: z.string().describe("Type of operator (e.g., 'CSVSource', 'Filter', 'Aggregate')"),
    }),
    execute: async (args: { operatorType: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Validate operator type exists
        if (!operatorMetadataService.operatorTypeExists(args.operatorType)) {
          return {
            success: false,
            error: `Unknown operator type: ${args.operatorType}. Use listOperatorTypes tool to see available types.`,
          };
        }

        // Get a new operator predicate with default settings
        const operator = workflowUtilService.getNewOperatorPredicate(args.operatorType);

        // Calculate a default position (can be adjusted by auto-layout later)
        const existingOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const defaultX = 100 + (existingOperators.length % 5) * 200;
        const defaultY = 100 + Math.floor(existingOperators.length / 5) * 150;
        const position = { x: defaultX, y: defaultY };

        // Add the operator to the workflow first
        workflowActionService.addOperator(operator, position);

        // Show copilot is adding this operator (after it's added to graph)
        setTimeout(() => {
          copilotCoeditor.showEditingOperator(operator.operatorID);
          copilotCoeditor.highlightOperators([operator.operatorID]);
        }, 100);

        return {
          success: true,
          operatorId: operator.operatorID,
          message: `Added ${args.operatorType} operator to workflow`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create addLink tool for connecting two operators
 */
export function createAddLinkTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "addLink",
    description: "Connect two operators with a link",
    inputSchema: z.object({
      sourceOperatorId: z.string().describe("ID of the source operator"),
      sourcePortId: z.string().optional().describe("Port ID on source operator (e.g., 'output-0')"),
      targetOperatorId: z.string().describe("ID of the target operator"),
      targetPortId: z.string().optional().describe("Port ID on target operator (e.g., 'input-0')"),
    }),
    execute: async (args: {
      sourceOperatorId: string;
      sourcePortId?: string;
      targetOperatorId: string;
      targetPortId?: string;
    }) => {
      try {
        // Default port IDs if not specified
        const sourcePId = args.sourcePortId || "output-0";
        const targetPId = args.targetPortId || "input-0";

        const link: OperatorLink = {
          linkID: `link_${Date.now()}`,
          source: {
            operatorID: args.sourceOperatorId,
            portID: sourcePId,
          },
          target: {
            operatorID: args.targetOperatorId,
            portID: targetPId,
          },
        };

        workflowActionService.addLink(link);

        return {
          success: true,
          linkId: link.linkID,
          message: `Connected ${args.sourceOperatorId}:${sourcePId} to ${args.targetOperatorId}:${targetPId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create listOperators tool for getting all operators in the workflow
 */
export function createListOperatorsTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "listOperators",
    description: "Get all operators in the current workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        const operators = workflowActionService.getTexeraGraph().getAllOperators();

        // Highlight all operators to show copilot is inspecting them
        const operatorIds = operators.map(op => op.operatorID);
        copilotCoeditor.highlightOperators(operatorIds);

        return {
          success: true,
          operators: operators,
          count: operators.length,
        };
      } catch (error: any) {
        // Can't clear highlights without operator IDs
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create listLinks tool for getting all links in the workflow
 */
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

/**
 * Create listOperatorTypes tool for getting all available operator types
 */
export function createListOperatorTypesTool(workflowUtilService: WorkflowUtilService) {
  return tool({
    name: "listOperatorTypes",
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

/**
 * Create getOperator tool for getting detailed information about a specific operator
 */
export function createGetOperatorTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperator",
    description: "Get detailed information about a specific operator in the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to retrieve"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is viewing this operator
        copilotCoeditor.showEditingOperator(args.operatorId);
        copilotCoeditor.highlightOperators([args.operatorId]);

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

/**
 * Create deleteOperator tool for removing an operator from the workflow
 */
export function createDeleteOperatorTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "deleteOperator",
    description: "Delete an operator from the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to delete"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is editing this operator before deletion
        copilotCoeditor.showEditingOperator(args.operatorId);

        workflowActionService.deleteOperator(args.operatorId);

        return {
          success: true,
          message: `Deleted operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create deleteLink tool for removing a link from the workflow
 */
export function createDeleteLinkTool(workflowActionService: WorkflowActionService) {
  return tool({
    name: "deleteLink",
    description: "Delete a link between two operators in the workflow by link ID",
    inputSchema: z.object({
      linkId: z.string().describe("ID of the link to delete"),
    }),
    execute: async (args: { linkId: string }) => {
      try {
        workflowActionService.deleteLinkWithID(args.linkId);
        return {
          success: true,
          message: `Deleted link ${args.linkId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create setOperatorProperty tool for modifying operator properties
 */
export function createSetOperatorPropertyTool(
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "setOperatorProperty",
    description: "Set or update properties of an operator in the workflow",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to modify"),
      properties: z.record(z.any()).describe("Properties object to set on the operator"),
    }),
    execute: async (args: { operatorId: string; properties: Record<string, any> }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Show copilot is editing this operator
        copilotCoeditor.showEditingOperator(args.operatorId);

        workflowActionService.setOperatorProperty(args.operatorId, args.properties);

        // Show property was changed
        copilotCoeditor.showPropertyChanged(args.operatorId);

        return {
          success: true,
          message: `Updated properties for operator ${args.operatorId}`,
          properties: args.properties,
        };
      } catch (error: any) {
        copilotCoeditor.clearEditingOperator();
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorSchema tool for getting operator schema information
 * Returns the original operator schema (not the dynamic one) to save tokens
 */
export function createGetOperatorSchemaTool(
  workflowActionService: WorkflowActionService,
  operatorMetadataService: OperatorMetadataService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorSchema",
    description:
      "Get the original schema of an operator, which includes all available properties and their types. Use this to understand what properties can be edited on an operator before modifying it.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        // Get the operator to find its type
        const operator = workflowActionService.getTexeraGraph().getOperator(args.operatorId);
        if (!operator) {
          return { success: false, error: `Operator ${args.operatorId} not found` };
        }

        // Get the original operator schema from metadata
        const schema = operatorMetadataService.getOperatorSchema(operator.operatorType);

        return {
          success: true,
          schema: schema,
          message: `Retrieved original schema for operator ${args.operatorId} (type: ${operator.operatorType})`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorInputSchema tool for getting operator's input schema from compilation
 */
export function createGetOperatorInputSchemaTool(
  workflowCompilingService: WorkflowCompilingService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorInputSchema",
    description:
      "Get the input schema for an operator, which shows what columns/attributes are available from upstream operators. This is determined by workflow compilation and schema propagation.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get input schema for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight the operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

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

/**
 * Create getWorkflowCompilationState tool for checking compilation status and errors
 */
export function createGetWorkflowCompilationStateTool(workflowCompilingService: WorkflowCompilingService) {
  return tool({
    name: "getWorkflowCompilationState",
    description:
      "Get the current workflow compilation state and any compilation errors. Use this to check if the workflow is valid and identify any operator configuration issues.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const compilationState = workflowCompilingService.getWorkflowCompilationState();
        const compilationErrors = workflowCompilingService.getWorkflowCompilationErrors();

        const hasErrors = Object.keys(compilationErrors).length > 0;

        return {
          success: true,
          state: compilationState,
          hasErrors: hasErrors,
          errors: hasErrors ? compilationErrors : undefined,
          message: hasErrors
            ? `Workflow compilation failed with ${Object.keys(compilationErrors).length} error(s)`
            : `Workflow compilation state: ${compilationState}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create executeWorkflow tool for running the workflow
 */
export function createExecuteWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "executeWorkflow",
    description: "Execute the current workflow",
    inputSchema: z.object({
      executionName: z.string().optional().describe("Name for this execution (default: 'Copilot Execution')"),
      targetOperatorId: z
        .string()
        .optional()
        .describe("Optional operator ID to execute up to (executes entire workflow if not specified)"),
    }),
    execute: async (args: { executionName?: string; targetOperatorId?: string }) => {
      try {
        const name = args.executionName || "Copilot Execution";
        executeWorkflowService.executeWorkflow(name, args.targetOperatorId);
        return {
          success: true,
          message: args.targetOperatorId
            ? `Started workflow execution up to operator ${args.targetOperatorId}`
            : "Started workflow execution",
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getExecutionState tool for checking workflow execution status
 */
export function createGetExecutionStateTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "getExecutionState",
    description: "Get the current execution state of the workflow",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const stateInfo = executeWorkflowService.getExecutionState();
        const stateString = ExecutionState[stateInfo.state];
        return {
          success: true,
          state: stateString,
          stateInfo: stateInfo,
          message: `Workflow execution state: ${stateString}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create hasOperatorResult tool for checking if an operator has results
 */
export function createHasOperatorResultTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "hasOperatorResult",
    description: "Check if an operator has any execution results available",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to check"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being checked
        copilotCoeditor.highlightOperators([args.operatorId]);

        const hasResult = workflowResultService.hasAnyResult(args.operatorId);

        return {
          success: true,
          hasResult: hasResult,
          message: hasResult
            ? `Operator ${args.operatorId} has results available`
            : `Operator ${args.operatorId} has no results`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorResultSnapshot tool for getting operator result data
 */
export function createGetOperatorResultSnapshotTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorResultSnapshot",
    description: "Get the result snapshot data for an operator (for visualization outputs)",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get results for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        const resultService = workflowResultService.getResultService(args.operatorId);
        if (!resultService) {
          return {
            success: false,
            error: `No result snapshot available for operator ${args.operatorId}. It may use paginated results instead.`,
          };
        }
        const snapshot = resultService.getCurrentResultSnapshot();

        return {
          success: true,
          operatorId: args.operatorId,
          result: snapshot,
          message: `Retrieved result snapshot for operator ${args.operatorId}`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getOperatorResultInfo tool for getting operator result information
 */
export function createGetOperatorResultInfoTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorResultInfo",
    description: "Get information about an operator's results, including total count and pagination details",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get result info for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being inspected
        copilotCoeditor.highlightOperators([args.operatorId]);

        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (!paginatedResultService) {
          return {
            success: false,
            error: `No paginated results available for operator ${args.operatorId}`,
          };
        }
        const totalTuples = paginatedResultService.getCurrentTotalNumTuples();
        const currentPage = paginatedResultService.getCurrentPageIndex();
        const schema = paginatedResultService.getSchema();

        return {
          success: true,
          operatorId: args.operatorId,
          totalTuples: totalTuples,
          currentPage: currentPage,
          schema: schema,
          message: `Operator ${args.operatorId} has ${totalTuples} result tuples`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
