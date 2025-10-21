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
import { CopilotCoeditorService } from "./copilot-coeditor.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";

// Tool execution timeout in milliseconds (5 seconds)
const TOOL_TIMEOUT_MS = 120000;

/**
 * Wraps a tool definition to add timeout protection to its execute function
 */
export function toolWithTimeout(toolConfig: any): any {
  const originalExecute = toolConfig.execute;

  return {
    ...toolConfig,
    execute: async (args: any) => {
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => {
          reject(new Error("timeout"));
        }, TOOL_TIMEOUT_MS);
      });

      try {
        return await Promise.race([originalExecute(args), timeoutPromise]);
      } catch (error: any) {
        // If it's a timeout error, return a properly formatted error response
        if (error.message === "timeout") {
          return {
            success: false,
            error: "Tool execution timeout - operation took longer than 5 seconds. Please try again later.",
          };
        }
        // Re-throw other errors to be handled by the original error handler
        throw error;
      }
    },
  };
}

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
      "Get the original schema of an operator, which includes properties of this operator. Use this to understand what properties can be edited on an operator before modifying it.",
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
        // Only include essential information, not the entire stateInfo which can be very large
        const result: any = {
          success: true,
          state: stateInfo,
        };
        return result;
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create killWorkflow tool for stopping workflow execution
 */
export function createKillWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: "killWorkflow",
    description:
      "Kill the currently running workflow execution. Use this when the workflow is stuck or you need to stop it. Cannot kill if workflow is uninitialized or already completed.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        executeWorkflowService.killWorkflow();
        return {
          success: true,
          message: "Workflow execution killed successfully",
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
 * Create getOperatorResultPage tool for getting operator result data page
 */
export function createGetOperatorResultPageTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "getOperatorResultPage",
    description:
      "Get a page of result data for an operator. Returns the first page (page 0) with up to 100 rows. Use this to inspect execution results.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get results for"),
      pageSize: z.number().optional().describe("Number of rows to retrieve (default: 100, max: 100)"),
    }),
    execute: async (args: { operatorId: string; pageSize?: number }) => {
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

        // Use provided pageSize, default to 100, cap at 100
        const pageSize = Math.min(args.pageSize || 100, 100);

        // Get page 0 with the specified page size
        const resultEvent = await new Promise((resolve, reject) => {
          paginatedResultService.selectPage(0, pageSize).subscribe({
            next: event => resolve(event),
            error: err => reject(err),
          });
        });

        return {
          success: true,
          operatorId: args.operatorId,
          pageIndex: 0,
          pageSize: pageSize,
          result: resultEvent,
          message: `Retrieved page 0 with ${pageSize} rows for operator ${args.operatorId}`,
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

/**
 * Create getWorkflowValidationErrors tool for getting workflow validation errors
 */
export function createGetWorkflowValidationErrorsTool(validationWorkflowService: ValidationWorkflowService) {
  return tool({
    name: "getWorkflowValidationErrors",
    description:
      "Get all current validation errors in the workflow. This shows which operators have validation issues and what the errors are. Use this to check if operators are properly configured before execution.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
        const errorCount = Object.keys(validationOutput.errors).length;

        return {
          success: true,
          errors: validationOutput.errors,
          errorCount: errorCount,
          message:
            errorCount === 0
              ? "No validation errors in the workflow"
              : `Found ${errorCount} operator(s) with validation errors`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create validateOperator tool for validating a specific operator
 */
export function createValidateOperatorTool(
  validationWorkflowService: ValidationWorkflowService,
  copilotCoeditor: CopilotCoeditorService
) {
  return tool({
    name: "validateOperator",
    description:
      "Validate a specific operator to check if it's properly configured. Returns validation status and any error messages if invalid.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to validate"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        // Clear previous highlights at start of tool execution
        copilotCoeditor.clearAll();

        // Highlight operator being validated
        copilotCoeditor.highlightOperators([args.operatorId]);

        const validation = validationWorkflowService.validateOperator(args.operatorId);

        if (validation.isValid) {
          return {
            success: true,
            isValid: true,
            message: `Operator ${args.operatorId} is valid`,
          };
        } else {
          return {
            success: true,
            isValid: false,
            errors: validation.messages,
            message: `Operator ${args.operatorId} has validation errors`,
          };
        }
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}

/**
 * Create getValidOperators tool for getting list of valid operators
 */
export function createGetValidOperatorsTool(
  validationWorkflowService: ValidationWorkflowService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: "getValidOperators",
    description:
      "Get a list of all valid operators in the workflow. This filters out operators with validation errors and returns only properly configured operators.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const validGraph = validationWorkflowService.getValidTexeraGraph();
        const validOperators = validGraph.getAllOperators();
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();

        const validOperatorIds = validOperators.map(op => op.operatorID);
        const invalidCount = allOperators.length - validOperators.length;

        return {
          success: true,
          validOperatorIds: validOperatorIds,
          validCount: validOperators.length,
          totalCount: allOperators.length,
          invalidCount: invalidCount,
          message: `Found ${validOperators.length} valid operator(s) out of ${allOperators.length} total`,
        };
      } catch (error: any) {
        return { success: false, error: error.message };
      }
    },
  });
}
