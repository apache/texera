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
import { ExecuteWorkflowService } from "../../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../../workflow-result/workflow-result.service";
import { WorkflowActionService } from "../../workflow-graph/model/workflow-action.service";
import { WorkflowConsoleService } from "../../workflow-console/workflow-console.service";
import {
  estimateTokenCount,
  MAX_OPERATOR_RESULT_TOKEN_LIMIT,
  createSuccessResult,
  createErrorResult,
} from "./tools-utility";

// Tool name constants
export const TOOL_NAME_EXECUTE_CURRENT_WORKFLOW = "executeCurrentWorkflow";
export const TOOL_NAME_GET_CURRENT_EXECUTION_STATE = "getCurrentExecutionState";
export const TOOL_NAME_KILL_CURRENT_WORKFLOW = "killCurrentWorkflow";
export const TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT = "hasCurrentOperatorResult";
export const TOOL_NAME_GET_CURRENT_OPERATOR_RESULT = "getCurrentOperatorResult";
export const TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO = "getCurrentOperatorResultInfo";
export const TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS = "getCurrentComputingUnitStatus";

/**
 * Create executeWorkflow tool for running the workflow
 */
export function createExecuteCurrentWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: TOOL_NAME_EXECUTE_CURRENT_WORKFLOW,
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
        return createSuccessResult(
          {
            message: args.targetOperatorId
              ? `Started workflow execution up to operator ${args.targetOperatorId}`
              : "Started workflow execution",
          },
          [],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create getExecutionState tool for checking workflow execution status
 */
export function createGetCurrentExecutionStateTool(
  executeWorkflowService: ExecuteWorkflowService,
  workflowActionService: WorkflowActionService,
  workflowConsoleService: WorkflowConsoleService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_EXECUTION_STATE,
    description: "Get the current execution state of the workflow, including console logs from operators",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const stateInfo = executeWorkflowService.getExecutionState();

        // Get console logs for all operators in the workflow
        const consoleLogs: { [operatorId: string]: ReadonlyArray<any> } = {};
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();

        for (const operator of allOperators) {
          const operatorId = operator.operatorID;
          if (workflowConsoleService.hasConsoleMessages(operatorId)) {
            const messages = workflowConsoleService.getConsoleMessages(operatorId);
            if (messages && messages.length > 0) {
              consoleLogs[operatorId] = messages;
            }
          }
        }

        // Only include essential information, not the entire stateInfo which can be very large
        return createSuccessResult(
          {
            state: stateInfo,
            consoleLogs: consoleLogs,
          },
          [],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create killWorkflow tool for stopping workflow execution
 */
export function createKillCurrentWorkflowTool(executeWorkflowService: ExecuteWorkflowService) {
  return tool({
    name: TOOL_NAME_KILL_CURRENT_WORKFLOW,
    description:
      "Kill the currently running workflow execution. Use this when the workflow is stuck or you need to stop it. Cannot kill if workflow is uninitialized or already completed.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        executeWorkflowService.killWorkflow();
        return createSuccessResult(
          {
            message: "Workflow execution killed successfully",
          },
          [],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create hasOperatorResult tool for checking if an operator has results
 */
export function createHasCurrentOperatorResultTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT,
    description: "Check if an operator in the current workflow has any execution results available",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to check"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const hasResult = workflowResultService.hasAnyResult(args.operatorId);

        return createSuccessResult(
          {
            hasResult: hasResult,
            message: hasResult
              ? `Operator ${args.operatorId} has results available`
              : `Operator ${args.operatorId} has no results`,
          },
          [args.operatorId],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create unified getOperatorResult tool that automatically handles both pagination and snapshot modes
 */
export function createGetCurrentOperatorResultTool(workflowResultService: WorkflowResultService) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_OPERATOR_RESULT,
    description:
      "Get result data for an operator in the current workflow. Automatically detects and uses the appropriate mode (pagination for tables, snapshot for visualizations). Returns rows limited by token count (~3000 tokens) to avoid overwhelming LLM context.",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get results for"),
    }),
    execute: async (args: { operatorId: string; signal?: AbortSignal }) => {
      try {
        // First, try pagination mode (for table results)
        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (paginatedResultService) {
          try {
            // Request first page with reasonable size (200 rows)
            // We'll filter by token limit after receiving
            const pageSize = 200;
            const resultEvent: any = await new Promise((resolve, reject) => {
              const subscription = paginatedResultService.selectPage(1, pageSize).subscribe({
                next: event => {
                  subscription.unsubscribe();
                  resolve(event);
                },
                error: (err: unknown) => {
                  subscription.unsubscribe();
                  reject(err);
                },
              });

              // Handle abort signal
              if (args.signal) {
                args.signal.addEventListener("abort", () => {
                  subscription.unsubscribe();
                  reject(new Error("Operation aborted"));
                });
              }
            });

            // Filter results by token limit
            const limitedResult: any[] = [];
            let currentTokenCount = 0;

            for (const row of resultEvent.table || []) {
              const rowTokens = estimateTokenCount(row);
              if (currentTokenCount + rowTokens > MAX_OPERATOR_RESULT_TOKEN_LIMIT) {
                break; // Stop if adding this row exceeds limit
              }
              limitedResult.push(row);
              currentTokenCount += rowTokens;
            }

            const totalRows = paginatedResultService.getCurrentTotalNumTuples();
            const wasLimited = limitedResult.length < (resultEvent.table?.length || 0);

            // Get table statistics (min, max, not_null_count for each column)
            const tableStats = paginatedResultService.getStats();

            return createSuccessResult(
              {
                operatorId: args.operatorId,
                mode: "pagination",
                totalRows: totalRows,
                displayedRows: limitedResult.length,
                estimatedTokens: currentTokenCount,
                truncated: wasLimited,
                tableStats: tableStats,
                result: { ...resultEvent, table: limitedResult },
                message: wasLimited
                  ? `Retrieved ${limitedResult.length} rows (out of ${totalRows} total, limited by token count ~${currentTokenCount} tokens) from paginated table results for operator ${args.operatorId}`
                  : `Retrieved ${limitedResult.length} rows (out of ${totalRows} total, ~${currentTokenCount} tokens) from paginated table results for operator ${args.operatorId}`,
              },
              [args.operatorId],
              []
            );
          } catch (error: any) {
            return createErrorResult(
              `Failed to fetch paginated results: ${error.message}. This may be due to backend storage issues or results not being ready yet.`
            );
          }
        }

        // If pagination mode is not available, try snapshot mode (for visualization results)
        const resultService = workflowResultService.getResultService(args.operatorId);
        if (resultService) {
          const snapshot = resultService.getCurrentResultSnapshot();
          if (!snapshot || snapshot.length === 0) {
            return createErrorResult(
              `Result snapshot is empty for operator ${args.operatorId}. Results might not be ready yet.`
            );
          }

          // Filter by token limit
          const limitedResult: any[] = [];
          let currentTokenCount = 0;

          for (const row of snapshot) {
            const rowTokens = estimateTokenCount(row);
            if (currentTokenCount + rowTokens > MAX_OPERATOR_RESULT_TOKEN_LIMIT) {
              break; // Stop if adding this row exceeds limit
            }
            limitedResult.push(row);
            currentTokenCount += rowTokens;
          }

          const wasLimited = limitedResult.length < snapshot.length;

          return createSuccessResult(
            {
              operatorId: args.operatorId,
              mode: "snapshot",
              totalRows: snapshot.length,
              displayedRows: limitedResult.length,
              estimatedTokens: currentTokenCount,
              truncated: wasLimited,
              result: limitedResult,
              message: wasLimited
                ? `Retrieved ${limitedResult.length} rows (out of ${snapshot.length} total, limited by token count ~${currentTokenCount} tokens) from snapshot results for operator ${args.operatorId}`
                : `Retrieved ${limitedResult.length} rows (out of ${snapshot.length} total, ~${currentTokenCount} tokens) from snapshot results for operator ${args.operatorId}`,
            },
            [args.operatorId],
            []
          );
        }

        // No results available at all
        return createErrorResult(
          `No results available for operator ${args.operatorId}. The operator may not have been executed yet, or it may not produce viewable results.`
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create getOperatorResultInfo tool for getting operator result information
 */
export function createGetCurrentOperatorResultInfoTool(
  workflowResultService: WorkflowResultService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO,
    description:
      "Get information about an operator's results in the current workflow, including total count and pagination details",
    inputSchema: z.object({
      operatorId: z.string().describe("ID of the operator to get result info for"),
    }),
    execute: async (args: { operatorId: string }) => {
      try {
        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (!paginatedResultService) {
          return createErrorResult(`No paginated results available for operator ${args.operatorId}`);
        }
        const totalTuples = paginatedResultService.getCurrentTotalNumTuples();
        const currentPage = paginatedResultService.getCurrentPageIndex();
        const schema = paginatedResultService.getSchema();

        return createSuccessResult(
          {
            operatorId: args.operatorId,
            totalTuples: totalTuples,
            currentPage: currentPage,
            schema: schema,
            message: `Operator ${args.operatorId} has ${totalTuples} result tuples`,
          },
          [args.operatorId],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}

/**
 * Create getComputingUnitStatus tool for checking computing unit connection status
 */
export function createGetCurrentComputingUnitStatusTool(computingUnitStatusService: any) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS,
    description:
      "Check the status of the computing unit connection for the current workflow. This is important before workflow execution - if the unit is disconnected, workflows cannot be executed. Use this when execution fails or to verify readiness for execution.",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const selectedUnit = computingUnitStatusService.getSelectedComputingUnitValue();

        if (!selectedUnit) {
          return createSuccessResult(
            {
              status: "No Computing Unit",
              isConnected: false,
              message:
                "No computing unit is selected. Workflow execution is not available. Please remind the user to connect to a computing unit.",
            },
            [],
            []
          );
        }

        const unitStatus = selectedUnit.status;
        const isConnected = unitStatus === "Running";

        return createSuccessResult(
          {
            status: unitStatus,
            isConnected: isConnected,
            computingUnit: {
              cuid: selectedUnit.computingUnit.cuid,
              name: selectedUnit.computingUnit.name,
            },
            message: isConnected
              ? `Computing unit "${selectedUnit.computingUnit.name}" is running and ready for workflow execution`
              : unitStatus === "Pending"
                ? `Computing unit "${selectedUnit.computingUnit.name}" is pending/starting. Workflow execution may not be available yet.`
                : `Computing unit is in state: ${unitStatus}. Workflow execution may not be available.`,
          },
          [],
          []
        );
      } catch (error: any) {
        return createErrorResult(error.message);
      }
    },
  });
}
