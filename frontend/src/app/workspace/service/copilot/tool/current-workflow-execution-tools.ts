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
import { WorkflowStatusService } from "../../workflow-status/workflow-status.service";
import { ValidationWorkflowService } from "../../validation/validation-workflow.service";
import { ExecutionState } from "../../../types/execute-workflow.interface";
import {
  estimateTokenCount,
  MAX_OPERATOR_RESULT_TOKEN_LIMIT,
  createSuccessResult,
  createErrorResult,
} from "./tools-utility";
import { Observable, of, throwError, defer, timer, forkJoin } from "rxjs";
import { filter, timeout, map, switchMap, catchError, take } from "rxjs/operators";

// Tool name constants
export const TOOL_NAME_EXECUTE_CURRENT_WORKFLOW = "executeCurrentWorkflow";
export const TOOL_NAME_GET_CURRENT_EXECUTION_STATE = "getCurrentExecutionState";
export const TOOL_NAME_KILL_CURRENT_WORKFLOW = "killCurrentWorkflow";
export const TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT = "hasCurrentOperatorResult";
export const TOOL_NAME_GET_CURRENT_OPERATOR_RESULT = "getCurrentOperatorResult";
export const TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO = "getCurrentOperatorResultInfo";
export const TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS = "getCurrentComputingUnitStatus";

// Execution timeout in milliseconds (10 minutes)
const EXECUTION_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Helper to collect console logs for all operators
 */
function collectConsoleLogs(
  operators: any[],
  workflowConsoleService: WorkflowConsoleService
): Record<string, ReadonlyArray<any>> {
  const consoleLogs: Record<string, ReadonlyArray<any>> = {};
  for (const operator of operators) {
    const operatorId = operator.operatorID;
    if (workflowConsoleService.hasConsoleMessages(operatorId)) {
      const messages = workflowConsoleService.getConsoleMessages(operatorId);
      if (messages && messages.length > 0) {
        consoleLogs[operatorId] = messages;
      }
    }
  }
  return consoleLogs;
}

/**
 * Helper to format operator states with units for readability
 */
function formatOperatorStates(operatorStates: Record<string, any>): Record<string, any> {
  const formatted: Record<string, any> = {};
  for (const [operatorId, stats] of Object.entries(operatorStates)) {
    formatted[operatorId] = {
      state: stats.operatorState,
      inputRows: `${stats.aggregatedInputRowCount} rows`,
      outputRows: `${stats.aggregatedOutputRowCount} rows`,
      inputPortMetrics: Object.fromEntries(
        Object.entries(stats.inputPortMetrics).map(([port, count]) => [port, `${count} rows`])
      ),
      outputPortMetrics: Object.fromEntries(
        Object.entries(stats.outputPortMetrics).map(([port, count]) => [port, `${count} rows`])
      ),
      ...(stats.numWorkers !== undefined && { numWorkers: `${stats.numWorkers} workers` }),
    };
  }
  return formatted;
}

/**
 * Helper to filter results by token limit
 */
function filterByTokenLimit(rows: readonly any[]): { limited: any[]; tokenCount: number; truncated: boolean } {
  const limited: any[] = [];
  let tokenCount = 0;
  for (const row of rows) {
    const rowTokens = estimateTokenCount(row);
    if (tokenCount + rowTokens > MAX_OPERATOR_RESULT_TOKEN_LIMIT) break;
    limited.push(row);
    tokenCount += rowTokens;
  }
  return { limited, tokenCount, truncated: limited.length < rows.length };
}

/**
 * Create unified executeWorkflow tool that validates, executes, monitors, and returns results
 */
export function createExecuteCurrentWorkflowTool(
  executeWorkflowService: ExecuteWorkflowService,
  validationWorkflowService: ValidationWorkflowService,
  workflowActionService: WorkflowActionService,
  workflowConsoleService: WorkflowConsoleService,
  workflowStatusService: WorkflowStatusService,
  workflowResultService: WorkflowResultService
) {
  return tool({
    name: TOOL_NAME_EXECUTE_CURRENT_WORKFLOW,
    description:
      "Execute the current workflow with full validation and monitoring. This tool will: 1) Kill any existing execution, 2) Validate the workflow, 3) Execute it if valid, 4) Monitor execution until completion, 5) Return comprehensive results including operator outputs, stats, console logs, and any errors. This is the primary tool for workflow execution.",
    inputSchema: z.object({
      executionName: z.string().optional().describe("Name for this execution (default: 'Copilot Execution')"),
      targetOperatorId: z
        .string()
        .optional()
        .describe("Optional operator ID to execute up to (executes entire workflow if not specified)"),
    }),
    execute: (args: { executionName?: string; targetOperatorId?: string; signal?: AbortSignal }) => {
      const name = args.executionName || "Copilot Execution";

      // Create the execution observable
      const execution$ = defer(() => {
        // Step 1: Kill existing execution if any
        const currentState = executeWorkflowService.getExecutionState();
        if (
          currentState.state !== ExecutionState.Uninitialized &&
          currentState.state !== ExecutionState.Completed &&
          currentState.state !== ExecutionState.Failed &&
          currentState.state !== ExecutionState.Killed
        ) {
          return of(null).pipe(
            switchMap(() => {
              try {
                executeWorkflowService.killWorkflow();
                // Wait 500ms for the workflow to be killed
                return timer(500);
              } catch (killError: any) {
                // If kill fails, it's likely because the workflow is already in a terminal state
                console.warn("Failed to kill existing execution:", killError.message);
                return of(null);
              }
            })
          );
        }
        return of(null);
      }).pipe(
        // Step 2: Validate the workflow
        switchMap(() => {
          const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
          const errorCount = Object.keys(validationOutput.errors).length;

          if (errorCount > 0) {
            const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
            const validGraph = validationWorkflowService.getValidTexeraGraph();
            const validOperators = validGraph.getAllOperators();

            return throwError(
              () =>
                new Error(
                  `Cannot execute workflow: Found ${errorCount} operator(s) with validation errors. ` +
                    `${validOperators.length} valid operator(s) out of ${allOperators.length} total. ` +
                    `Validation errors: ${JSON.stringify(validationOutput.errors, null, 2)}`
                )
            );
          }

          const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
          if (allOperators.length === 0) {
            return throwError(
              () => new Error("Cannot execute workflow: The workflow is empty. Please add operators first.")
            );
          }

          return of(allOperators);
        }),
        // Step 3: Start execution and monitor until completion
        switchMap(allOperators => {
          // Start the execution
          executeWorkflowService.executeWorkflow(name, args.targetOperatorId);

          // Monitor execution state
          return executeWorkflowService.getExecutionStateStream().pipe(
            filter(
              stateChange =>
                stateChange.current.state === ExecutionState.Completed ||
                stateChange.current.state === ExecutionState.Failed ||
                stateChange.current.state === ExecutionState.Killed
            ),
            take(1),
            timeout(EXECUTION_TIMEOUT_MS),
            map(stateChange => ({ finalState: stateChange.current, allOperators })),
            catchError(error => {
              if (error.name === "TimeoutError") {
                return throwError(
                  () =>
                    new Error(
                      `Workflow execution timed out after ${EXECUTION_TIMEOUT_MS / 1000} seconds. ` +
                        `The workflow may still be running. Use getCurrentExecutionState to check status.`
                    )
                );
              }
              return throwError(() => error);
            })
          );
        }),
        // Step 4: Collect comprehensive results after execution completes
        switchMap(({ finalState, allOperators }) => {
          const finalStateInfo = executeWorkflowService.getExecutionState();
          const consoleLogs = collectConsoleLogs(allOperators, workflowConsoleService);
          const formattedOperatorStates = formatOperatorStates(workflowStatusService.getCurrentStatus());

          // Collect results for operators that have results (in parallel)
          const resultObservables: Observable<{ operatorId: string; result: any }>[] = [];
          for (const operator of allOperators) {
            const operatorId = operator.operatorID;
            if (workflowResultService.hasAnyResult(operatorId)) {
              resultObservables.push(
                getOperatorResult$(operatorId, workflowResultService).pipe(
                  map(result => ({ operatorId, result })),
                  catchError(error =>
                    of({
                      operatorId,
                      result: { error: `Failed to fetch results: ${error.message}` },
                    })
                  )
                )
              );
            }
          }

          // Wait for all result fetches to complete
          const resultsStream$ =
            resultObservables.length > 0
              ? forkJoin(resultObservables).pipe(
                  map(results => {
                    const operatorResults: Record<string, any> = {};
                    for (const { operatorId, result } of results) {
                      operatorResults[operatorId] = result;
                    }
                    return operatorResults;
                  })
                )
              : of({});

          return resultsStream$.pipe(
            map(operatorResults => ({
              finalState,
              finalStateInfo,
              formattedOperatorStates,
              consoleLogs,
              operatorResults,
            }))
          );
        }),
        // Build final result based on execution state
        map(({ finalState, finalStateInfo, formattedOperatorStates, consoleLogs, operatorResults }) => {
          const errorMessages = executeWorkflowService.getErrorMessages();

          if (finalState.state === ExecutionState.Completed) {
            return createSuccessResult(
              {
                executionState: "Completed",
                message: args.targetOperatorId
                  ? `Workflow executed successfully up to operator ${args.targetOperatorId}`
                  : "Workflow executed successfully",
                state: finalStateInfo,
                operatorStates: formattedOperatorStates,
                consoleLogs: consoleLogs,
                operatorResults: operatorResults,
              },
              [],
              []
            );
          } else if (finalState.state === ExecutionState.Failed) {
            return createErrorResult(
              `Workflow execution failed. Error messages: ${JSON.stringify(errorMessages, null, 2)}. ` +
                `Console logs: ${JSON.stringify(consoleLogs, null, 2)}. ` +
                `Operator states: ${JSON.stringify(formattedOperatorStates, null, 2)}`
            );
          } else if (finalState.state === ExecutionState.Killed) {
            return createErrorResult(
              "Workflow execution was killed. " +
                `Console logs: ${JSON.stringify(consoleLogs, null, 2)}. ` +
                `Operator states: ${JSON.stringify(formattedOperatorStates, null, 2)}`
            );
          } else {
            return createErrorResult(`Unexpected execution state: ${finalState.state}`);
          }
        }),
        catchError((error: any) => of(createErrorResult(`Execution error: ${error.message}`)))
      );

      // Convert observable to promise for the tool framework
      return new Promise((resolve, reject) => {
        const subscription = execution$.subscribe({
          next: result => {
            subscription.unsubscribe();
            resolve(result);
          },
          error: err => {
            subscription.unsubscribe();
            reject(err);
          },
        });

        // Handle abort signal
        if (args.signal) {
          args.signal.addEventListener("abort", () => {
            subscription.unsubscribe();
            reject(new Error("Operation aborted by user"));
          });
        }
      });
    },
  });
}

/**
 * Helper function to get operator result with token limit (Observable-based)
 */
function getOperatorResult$(operatorId: string, workflowResultService: WorkflowResultService): Observable<any> {
  return defer(() => {
    const paginatedResultService = workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      return paginatedResultService.selectPage(1, 200).pipe(
        take(1),
        map(resultEvent => {
          const { limited, tokenCount, truncated } = filterByTokenLimit(resultEvent.table || []);
          return {
            mode: "pagination",
            totalRows: paginatedResultService.getCurrentTotalNumTuples(),
            displayedRows: limited.length,
            estimatedTokens: tokenCount,
            truncated,
            tableStats: paginatedResultService.getStats(),
            result: { ...resultEvent, table: limited },
          };
        })
      );
    }

    const resultService = workflowResultService.getResultService(operatorId);
    if (resultService) {
      const snapshot = resultService.getCurrentResultSnapshot();
      if (!snapshot?.length) {
        return throwError(() => new Error("Result snapshot is empty"));
      }
      const { limited, tokenCount, truncated } = filterByTokenLimit(snapshot);
      return of({
        mode: "snapshot",
        totalRows: snapshot.length,
        displayedRows: limited.length,
        estimatedTokens: tokenCount,
        truncated,
        result: limited,
      });
    }

    return throwError(() => new Error("No results available"));
  });
}

/**
 * Create getExecutionState tool for checking workflow execution status
 */
export function createGetCurrentExecutionStateTool(
  executeWorkflowService: ExecuteWorkflowService,
  workflowActionService: WorkflowActionService,
  workflowConsoleService: WorkflowConsoleService,
  workflowStatusService: WorkflowStatusService
) {
  return tool({
    name: TOOL_NAME_GET_CURRENT_EXECUTION_STATE,
    description:
      "Get the current execution state of the workflow, including console logs, execution duration, and operator states (Running=orange, Completed=green, Ready=lime, Paused=magenta)",
    inputSchema: z.object({}),
    execute: async () => {
      try {
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
        return createSuccessResult(
          {
            state: executeWorkflowService.getExecutionState(),
            consoleLogs: collectConsoleLogs(allOperators, workflowConsoleService),
            operatorStates: formatOperatorStates(workflowStatusService.getCurrentStatus()),
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
 * Helper to build result message
 */
function buildResultMessage(
  operatorId: string,
  mode: string,
  displayedRows: number,
  totalRows: number,
  tokenCount: number,
  truncated: boolean
): string {
  const base = `Retrieved ${displayedRows} rows (out of ${totalRows} total, ~${tokenCount} tokens) from ${mode} results for operator ${operatorId}`;
  return truncated ? base.replace(")", ", limited by token count)") : base;
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
        const paginatedResultService = workflowResultService.getPaginatedResultService(args.operatorId);
        if (paginatedResultService) {
          try {
            const resultEvent: any = await new Promise((resolve, reject) => {
              const subscription = paginatedResultService.selectPage(1, 200).subscribe({
                next: event => {
                  subscription.unsubscribe();
                  resolve(event);
                },
                error: (err: unknown) => {
                  subscription.unsubscribe();
                  reject(err);
                },
              });
              if (args.signal) {
                args.signal.addEventListener("abort", () => {
                  subscription.unsubscribe();
                  reject(new Error("Operation aborted"));
                });
              }
            });

            const { limited, tokenCount, truncated } = filterByTokenLimit(resultEvent.table || []);
            const totalRows = paginatedResultService.getCurrentTotalNumTuples();

            return createSuccessResult(
              {
                operatorId: args.operatorId,
                mode: "pagination",
                totalRows,
                displayedRows: limited.length,
                estimatedTokens: tokenCount,
                truncated,
                tableStats: paginatedResultService.getStats(),
                result: { ...resultEvent, table: limited },
                message: buildResultMessage(
                  args.operatorId,
                  "paginated table",
                  limited.length,
                  totalRows,
                  tokenCount,
                  truncated
                ),
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

        const resultService = workflowResultService.getResultService(args.operatorId);
        if (resultService) {
          const snapshot = resultService.getCurrentResultSnapshot();
          if (!snapshot?.length) {
            return createErrorResult(
              `Result snapshot is empty for operator ${args.operatorId}. Results might not be ready yet.`
            );
          }

          const { limited, tokenCount, truncated } = filterByTokenLimit(snapshot);

          return createSuccessResult(
            {
              operatorId: args.operatorId,
              mode: "snapshot",
              totalRows: snapshot.length,
              displayedRows: limited.length,
              estimatedTokens: tokenCount,
              truncated,
              result: limited,
              message: buildResultMessage(
                args.operatorId,
                "snapshot",
                limited.length,
                snapshot.length,
                tokenCount,
                truncated
              ),
            },
            [args.operatorId],
            []
          );
        }

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
