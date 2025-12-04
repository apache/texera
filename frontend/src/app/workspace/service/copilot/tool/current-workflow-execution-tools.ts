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
import { WorkflowCompilingService } from "../../compile-workflow/workflow-compiling.service";
import { ExecutionState, ExecutionStateInfo, OperatorStatistics } from "../../../types/execute-workflow.interface";
import { CompilationState } from "../../../types/workflow-compiling.interface";
import { ConsoleMessage, OperatorPredicate } from "../../../types/workflow-common.interface";
import { IndexableObject } from "../../../types/result-table.interface";
import { PaginatedResultEvent } from "../../../types/workflow-websocket.interface";
import {
  estimateTokenCount,
  MAX_OPERATOR_RESULT_TOKEN_LIMIT,
  createSuccessResult,
  createErrorResult,
} from "./tools-utility";
import { Observable, of, throwError, defer, timer, interval, EMPTY, firstValueFrom, forkJoin } from "rxjs";
import { filter, timeout, map, switchMap, catchError, take, tap, defaultIfEmpty } from "rxjs/operators";

// Tool name constants
export const TOOL_NAME_EXECUTE_CURRENT_WORKFLOW_AND_RETRIEVE_RESULTS = "executeCurrentWorkflowAndRetrieveResults";
export const TOOL_NAME_GET_CURRENT_EXECUTION_STATE = "getCurrentExecutionState";
export const TOOL_NAME_KILL_CURRENT_WORKFLOW = "killCurrentWorkflow";
export const TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT = "hasCurrentOperatorResult";
export const TOOL_NAME_GET_EXISTING_WORKFLOW_EXECUTION_RESULT = "getExistingWorkflowExecutionResult";
export const TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO = "getCurrentOperatorResultInfo";
export const TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS = "getCurrentComputingUnitStatus";

// Execution timeout in milliseconds (10 minutes)
const EXECUTION_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Helper to collect console logs for specified operators
 */
function collectConsoleLogs(
  operatorIds: readonly string[],
  workflowConsoleService: WorkflowConsoleService
): Record<string, ReadonlyArray<ConsoleMessage>> {
  const consoleLogs: Record<string, ReadonlyArray<ConsoleMessage>> = {};
  for (const operatorId of operatorIds) {
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
 * Formatted operator state for readability
 */
interface FormattedOperatorState {
  state: string;
  inputRows: string;
  outputRows: string;
  inputPortMetrics: Record<string, string>;
  outputPortMetrics: Record<string, string>;
}

/**
 * Helper to format operator states with units for readability
 */
function formatOperatorStates(
  operatorStates: Record<string, OperatorStatistics>
): Record<string, FormattedOperatorState> {
  const formatted: Record<string, FormattedOperatorState> = {};
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
    };
  }
  return formatted;
}

/**
 * Helper to filter results by token limit
 */
function filterByTokenLimit(rows: readonly IndexableObject[]): {
  limited: IndexableObject[];
  tokenCount: number;
  truncated: boolean;
} {
  const limited: IndexableObject[] = [];
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
 * Operator result with metadata
 */
export interface OperatorResultInfo {
  mode: "pagination" | "snapshot";
  totalRows: number;
  displayedRows: number;
  estimatedTokens: number;
  truncated: boolean;
  tableStats?: Record<string, Record<string, number>>;
  result: PaginatedResultEvent | IndexableObject[];
}

/**
 * Options for the common execution function
 */
export interface ExecuteWorkflowOptions {
  executionName?: string;
  targetOperatorIds: readonly string[]; // Required: operator IDs to monitor results for
  includeOperatorResults?: boolean; // Default: true
}

/**
 * Result of workflow execution
 */
export interface ExecuteWorkflowResult {
  success: boolean;
  executionState: string;
  message: string;
  state?: ExecutionStateInfo;
  operatorStates?: Record<string, FormattedOperatorState>;
  consoleLogs?: Record<string, ReadonlyArray<ConsoleMessage>>;
  operatorResults?: Record<string, OperatorResultInfo>;
  error?: string;
}

/**
 * Services required for workflow execution
 */
export interface WorkflowExecutionServices {
  executeWorkflowService: ExecuteWorkflowService;
  validationWorkflowService: ValidationWorkflowService;
  workflowCompilingService: WorkflowCompilingService;
  workflowActionService: WorkflowActionService;
  workflowConsoleService: WorkflowConsoleService;
  workflowStatusService: WorkflowStatusService;
  workflowResultService: WorkflowResultService;
}

/**
 * Common function to execute workflow and get results as an Observable.
 * This is the core logic shared by both the executeCurrentWorkflow tool and baseline tools.
 *
 * Execution behavior based on targetOperatorIds:
 * - Empty array or multiple elements: Execute entire workflow, collect results for specified operators
 * - Single element: Execute up to that operator only (executeTo mode)
 *
 * @param services - Required workflow services
 * @param options - Execution options including target operators to monitor
 * @returns Observable with execution result
 */
export function executeWorkflowAndGetResults$(
  services: WorkflowExecutionServices,
  options: ExecuteWorkflowOptions
): Observable<ExecuteWorkflowResult> {
  const {
    executeWorkflowService,
    validationWorkflowService,
    workflowCompilingService,
    workflowActionService,
    workflowConsoleService,
    workflowStatusService,
    workflowResultService,
  } = services;

  const name = options.executionName || "Copilot Execution";
  const includeResults = options.includeOperatorResults !== false;
  const targetIds = options.targetOperatorIds;

  // Determine execution mode: single operator = executeTo, else full workflow
  const isSingleOperatorMode = targetIds.length === 1;
  const executeToOperatorId = isSingleOperatorMode ? targetIds[0] : undefined;

  return defer(() => {
    // Step 1: Kill existing execution if needed
    const currentState = executeWorkflowService.getExecutionState();
    const needsKill =
      currentState.state !== ExecutionState.Uninitialized &&
      currentState.state !== ExecutionState.Completed &&
      currentState.state !== ExecutionState.Failed &&
      currentState.state !== ExecutionState.Killed;

    if (needsKill) {
      try {
        executeWorkflowService.killWorkflow();
      } catch (e: unknown) {
        console.warn("Failed to kill existing execution:", e instanceof Error ? e.message : String(e));
      }
      return timer(500).pipe(map(() => null));
    }
    return of(null);
  }).pipe(
    // Step 2: Validate workflow
    switchMap(() => {
      const validationOutput = validationWorkflowService.getCurrentWorkflowValidationError();
      const errorCount = Object.keys(validationOutput.errors).length;

      if (errorCount > 0) {
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const validOperators = validationWorkflowService.getValidTexeraGraph().getAllOperators();
        return throwError(
          () =>
            new Error(
              `Cannot execute: ${errorCount} operator(s) with validation errors. ` +
                `${validOperators.length}/${allOperators.length} valid. ` +
                `Errors: ${JSON.stringify(validationOutput.errors, null, 2)}`
            )
        );
      }

      const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
      if (allOperators.length === 0) {
        return throwError(() => new Error("Cannot execute: workflow is empty."));
      }

      // Check compilation state
      const compilationState = workflowCompilingService.getWorkflowCompilationState();
      if (compilationState === CompilationState.Failed) {
        const compilationErrors = workflowCompilingService.getWorkflowCompilationErrors();
        const errorCount = Object.keys(compilationErrors).length;
        return throwError(
          () =>
            new Error(
              `Cannot execute: workflow compilation failed with ${errorCount} error(s). ` +
                `Errors: ${JSON.stringify(compilationErrors, null, 2)}`
            )
        );
      }

      return of(allOperators);
    }),
    // Step 3: Set view results and start execution
    switchMap((allOperators: readonly OperatorPredicate[]) => {
      // For non-single mode with specified operators, set view results first
      if (!isSingleOperatorMode && targetIds.length > 0) {
        workflowActionService.setViewOperatorResults(targetIds);
      }

      // Start execution
      executeWorkflowService.executeWorkflow(name, executeToOperatorId);

      // Monitor execution state
      const executionState$ = executeWorkflowService.getExecutionStateStream().pipe(
        filter(
          change =>
            change.current.state === ExecutionState.Completed ||
            change.current.state === ExecutionState.Failed ||
            change.current.state === ExecutionState.Killed ||
            change.current.state === ExecutionState.Paused
        ),
        map(change => change.current)
      );

      // Detect stuck state (Running but operator Paused) or error console logs
      // When detected, kill the workflow - the executionState$ will then emit the Killed state
      const stuckDetection$ = interval(1000).pipe(
        filter(() => {
          const state = executeWorkflowService.getExecutionState();
          if (state.state !== ExecutionState.Running) return false;

          // Check for paused operators
          const opStates = workflowStatusService.getCurrentStatus();
          const hasPausedOperator = Object.values(opStates).some(s => s.operatorState === "Paused");
          if (hasPausedOperator) return true;

          // Check for ERROR level console logs in any operator
          const allOperatorIds = allOperators.map(op => op.operatorID);
          for (const operatorId of allOperatorIds) {
            if (workflowConsoleService.hasConsoleMessages(operatorId)) {
              const messages = workflowConsoleService.getConsoleMessages(operatorId);
              if (messages && messages.some(msg => msg.msgType.name === "ERROR")) {
                return true;
              }
            }
          }

          return false;
        }),
        take(1),
        tap(() => {
          try {
            executeWorkflowService.killWorkflow();
          } catch {
            /* ignore */
          }
        }),
        // Don't emit anything - let executionState$ catch the Killed state
        switchMap(() => EMPTY)
      );

      // Subscribe to stuckDetection$ to trigger kill, but only wait for executionState$
      // Use a shared subscription so stuckDetection$ side effects happen
      const stuckDetectionSubscription = stuckDetection$.subscribe();

      return executionState$.pipe(
        take(1),
        timeout(EXECUTION_TIMEOUT_MS),
        map(finalState => ({ finalState, allOperators })),
        tap(() => stuckDetectionSubscription.unsubscribe()),
        catchError((error: unknown) => {
          stuckDetectionSubscription.unsubscribe();
          if (error instanceof Error && error.name === "TimeoutError") {
            return throwError(() => new Error(`Execution timed out after ${EXECUTION_TIMEOUT_MS / 1000}s.`));
          }
          return throwError(() => error);
        })
      );
    }),
    // Step 4: Collect results at the end
    switchMap(({ finalState, allOperators }) => {
      const finalStateInfo = executeWorkflowService.getExecutionState();
      const allOperatorIds = allOperators.map(op => op.operatorID);
      const consoleLogs = collectConsoleLogs(allOperatorIds, workflowConsoleService);
      const formattedStates = formatOperatorStates(workflowStatusService.getCurrentStatus());

      // Collect results at the end using RxJS
      if (!includeResults) {
        return of({ finalState, finalStateInfo, formattedStates, consoleLogs, operatorResults: {} });
      }

      const opsToFetch = targetIds.length > 0 ? targetIds : allOperatorIds;
      return collectOperatorResults$(opsToFetch, workflowResultService).pipe(
        map(operatorResults => ({ finalState, finalStateInfo, formattedStates, consoleLogs, operatorResults }))
      );
    }),
    // Step 5: Build result
    map(({ finalState, finalStateInfo, formattedStates, consoleLogs, operatorResults }) => {
      const errorMessages = executeWorkflowService.getErrorMessages();
      const targetMsg = isSingleOperatorMode ? ` up to operator ${executeToOperatorId}` : "";

      if (finalState.state === ExecutionState.Completed) {
        return {
          success: true,
          executionState: "Completed",
          message: `Workflow executed successfully${targetMsg}`,
          state: finalStateInfo,
          operatorStates: formattedStates,
          consoleLogs,
          operatorResults,
        };
      }

      const baseError = {
        success: false,
        operatorStates: formattedStates,
        consoleLogs,
      };

      // Include operatorResults in all failure cases for debugging
      const baseErrorWithResults = {
        ...baseError,
        operatorResults,
      };

      if (finalState.state === ExecutionState.Failed) {
        return {
          ...baseErrorWithResults,
          executionState: "Failed",
          message: "Workflow execution failed",
          error: `Failed. Errors: ${JSON.stringify(errorMessages)}`,
        };
      }
      if (finalState.state === ExecutionState.Killed) {
        return {
          ...baseErrorWithResults,
          executionState: "Killed",
          message: "Workflow execution was killed (possibly due to paused operator or error console logs)",
          error: `Execution was killed. Errors: ${JSON.stringify(errorMessages)}`,
        };
      }
      if (finalState.state === ExecutionState.Paused) {
        return {
          ...baseErrorWithResults,
          executionState: "Paused",
          message: "Workflow paused (likely error)",
          error: `Paused. Errors: ${JSON.stringify(errorMessages)}`,
        };
      }

      return {
        ...baseErrorWithResults,
        executionState: String(finalState.state),
        message: `Unexpected state: ${finalState.state}`,
        error: `Unexpected state: ${finalState.state}`,
      };
    }),
    catchError((error: unknown) =>
      of({
        success: false,
        executionState: "Error",
        message: "Execution error",
        error: `Error: ${error instanceof Error ? error.message : String(error)}`,
        // Note: consoleLogs may not be available in catchError since we failed before collecting them
      })
    )
  );
}

/**
 * Create unified executeWorkflow tool that validates, executes, monitors, and returns results
 */
export function createExecuteCurrentWorkflowTool(
  executeWorkflowService: ExecuteWorkflowService,
  validationWorkflowService: ValidationWorkflowService,
  workflowCompilingService: WorkflowCompilingService,
  workflowActionService: WorkflowActionService,
  workflowConsoleService: WorkflowConsoleService,
  workflowStatusService: WorkflowStatusService,
  workflowResultService: WorkflowResultService
) {
  const services: WorkflowExecutionServices = {
    executeWorkflowService,
    validationWorkflowService,
    workflowCompilingService,
    workflowActionService,
    workflowConsoleService,
    workflowStatusService,
    workflowResultService,
  };

  return tool({
    name: TOOL_NAME_EXECUTE_CURRENT_WORKFLOW_AND_RETRIEVE_RESULTS,
    description:
      "Execute the current workflow with full validation and monitoring. This tool will: 1) Kill any existing execution, 2) Validate the workflow, 3) Execute it if valid, 4) Monitor execution until completion, 5) Return comprehensive results including operator outputs, stats, console logs, and any errors. This is the primary tool for workflow execution.",
    inputSchema: z.object({
      executionName: z.string().optional().describe("Name for this execution (default: 'Copilot Execution')"),
      targetOperatorIds: z
        .array(z.string())
        .describe(
          "Operator IDs to monitor results for. If single ID, executes up to that operator only. " +
            "If empty or multiple IDs, executes entire workflow and collects results for target operators."
        ),
    }),
    execute: async (args: { executionName?: string; targetOperatorIds: string[] }) => {
      try {
        const result = await firstValueFrom(
          executeWorkflowAndGetResults$(services, {
            executionName: args.executionName,
            targetOperatorIds: args.targetOperatorIds,
            includeOperatorResults: true,
          })
        );

        if (result.success) {
          return createSuccessResult(
            {
              executionState: result.executionState,
              message: result.message,
              state: result.state,
              operatorStates: result.operatorStates,
              consoleLogs: result.consoleLogs,
              operatorResults: result.operatorResults,
            },
            [],
            []
          );
        } else {
          // Include console logs, operator states, and operator results even on failure for debugging
          return {
            success: false,
            error: result.error || result.message,
            executionState: result.executionState,
            message: result.message,
            operatorStates: result.operatorStates,
            consoleLogs: result.consoleLogs,
            operatorResults: result.operatorResults,
            viewedOperatorIds: [],
            modifiedOperatorIds: [],
          };
        }
      } catch (error: unknown) {
        // Catch any unhandled errors from the Observable
        return createErrorResult(
          `Execution failed unexpectedly: ${error instanceof Error ? error.message : String(error)}`
        );
      }
    },
  });
}

// Timeout for fetching paginated results (5 seconds)
const RESULT_FETCH_TIMEOUT_MS = 5000;
// Retry configuration for waiting for results
const RESULT_WAIT_MAX_RETRIES = 10;
const RESULT_WAIT_DELAY_MS = 200;

/**
 * Get operator result as Observable with token limit
 */
function getOperatorResult$(
  operatorId: string,
  workflowResultService: WorkflowResultService
): Observable<OperatorResultInfo | null> {
  return defer(() => {
    // Try paginated result service first
    const paginatedResultService = workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      return paginatedResultService.selectPage(1, 200).pipe(
        timeout(RESULT_FETCH_TIMEOUT_MS),
        map((resultEvent): OperatorResultInfo => {
          const table = (resultEvent.table || []) as IndexableObject[];
          const { limited, tokenCount, truncated } = filterByTokenLimit(table);
          return {
            mode: "pagination",
            totalRows: paginatedResultService.getCurrentTotalNumTuples(),
            displayedRows: limited.length,
            estimatedTokens: tokenCount,
            truncated,
            tableStats: paginatedResultService.getStats(),
            result: limited,
          };
        }),
        catchError(() => {
          // Fall through to try snapshot result service
          return getSnapshotResult$(operatorId, workflowResultService);
        })
      );
    }

    // Try snapshot result service
    return getSnapshotResult$(operatorId, workflowResultService);
  });
}

/**
 * Get snapshot result as Observable
 */
function getSnapshotResult$(
  operatorId: string,
  workflowResultService: WorkflowResultService
): Observable<OperatorResultInfo | null> {
  const resultService = workflowResultService.getResultService(operatorId);
  if (resultService) {
    const snapshot = resultService.getCurrentResultSnapshot() as IndexableObject[] | null;
    if (snapshot && snapshot.length > 0) {
      const { limited, tokenCount, truncated } = filterByTokenLimit(snapshot);
      return of({
        mode: "snapshot" as const,
        totalRows: snapshot.length,
        displayedRows: limited.length,
        estimatedTokens: tokenCount,
        truncated,
        result: limited,
      });
    }
  }
  return of(null);
}

/**
 * Wait for results to become available with retry using RxJS
 */
function waitForResults$(
  operatorIds: readonly string[],
  workflowResultService: WorkflowResultService
): Observable<string[]> {
  // If no operators to check, return empty immediately
  if (operatorIds.length === 0) {
    return of([]);
  }

  return interval(RESULT_WAIT_DELAY_MS).pipe(
    take(RESULT_WAIT_MAX_RETRIES),
    map(() => operatorIds.filter(id => workflowResultService.hasAnyResult(id))),
    filter(available => available.length > 0),
    take(1),
    // If no results found after all retries (stream completes without emitting), return empty array
    defaultIfEmpty([] as string[]),
    // If any error occurs, also return empty array
    catchError(() => of([] as string[]))
  );
}

/**
 * Collect operator results for given operator IDs using RxJS
 */
function collectOperatorResults$(
  operatorIds: readonly string[],
  workflowResultService: WorkflowResultService
): Observable<Record<string, OperatorResultInfo>> {
  if (operatorIds.length === 0) {
    return of({});
  }

  return waitForResults$(operatorIds, workflowResultService).pipe(
    switchMap(availableIds => {
      if (availableIds.length === 0) {
        return of({});
      }

      const resultObservables = availableIds.map(operatorId =>
        getOperatorResult$(operatorId, workflowResultService).pipe(map(result => ({ operatorId, result })))
      );

      return forkJoin(resultObservables).pipe(
        map(results => {
          const operatorResults: Record<string, OperatorResultInfo> = {};
          for (const { operatorId, result } of results) {
            if (result) {
              operatorResults[operatorId] = result;
            }
          }
          return operatorResults;
        })
      );
    })
  );
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
 * Create getExistingWorkflowExecutionResult tool that retrieves results and console logs
 * from a previous workflow execution. Optionally filters by operator IDs.
 */
export function createGetExistingWorkflowExecutionResultTool(
  workflowResultService: WorkflowResultService,
  workflowConsoleService: WorkflowConsoleService,
  workflowActionService: WorkflowActionService
) {
  return tool({
    name: TOOL_NAME_GET_EXISTING_WORKFLOW_EXECUTION_RESULT,
    description:
      "Get results and console logs from a previous workflow execution. If targetOperatorIds is empty or not provided, " +
      "retrieves results for all operators in the workflow. Returns operator results (limited by token count ~3000 tokens) " +
      "and console logs for each operator. Use this to inspect existing execution results without re-running the workflow.",
    inputSchema: z.object({
      targetOperatorIds: z
        .array(z.string())
        .optional()
        .describe(
          "Optional list of operator IDs to retrieve results for. If empty or not provided, retrieves results for all operators."
        ),
    }),
    execute: async (args: { targetOperatorIds?: string[] }) => {
      try {
        // Determine which operators to check
        const allOperators = workflowActionService.getTexeraGraph().getAllOperators();
        const allOperatorIds = allOperators.map(op => op.operatorID);
        const targetIds =
          args.targetOperatorIds && args.targetOperatorIds.length > 0 ? args.targetOperatorIds : allOperatorIds;

        // Collect console logs for all target operators
        const consoleLogs = collectConsoleLogs(targetIds, workflowConsoleService);

        // Collect results for all target operators
        const operatorResults: Record<string, OperatorResultInfo | { error: string }> = {};

        for (const operatorId of targetIds) {
          // Try paginated result service first
          const paginatedResultService = workflowResultService.getPaginatedResultService(operatorId);
          if (paginatedResultService) {
            try {
              const resultEvent = await firstValueFrom(
                paginatedResultService.selectPage(1, 200).pipe(timeout(RESULT_FETCH_TIMEOUT_MS))
              );
              const table = (resultEvent.table || []) as IndexableObject[];
              const { limited, tokenCount, truncated } = filterByTokenLimit(table);
              const totalRows = paginatedResultService.getCurrentTotalNumTuples();

              operatorResults[operatorId] = {
                mode: "pagination",
                totalRows,
                displayedRows: limited.length,
                estimatedTokens: tokenCount,
                truncated,
                tableStats: paginatedResultService.getStats(),
                result: limited,
              };
              continue;
            } catch {
              // Fall through to try snapshot
            }
          }

          // Try snapshot result service
          const resultService = workflowResultService.getResultService(operatorId);
          if (resultService) {
            const snapshot = resultService.getCurrentResultSnapshot() as IndexableObject[] | null;
            if (snapshot && snapshot.length > 0) {
              const { limited, tokenCount, truncated } = filterByTokenLimit(snapshot);
              operatorResults[operatorId] = {
                mode: "snapshot",
                totalRows: snapshot.length,
                displayedRows: limited.length,
                estimatedTokens: tokenCount,
                truncated,
                result: limited,
              };
              continue;
            }
          }

          // No results available for this operator - mark as empty (not an error)
          // Don't add anything, the operator simply has no results
        }

        const operatorsWithResults = Object.keys(operatorResults).length;
        const operatorsWithLogs = Object.keys(consoleLogs).length;

        return createSuccessResult(
          {
            targetOperatorIds: targetIds,
            operatorResults,
            consoleLogs,
            summary: {
              totalOperatorsChecked: targetIds.length,
              operatorsWithResults,
              operatorsWithConsoleLogs: operatorsWithLogs,
            },
            message:
              `Retrieved execution results for ${operatorsWithResults}/${targetIds.length} operators ` +
              `and console logs for ${operatorsWithLogs} operators.`,
          },
          targetIds,
          []
        );
      } catch (error: unknown) {
        return createErrorResult(error instanceof Error ? error.message : String(error));
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
      } catch (error: unknown) {
        return createErrorResult(error instanceof Error ? error.message : String(error));
      }
    },
  });
}

/**
 * Computing unit status service interface (minimal for type safety)
 */
interface ComputingUnitStatusService {
  getSelectedComputingUnitValue(): { status: string; computingUnit: { cuid: number; name: string } } | null;
}

/**
 * Create getComputingUnitStatus tool for checking computing unit connection status
 */
export function createGetCurrentComputingUnitStatusTool(computingUnitStatusService: ComputingUnitStatusService) {
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
      } catch (error: unknown) {
        return createErrorResult(error instanceof Error ? error.message : String(error));
      }
    },
  });
}
