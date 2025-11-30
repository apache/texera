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
import {
  ExecutionState,
  ExecutionStateInfo,
  OperatorStatistics,
  WorkflowResultTableStats,
} from "../../../types/execute-workflow.interface";
import { ConsoleMessage, OperatorPredicate } from "../../../types/workflow-common.interface";
import { IndexableObject } from "../../../types/result-table.interface";
import { PaginatedResultEvent } from "../../../types/workflow-websocket.interface";
import {
  estimateTokenCount,
  MAX_OPERATOR_RESULT_TOKEN_LIMIT,
  createSuccessResult,
  createErrorResult,
} from "./tools-utility";
import { Observable, of, throwError, defer, timer, forkJoin, interval, merge, EMPTY, firstValueFrom } from "rxjs";
import { filter, timeout, map, switchMap, catchError, take, tap } from "rxjs/operators";

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
  numWorkers?: string;
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
      ...(stats.numWorkers !== undefined && { numWorkers: `${stats.numWorkers} workers` }),
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

      // Detect stuck state (Running but operator Paused)
      const stuckDetection$ = interval(1000).pipe(
        filter(() => {
          const state = executeWorkflowService.getExecutionState();
          if (state.state !== ExecutionState.Running) return false;
          const opStates = workflowStatusService.getCurrentStatus();
          return Object.values(opStates).some(s => s.operatorState === "Paused");
        }),
        take(1),
        tap(() => {
          try {
            executeWorkflowService.killWorkflow();
          } catch {
            /* ignore */
          }
        }),
        switchMap(() => EMPTY)
      );

      return merge(executionState$, stuckDetection$).pipe(
        take(1),
        timeout(EXECUTION_TIMEOUT_MS),
        map(finalState => ({ finalState, allOperators })),
        catchError((error: unknown) => {
          if (error instanceof Error && error.name === "TimeoutError") {
            return throwError(
              () => new Error(`Execution timed out after ${EXECUTION_TIMEOUT_MS / 1000}s.`)
            );
          }
          return throwError(() => error);
        })
      );
    }),
    // Step 4: Collect results
    switchMap(({ finalState, allOperators }) => {
      const finalStateInfo = executeWorkflowService.getExecutionState();
      const allOperatorIds = allOperators.map(op => op.operatorID);
      const consoleLogs = collectConsoleLogs(allOperatorIds, workflowConsoleService);
      const formattedStates = formatOperatorStates(workflowStatusService.getCurrentStatus());

      if (!includeResults) {
        return of({ finalState, finalStateInfo, formattedStates, consoleLogs, operatorResults: {} });
      }

      // Collect results only for target operators (or all if empty)
      const opsToFetch = targetIds.length > 0 ? targetIds : allOperatorIds;
      const resultObs: Observable<{ operatorId: string; result: OperatorResultInfo | null }>[] = opsToFetch
        .filter(id => workflowResultService.hasAnyResult(id))
        .map(operatorId =>
          getOperatorResult$(operatorId, workflowResultService).pipe(
            map(result => ({ operatorId, result })),
            catchError(() => of({ operatorId, result: null }))
          )
        );

      const results$ = resultObs.length > 0 ? forkJoin(resultObs) : of([]);

      return results$.pipe(
        map(results => {
          const operatorResults: Record<string, OperatorResultInfo> = {};
          for (const { operatorId, result } of results) {
            if (result) operatorResults[operatorId] = result;
          }
          return { finalState, finalStateInfo, formattedStates, consoleLogs, operatorResults };
        })
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

      if (finalState.state === ExecutionState.Failed) {
        return {
          ...baseError,
          executionState: "Failed",
          message: "Workflow execution failed",
          error: `Failed. Errors: ${JSON.stringify(errorMessages)}`,
        };
      }
      if (finalState.state === ExecutionState.Killed) {
        return {
          ...baseError,
          executionState: "Killed",
          message: "Workflow execution was killed",
          error: "Execution was killed.",
        };
      }
      if (finalState.state === ExecutionState.Paused) {
        return {
          ...baseError,
          executionState: "Paused",
          message: "Workflow paused (likely error)",
          error: `Paused. Errors: ${JSON.stringify(errorMessages)}`,
        };
      }

      return {
        ...baseError,
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
 * Convenience wrapper that converts the Observable to a Promise.
 * Use executeWorkflowAndGetResults$ directly for better RxJS integration.
 */
export function executeWorkflowAndGetResults(
  services: WorkflowExecutionServices,
  options: ExecuteWorkflowOptions
): Promise<ExecuteWorkflowResult> {
  return firstValueFrom(executeWorkflowAndGetResults$(services, options));
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
  const services: WorkflowExecutionServices = {
    executeWorkflowService,
    validationWorkflowService,
    workflowActionService,
    workflowConsoleService,
    workflowStatusService,
    workflowResultService,
  };

  return tool({
    name: TOOL_NAME_EXECUTE_CURRENT_WORKFLOW,
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
      const result = await executeWorkflowAndGetResults(services, {
        executionName: args.executionName,
        targetOperatorIds: args.targetOperatorIds,
        includeOperatorResults: true,
      });

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
        // Include console logs and operator states even on failure for debugging
        return {
          success: false,
          error: result.error || result.message,
          executionState: result.executionState,
          message: result.message,
          operatorStates: result.operatorStates,
          consoleLogs: result.consoleLogs,
          viewedOperatorIds: [],
          modifiedOperatorIds: [],
        };
      }
    },
  });
}

/**
 * Helper function to get operator result with token limit (Observable-based)
 */
function getOperatorResult$(
  operatorId: string,
  workflowResultService: WorkflowResultService
): Observable<OperatorResultInfo> {
  return defer(() => {
    const paginatedResultService = workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      return paginatedResultService.selectPage(1, 200).pipe(
        take(1),
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
            result: { ...resultEvent, table: limited } as PaginatedResultEvent,
          };
        })
      );
    }

    const resultService = workflowResultService.getResultService(operatorId);
    if (resultService) {
      const snapshot = resultService.getCurrentResultSnapshot() as IndexableObject[] | null;
      if (!snapshot?.length) {
        return throwError(() => new Error("Result snapshot is empty"));
      }
      const { limited, tokenCount, truncated } = filterByTokenLimit(snapshot);
      return of<OperatorResultInfo>({
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
        const allOperatorIds = workflowActionService.getTexeraGraph().getAllOperators().map(op => op.operatorID);
        return createSuccessResult(
          {
            state: executeWorkflowService.getExecutionState(),
            consoleLogs: collectConsoleLogs(allOperatorIds, workflowConsoleService),
            operatorStates: formatOperatorStates(workflowStatusService.getCurrentStatus()),
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
      } catch (error: unknown) {
        return createErrorResult(error instanceof Error ? error.message : String(error));
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
      } catch (error: unknown) {
        return createErrorResult(error instanceof Error ? error.message : String(error));
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
            const resultEvent = await firstValueFrom(paginatedResultService.selectPage(1, 200));
            const table = (resultEvent.table || []) as IndexableObject[];
            const { limited, tokenCount, truncated } = filterByTokenLimit(table);
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
          } catch (error: unknown) {
            const msg = error instanceof Error ? error.message : String(error);
            return createErrorResult(
              `Failed to fetch paginated results: ${msg}. This may be due to backend storage issues or results not being ready yet.`
            );
          }
        }

        const resultService = workflowResultService.getResultService(args.operatorId);
        if (resultService) {
          const snapshot = resultService.getCurrentResultSnapshot() as IndexableObject[] | null;
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
