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
import {
  ConsoleMessageType,
  OperatorState,
  WorkflowExecutionState,
  WorkflowFatalErrorType,
  type OperatorExecutionSummary,
  type Tuple,
  type WebOutputMode,
  type WorkflowExecutionSummary,
  type WorkflowFatalError,
} from "../types/execution";

export interface LogicalLink {
  fromOpId: string;
  fromPortId: { id: number; internal: boolean };
  toOpId: string;
  toPortId: { id: number; internal: boolean };
}

interface LogicalOperator {
  operatorID: string;
  operatorType: string;
  [key: string]: any;
}

export interface LogicalPlan {
  operators: LogicalOperator[];
  links: LogicalLink[];
  opsToViewResult?: string[];
  opsToReuseResult?: string[];
}

const legacyConsoleMessageSchema = z.object({
  msgType: z.nativeEnum(ConsoleMessageType),
  title: z.string().default(""),
  message: z.string(),
});

const legacyResultModeSchema = z.enum(["table", "visualization"]);
type LegacyResultMode = z.output<typeof legacyResultModeSchema>;

const legacyResultModeToWebOutputMode = {
  table: { type: "PaginationMode" },
  visualization: { type: "SetSnapshotMode" },
} satisfies Record<LegacyResultMode, WebOutputMode>;

const legacyOperatorInfoSchema = z
  .object({
    state: z.string(),
    inputTuples: z.number(),
    outputTuples: z.number(),
    resultMode: legacyResultModeSchema,
    result: z.array(z.record(z.unknown())).nullish(),
    totalRowCount: z.number().int().nonnegative().nullish(),
    displayedRows: z.number().int().nonnegative().nullish(),
    truncated: z.boolean().nullish(),
    consoleLogs: z.array(legacyConsoleMessageSchema).nullish(),
    error: z.string().nullish(),
    warnings: z.array(z.string()).nullish(),
  })
  .passthrough();

const legacySyncExecutionResultSchema = z
  .object({
    success: z.boolean(),
    state: z.string(),
    operators: z.record(legacyOperatorInfoSchema),
    compilationErrors: z.record(z.string()).nullish(),
    errors: z.array(z.string()).nullish(),
  })
  .passthrough();

export type LegacySyncExecutionResult = z.input<typeof legacySyncExecutionResultSchema>;

const internalResultKeys = new Set(["__row_index__", "__is_visualization__"]);
const operatorStates = new Set<string>(Object.values(OperatorState));
const workflowStates = new Set<string>(Object.values(WorkflowExecutionState));

function normalizeOperatorState(state: string): OperatorState {
  return operatorStates.has(state) ? (state as OperatorState) : OperatorState.UNKNOWN;
}

// Keep the legacy boolean at the wire boundary: canonical consumers use state alone,
// so an unclean legacy completion must become an explicit failure state.
function normalizeWorkflowState(state: string, success: boolean): WorkflowExecutionState {
  const normalized = workflowStates.has(state) ? (state as WorkflowExecutionState) : WorkflowExecutionState.UNKNOWN;
  return !success && normalized === WorkflowExecutionState.COMPLETED ? WorkflowExecutionState.FAILED : normalized;
}

function normalizeCellValue(value: unknown): unknown {
  if (value === null) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);

  const serialized = JSON.stringify(value);
  return serialized === undefined ? String(value) : serialized;
}

function legacyRowToTuple(row: Record<string, unknown>, fallbackIndex: number): [number, Tuple] {
  const rowIndexValue = row["__row_index__"];
  if (rowIndexValue !== undefined && (!Number.isInteger(rowIndexValue) || (rowIndexValue as number) < 0)) {
    throw new Error(`Invalid __row_index__ for sampled row ${fallbackIndex}`);
  }
  const rowIndex = rowIndexValue === undefined ? fallbackIndex : (rowIndexValue as number);
  const entries = Object.entries(row).filter(([name]) => !internalResultKeys.has(name));

  return [
    rowIndex,
    {
      schema: {
        attributes: entries.map(([attributeName]) => ({ attributeName, attributeType: "string" })),
      },
      fields: entries.map(([, value]) => normalizeCellValue(value)),
    },
  ];
}

function makeWorkflowError(type: WorkflowFatalErrorType, message: string, operatorId: string): WorkflowFatalError {
  const now = Date.now();
  return {
    type: { name: type },
    timestamp: { seconds: Math.floor(now / 1000), nanos: (now % 1000) * 1_000_000 },
    message,
    details: "",
    operatorId,
    workerId: "",
  };
}

function adaptOperatorInfo(
  operatorId: string,
  operator: z.output<typeof legacyOperatorInfoSchema>
): OperatorExecutionSummary {
  const resultSummary =
    operator.result == null
      ? undefined
      : {
          resultMode: legacyResultModeToWebOutputMode[operator.resultMode],
          sampleTuples: operator.result.map(legacyRowToTuple),
          totalTuplesCount: operator.totalRowCount ?? operator.outputTuples,
        };

  const consoleMessages = operator.consoleLogs?.length ? operator.consoleLogs : undefined;
  const errorMessages = operator.error
    ? [makeWorkflowError(WorkflowFatalErrorType.EXECUTION_FAILURE, operator.error, operatorId)]
    : [];

  return {
    state: normalizeOperatorState(operator.state),
    errorMessages,
    resultSummary,
    consoleMessages,
  };
}

/**
 * Compatibility boundary for the current Scala sync-execution response.
 *
 * The backend is intentionally left on its legacy contract in this change. Its execution
 * result model will be redesigned and refactored separately; remove these legacy schemas
 * and this adapter once that backend work lands.
 */
export function adaptLegacySyncExecutionResult(input: unknown): WorkflowExecutionSummary {
  const parsed = legacySyncExecutionResultSchema.safeParse(input);
  if (!parsed.success) {
    throw new Error(`Invalid legacy sync-execution response: ${parsed.error.message}`);
  }

  const legacy = parsed.data;
  const operators = Object.fromEntries(
    Object.entries(legacy.operators).map(([operatorId, operator]) => [
      operatorId,
      adaptOperatorInfo(operatorId, operator),
    ])
  );

  const compilationMessages = new Set(Object.values(legacy.compilationErrors ?? {}));
  const allMessages = [...compilationMessages, ...(legacy.errors ?? [])];
  const errors = [...new Set(allMessages)].map(message =>
    makeWorkflowError(
      legacy.state === WorkflowExecutionState.COMPILATION_FAILED || compilationMessages.has(message)
        ? WorkflowFatalErrorType.COMPILATION_ERROR
        : WorkflowFatalErrorType.EXECUTION_FAILURE,
      message,
      ""
    )
  );

  return {
    state: normalizeWorkflowState(legacy.state, legacy.success),
    operators,
    errors,
  };
}
