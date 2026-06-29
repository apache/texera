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

// A fatal error reported for one operator. Reuses the engine's wire shape
// (workflowruntimestate.proto): `type` is the FatalErrorType enum name. The same
// type the workflow-compiling service returns for compilation errors, so compile
// and execution errors share one shape. Re-exported by api/compile-api.ts.
export interface WorkflowFatalError {
  // FatalErrorType enum name, e.g. "COMPILATION_ERROR" | "EXECUTION_FAILURE".
  type: string;
  message: string;
  details?: string;
  operatorId?: string;
  workerId?: string;
  timestamp?: { seconds: number; nanos: number };
}

// Lifecycle state of a single operator, as reported by the engine
// (mirrors the backend's WorkflowAggregatedState string mapping).
export type OperatorState =
  | "Uninitialized"
  | "Ready"
  | "Running"
  | "Pausing"
  | "Paused"
  | "Resuming"
  | "Completed"
  | "Failed"
  | "Killed"
  | "Terminated"
  | "Unknown";

// Aggregated state of a whole workflow execution: the OperatorState values the
// engine reports, plus the synthetic outcomes the sync-execution endpoint adds.
export type WorkflowExecutionState = OperatorState | "Error" | "CompilationFailed";

// A single console message emitted by an operator during execution.
// `title` is the short header (Scala errors put their text here); `message` is
// the body (Python errors / stack traces).
export interface ConsoleMessage {
  msgType: string;
  title: string;
  message: string;
}

// One sampled output row: its original position plus the row's columns. (A viz
// payload's tuple still carries an `__is_visualization__` marker.)
export interface SampleRow {
  rowIndex: number;
  tuple: Record<string, unknown>;
}

// An operator's output, summarized for the agent. `sampleTuples` are the
// symmetrically-truncated output rows (the middle is dropped, so `rowIndex`
// values may have gaps). `outputSchema` / per-column statistics are intended
// future additions — the engine does not produce them yet.
export interface OperatorOutputSummary {
  // "table" or "visualization".
  resultMode: string;
  sampleTuples: SampleRow[];
  // Total output rows before truncation (sampleTuples may hold fewer).
  totalRowCount: number;
}

// An operator's console output. Warnings are not a separate field: they are the
// messages whose `title` the engine prefixes with "WARNING: ", derived on demand.
export interface OperatorConsoleLogsSummary {
  messages: ConsoleMessage[];
}

// Per-operator execution summary returned by the sync-execution backend.
// Orthogonal sub-summaries replace the previous flat `OperatorInfo`.
export interface OperatorExecutionSummary {
  state: OperatorState;
  // Empty means the operator did not fail.
  errorMessages: ReadonlyArray<WorkflowFatalError>;
  // Absent when the operator produced no materialized result.
  resultSummary?: OperatorOutputSummary;
  // Absent when the operator produced no console output.
  consoleLogsSummary?: OperatorConsoleLogsSummary;
}

// The result of one synchronous workflow execution.
export interface WorkflowExecutionSummary {
  // True only on a clean run; can be false even when state is "Completed"
  // (e.g. an operator logged a console error without aborting the run).
  success: boolean;
  state: WorkflowExecutionState;
  operators: Record<string, OperatorExecutionSummary>;
  // Workflow-level errors (timeouts, init/compile failures, fatal errors);
  // empty means none.
  errors: string[];
}
