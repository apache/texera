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

export enum WorkflowFatalErrorType {
  COMPILATION_ERROR = "COMPILATION_ERROR",
  EXECUTION_FAILURE = "EXECUTION_FAILURE",
}

// A fatal error reported for one operator. Reuses the engine's wire shape
// (workflowruntimestate.proto). The same type the workflow-compiling service
// returns for compilation errors, so compile and execution errors share one
// shape. Re-exported by api/compile-api.ts.
export interface WorkflowFatalError {
  type: { name: WorkflowFatalErrorType };
  timestamp: { seconds: number; nanos: number };
  message: string;
  details: string;
  operatorId: string;
  workerId: string;
}

// Lifecycle state of a single operator, as reported by the engine
// (mirrors the backend's WorkflowAggregatedState string mapping).
export enum OperatorState {
  UNINITIALIZED = "Uninitialized",
  READY = "Ready",
  RUNNING = "Running",
  PAUSING = "Pausing",
  PAUSED = "Paused",
  RESUMING = "Resuming",
  COMPLETED = "Completed",
  FAILED = "Failed",
  KILLED = "Killed",
  TERMINATED = "Terminated",
  UNKNOWN = "Unknown",
}

// Aggregated state of a whole workflow execution: the OperatorState values the
// engine reports, plus the synthetic outcomes the sync-execution endpoint adds.
export enum WorkflowExecutionState {
  UNINITIALIZED = "Uninitialized",
  READY = "Ready",
  RUNNING = "Running",
  PAUSING = "Pausing",
  PAUSED = "Paused",
  RESUMING = "Resuming",
  COMPLETED = "Completed",
  FAILED = "Failed",
  KILLED = "Killed",
  TERMINATED = "Terminated",
  UNKNOWN = "Unknown",
  ERROR = "Error",
  COMPILATION_FAILED = "CompilationFailed",
}

export enum ConsoleMessageType {
  PRINT = "PRINT",
  ERROR = "ERROR",
  COMMAND = "COMMAND",
  DEBUGGER = "DEBUGGER",
}

// A reduced console-message projection for sync-execution summaries. The engine
// proto also has workerId/timestamp/source; this summary keeps only the fields
// consumed by agent-service.
export interface ConsoleMessageSummary {
  msgType: ConsoleMessageType;
  title: string;
  message: string;
}

// One sampled output row: its original position plus the row's columns.
export interface SampleRow {
  rowIndex: number;
  tuple: Record<string, unknown>;
}

export enum OperatorResultMode {
  TABLE = "table",
  VISUALIZATION = "visualization",
}

// An operator's output, summarized for the agent. `sampleTuples` are the
// symmetrically-truncated output rows (the middle is dropped, so `rowIndex`
// values may have gaps). `outputSchema` / per-column statistics are intended
// future additions — the engine does not produce them yet.
export interface OperatorResultSummary {
  resultMode: OperatorResultMode;
  sampleTuples: SampleRow[];
  // Total output rows before truncation (sampleTuples may hold fewer).
  tuplesCount: number;
}

// Per-operator execution summary returned by the sync-execution backend.
// Orthogonal sub-summaries replace the previous flat `OperatorInfo`.
export interface OperatorExecutionSummary {
  state: OperatorState;
  // Empty means the operator did not fail.
  errorMessages: ReadonlyArray<WorkflowFatalError>;
  // Absent when the operator produced no materialized result.
  resultSummary?: OperatorResultSummary;
  // Absent when the operator produced no console output.
  consoleMessages?: ConsoleMessageSummary[];
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
