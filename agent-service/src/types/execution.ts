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

// Canonical agent-service error projection. It follows the engine's
// workflowruntimestate.proto shape so compile and execution errors share one model.
// The legacy sync-execution adapter synthesizes metadata the backend does not yet emit.
// Re-exported by api/compile-api.ts.
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

// A normalized result row using the engine Tuple shape: a schema plus positional fields.
// The legacy backend returns JSON records, so the adapter builds a synthetic all-STRING
// schema after the backend has truncated values for display.
export interface Attribute {
  attributeName: string;
  attributeType: string;
}

export interface Schema {
  attributes: Attribute[];
}

export interface Tuple {
  schema: Schema;
  fields: unknown[];
}

// Mirrors ExecutionResultService.WebOutputMode's JSON representation.
export type PaginationMode = Readonly<{ type: "PaginationMode" }>;
export type SetSnapshotMode = Readonly<{ type: "SetSnapshotMode" }>;
export type SetDeltaMode = Readonly<{ type: "SetDeltaMode" }>;
export type WebOutputMode = PaginationMode | SetSnapshotMode | SetDeltaMode;

// An operator's output summary. Sample tuples carry their original row index.
export interface OperatorResultSummary {
  resultMode: WebOutputMode;
  sampleTuples: [number, Tuple][];
  totalTuplesCount: number;
}

// Canonical per-operator summary used inside agent-service and exposed to the frontend.
// The legacy backend's flat `OperatorInfo` is converted at the execution API boundary.
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
  state: WorkflowExecutionState;
  operators: Record<string, OperatorExecutionSummary>;
  // Workflow-level errors (timeouts, init/compile failures, fatal errors);
  // empty means none. For workflow-level failures, operatorId/workerId are empty.
  errors: WorkflowFatalError[];
}
