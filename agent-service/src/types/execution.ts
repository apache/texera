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
// Re-exported by api/compile-api.ts.
export interface WorkflowFatalError {
  readonly type: Readonly<{ name: WorkflowFatalErrorType }>;
  readonly timestamp: Readonly<{ seconds: number; nanos: number }>;
  readonly message: string;
  readonly details: string;
  readonly operatorId: string;
  readonly workerId: string;
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
// engine reports, plus agent-service execution outcomes.
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

// A reduced console-message projection. The engine proto also has
// workerId/timestamp/source; this summary keeps only the fields consumed by agent-service.
export interface ConsoleMessageSummary {
  readonly msgType: ConsoleMessageType;
  readonly title: string;
  readonly message: string;
}

// A normalized result tuple using the engine Tuple shape: a schema plus positional fields.
export interface Attribute {
  readonly attributeName: string;
  readonly attributeType: string;
}

export interface Schema {
  readonly attributes: ReadonlyArray<Attribute>;
}

export interface Tuple {
  readonly schema: Schema;
  readonly fields: ReadonlyArray<unknown>;
}

export type IndexedTuple = readonly [rowIndex: number, tuple: Tuple];

// Mirrors ExecutionResultService.WebOutputMode's JSON representation.
export type PaginationMode = Readonly<{ type: "PaginationMode" }>;
export type SetSnapshotMode = Readonly<{ type: "SetSnapshotMode" }>;
export type SetDeltaMode = Readonly<{ type: "SetDeltaMode" }>;
export type WebOutputMode = PaginationMode | SetSnapshotMode | SetDeltaMode;

// An operator's output summary. Sample tuples carry their original row index.
export interface OperatorResultSummary {
  readonly resultMode: WebOutputMode;
  readonly sampleTuples: ReadonlyArray<IndexedTuple>;
  readonly totalTuplesCount: number;
}

// Canonical per-operator summary used inside agent-service and exposed to the frontend.
export interface OperatorExecutionSummary {
  readonly state: OperatorState;
  // Empty means the operator did not fail.
  readonly errorMessages: ReadonlyArray<WorkflowFatalError>;
  // Absent when the operator produced no materialized result.
  readonly resultSummary?: OperatorResultSummary;
  // Absent when the operator produced no console output.
  readonly consoleMessages?: ReadonlyArray<ConsoleMessageSummary>;
}

// The result of one workflow execution.
export interface WorkflowExecutionSummary {
  readonly state: WorkflowExecutionState;
  readonly operators: Readonly<Record<string, OperatorExecutionSummary>>;
  // Workflow-level errors (timeouts, init/compile failures, fatal errors);
  // empty means none. For workflow-level failures, operatorId/workerId are empty.
  readonly errors: ReadonlyArray<WorkflowFatalError>;
}
