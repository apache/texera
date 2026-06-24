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

// Server -> client WebSocket frames for this service's protocol
// (/agents/:id/react). Modeled as a discriminated union on `type` so each
// message kind declares exactly the fields it sends.

import type { AgentState, ReActStep } from "../agent";
import type { WorkflowContent } from "../workflow";

// Wire projection of an operator's execution result, summarized for the client
// (counts instead of full payloads; only a sample of records).
export interface OperatorResultSummaryWs {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: { portIndex: number; rows: number; columns: number }[];
  outputColumns?: number;
  error?: string;
  warnings?: string[];
  consoleLogCount?: number;
  totalRowCount?: number;
  sampleRecords?: Record<string, unknown>[];
  resultStatistics?: Record<string, string>;
}

type OperatorResults = Record<string, OperatorResultSummaryWs>;

interface WsServerMessageBase {
  type: "snapshot" | "step" | "status" | "completion" | "error" | "headChange";
}

// Sent once on connect: a snapshot of the agent's current state and steps.
export interface WsServerSnapshotMessage extends WsServerMessageBase {
  type: "snapshot";
  state: AgentState;
  steps: ReActStep[];
  headId: string;
  operatorResults: OperatorResults;
}

// A single ReAct step streamed as the agent runs. Operator results accompany
// steps that ran tools.
export interface WsServerStepMessage extends WsServerMessageBase {
  type: "step";
  step: ReActStep;
  operatorResults?: OperatorResults;
}

// An agent lifecycle transition (e.g. GENERATING, STOPPING).
export interface WsServerStatusMessage extends WsServerMessageBase {
  type: "status";
  state: AgentState;
}

// Terminal message for a finished run.
export interface WsServerCompletionMessage extends WsServerMessageBase {
  type: "completion";
  state: AgentState;
  operatorResults: OperatorResults;
}

// An error surfaced to the client.
export interface WsServerErrorMessage extends WsServerMessageBase {
  type: "error";
  error: string;
}

// Emitted after a checkout: the head moved, carrying the full step list and the
// workflow snapshot at the new head.
export interface WsServerHeadChangeMessage extends WsServerMessageBase {
  type: "headChange";
  headId: string;
  steps: ReActStep[];
  workflowContent?: WorkflowContent;
  operatorResults: OperatorResults;
}

export type WsServerMessage =
  | WsServerSnapshotMessage
  | WsServerStepMessage
  | WsServerStatusMessage
  | WsServerCompletionMessage
  | WsServerErrorMessage
  | WsServerHeadChangeMessage;
