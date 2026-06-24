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
// (`/agents/:id/react`). Modeled as a discriminated union on `type`, so each
// message kind declares exactly the fields it sends.

import type { AgentState, ReActStep } from "../agent";
import type { WorkflowContent } from "../workflow";

/**
 * Wire projection of one operator's execution result, summarized for the
 * client: counts and a small record sample instead of full payloads.
 */
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

/** Per-operator result summaries, keyed by operator id. */
type OperatorResults = Record<string, OperatorResultSummaryWs>;

/** Shared discriminator base; every server frame sets a unique `type`. */
interface WsServerMessageBase {
  type: "snapshot" | "step" | "status" | "completion" | "error" | "headChange";
}

/**
 * Full state pushed once when a client connects: the agent's current lifecycle
 * state, the complete step list, the HEAD pointer, and the latest operator
 * results.
 */
export interface WsServerSnapshotMessage extends WsServerMessageBase {
  type: "snapshot";
  state: AgentState;
  steps: ReActStep[];
  headId: string;
  operatorResults: OperatorResults;
}

/**
 * A single ReAct step, streamed live as the agent runs. Carries operator
 * results when the step ran tools.
 */
export interface WsServerStepMessage extends WsServerMessageBase {
  type: "step";
  step: ReActStep;
  operatorResults?: OperatorResults;
}

/**
 * An agent lifecycle transition (e.g. GENERATING when a run starts, the resting
 * state when it ends, STOPPING on stop).
 */
export interface WsServerStatusMessage extends WsServerMessageBase {
  type: "status";
  state: AgentState;
}

/**
 * Terminal frame for a finished run: the final authoritative operator-results
 * snapshot. The agent's resting state is delivered separately via a `status`
 * frame emitted at end-of-run, so completion is purely about results.
 */
export interface WsServerCompletionMessage extends WsServerMessageBase {
  type: "completion";
  operatorResults: OperatorResults;
}

/** An error surfaced to the client (agent not found, bad request, failed run). */
export interface WsServerErrorMessage extends WsServerMessageBase {
  type: "error";
  error: string;
}

/**
 * Emitted after a checkout: HEAD moved, carrying the full step list and the
 * workflow snapshot at the new head.
 *
 * @deprecated Redundant and unused — the checkout flow that produces this frame
 * is unreachable in the product (nothing invokes the client's `checkoutStep()`).
 * Scheduled for removal (see #5930); do not build new code on it.
 */
export interface WsServerHeadChangeMessage extends WsServerMessageBase {
  type: "headChange";
  headId: string;
  steps: ReActStep[];
  workflowContent?: WorkflowContent;
  operatorResults: OperatorResults;
}

/** Discriminated union of every server -> client frame. */
export type WsServerMessage =
  | WsServerSnapshotMessage
  | WsServerStepMessage
  | WsServerStatusMessage
  | WsServerCompletionMessage
  | WsServerErrorMessage
  | WsServerHeadChangeMessage;
