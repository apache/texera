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

import type { AgentState, ReActStep } from "../agent";
import type { WorkflowContent } from "../workflow";
import type { CustomUnionType } from "../util";

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

/**
 * Full state pushed once when a client connects: the agent's current lifecycle
 * state, the complete step list, and the HEAD pointer. Operator results are not
 * included — they are pulled on demand via `GET /operator-results`.
 */
export interface WsServerSnapshotEvent extends Readonly<{
  state: AgentState;
  steps: ReActStep[];
  headId: string;
}> {}

/** A single ReAct step, streamed live as the agent runs. */
export interface WsServerStepEvent extends Readonly<{
  step: ReActStep;
}> {}

/**
 * An agent lifecycle transition (e.g. GENERATING when a run starts, the resting
 * state when it ends, STOPPING on stop).
 */
export interface WsServerStatusEvent extends Readonly<{
  state: AgentState;
}> {}

/** An error surfaced to the client (agent not found, bad request, failed run). */
export interface WsServerErrorEvent extends Readonly<{
  error: string;
}> {}

/**
 * Emitted after a checkout: HEAD moved, carrying the full step list and the
 * workflow snapshot at the new head.
 *
 * @deprecated Redundant and unused. TODO: remove this message and related caller logics.
 */
export interface WsServerHeadChangeEvent extends Readonly<{
  headId: string;
  steps: ReActStep[];
  workflowContent?: WorkflowContent;
}> {}

export type WsServerEventTypeMap = {
  snapshot: WsServerSnapshotEvent;
  step: WsServerStepEvent;
  status: WsServerStatusEvent;
  error: WsServerErrorEvent;
  headChange: WsServerHeadChangeEvent;
};

/** Discriminated union of every server -> client frame. */
export type WsServerEvent = CustomUnionType<WsServerEventTypeMap>;
