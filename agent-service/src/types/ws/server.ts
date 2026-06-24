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

import type { ReActStep } from "../agent";
import type { OperatorExecutionSummary } from "../execution";
import type { WorkflowContent } from "../workflow";

// The server streams the canonical per-operator execution summaries straight to
// the client, keyed by operator id.
type OperatorResults = Record<string, OperatorExecutionSummary>;

interface WsServerMessageBase {
  type: "init" | "step" | "state" | "complete" | "error" | "headChange";
}

// Sent once on connect: a snapshot of the agent's current state and steps.
export interface WsServerInitMessage extends WsServerMessageBase {
  type: "init";
  state: string;
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
export interface WsServerStateMessage extends WsServerMessageBase {
  type: "state";
  state: string;
}

// Terminal message for a finished run.
export interface WsServerCompleteMessage extends WsServerMessageBase {
  type: "complete";
  state: string;
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
  | WsServerInitMessage
  | WsServerStepMessage
  | WsServerStateMessage
  | WsServerCompleteMessage
  | WsServerErrorMessage
  | WsServerHeadChangeMessage;
