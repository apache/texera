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
// (`/agents/:id/react`). Each frame is a class whose `type` discriminator
// equals its class name, so `new WsServerStatusEvent(...)` sets the wire tag
// for you. `WsServerEvent` is their discriminated union.

import type { AgentState, ReActStep } from "../agent";

/**
 * Full state pushed once when a client connects: the agent's current lifecycle
 * state, the complete step list, and the HEAD pointer. Operator results are not
 * included — they are pulled on demand via `GET /agents/:id/operator-results`.
 */
export class WsServerSnapshotEvent {
  readonly type = "WsServerSnapshotEvent";
  constructor(
    readonly state: AgentState,
    readonly steps: ReActStep[],
    readonly headId: string,
    // Deliberately no workflowContent: the agent's in-memory copy can lag the
    // backend (it never sees manual canvas edits), so pushing it on connect
    // would overwrite the user's canvas with stale state. Canvas sync stays
    // owned by backend polling; only live revert/redo events carry content.
    readonly canRedo: boolean = false
  ) {}
}

/** A single ReAct step, streamed live as the agent runs. */
export class WsServerStepEvent {
  readonly type = "WsServerStepEvent";
  constructor(readonly step: ReActStep) {}
}

/**
 * An agent lifecycle transition (e.g. GENERATING when a run starts, the resting
 * state when it ends, STOPPING on stop).
 */
export class WsServerStatusEvent {
  readonly type = "WsServerStatusEvent";
  constructor(readonly state: AgentState) {}
}

/** An error surfaced to the client (agent not found, bad request, failed run). */
export class WsServerErrorEvent {
  readonly type = "WsServerErrorEvent";
  constructor(readonly error: string) {}
}

/**
 * HEAD moved without a new step being produced — emitted after a revert. Carries
 * the new HEAD id and the workflow content at that point, so clients can recompute
 * the visible step path and reload the canvas.
 */
export class WsServerHeadChangeEvent {
  readonly type = "WsServerHeadChangeEvent";
  constructor(
    readonly headId: string,
    readonly workflowContent: any,
    readonly canRedo: boolean = false
  ) {}
}

/** Discriminated union of every server -> client frame. */
export type WsServerEvent =
  WsServerSnapshotEvent | WsServerStepEvent | WsServerStatusEvent | WsServerErrorEvent | WsServerHeadChangeEvent;
