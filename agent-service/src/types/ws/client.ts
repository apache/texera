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

// Client -> server WebSocket frames for this service's protocol
// (`/agents/:id/react`). Modeled as a discriminated union on `type`, so each
// request kind carries only its own fields.

/** Shared discriminator base; every client request sets a unique `type`. */
interface WsClientRequestBase {
  type: "prompt" | "command";
}

/**
 * A user prompt for the agent to run. `messageSource` notes where it
 * originated (interactive chat vs. an operator feedback action).
 */
export interface WsClientRequestPrompt extends WsClientRequestBase {
  type: "prompt";
  content: string;
  messageSource?: "chat" | "feedback";
}

/**
 * A control command. Today the only command stops the in-flight run; the
 * `commandType` discriminator leaves room for more commands later.
 */
export interface WsClientRequestStopCommand extends WsClientRequestBase {
  type: "command";
  commandType: "stop";
}

/** Discriminated union of every client -> server frame. */
export type WsClientRequest = WsClientRequestPrompt | WsClientRequestStopCommand;
