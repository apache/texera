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
// (`/agents/:id/react`). Each frame is a class whose `type` discriminator
// equals its class name, so `new WsClientPromptCommand(...)` sets the wire tag
// for you. `WsClientCommand` is their discriminated union.

/** Send a prompt to the agent to start (or continue) its ReAct loop. */
export class WsClientPromptCommand {
  readonly type = "WsClientPromptCommand";
  constructor(
    readonly content: string,
    readonly messageSource?: "chat" | "feedback"
  ) {}
}

/** Stop the agent's in-flight ReAct loop. Carries no payload. */
export class WsClientStopCommand {
  readonly type = "WsClientStopCommand";
}

/** Discriminated union of every client -> server frame. */
export type WsClientCommand = WsClientPromptCommand | WsClientStopCommand;
