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

import { createAuthHeaders } from "../src/api/auth-api";
import { AgentState, type ReActStep } from "../src/types/agent";
import { WsClientPromptCommand } from "../src/types/ws/client";
import type { WsServerEvent } from "../src/types/ws/server";

/** One prompt and what the agent did with it. */
export interface Turn {
  prompt: string;
  /** The turn's steps, the user's first; each step once, as it ended. */
  steps: ReActStep[];
  /** The text of the turn's last step. */
  answer: string;
}

const TURN_TIMEOUT_MS = 10 * 60_000;

/**
 * One agent, driven the way the chat drives it: created over HTTP, then prompted over its WebSocket
 * with the service's own command frames.
 */
export class AgentSession {
  private readonly events: WsServerEvent[] = [];
  private listener: (event: WsServerEvent) => void = () => {};

  private constructor(
    private readonly agentUrl: string,
    private readonly agentId: string,
    private readonly socket: WebSocket
  ) {
    socket.onmessage = message => {
      const event = JSON.parse(String(message.data)) as WsServerEvent;
      this.events.push(event);
      this.listener(event);
    };
  }

  static async start(
    agentUrl: string,
    token: string,
    agent: { name: string; model: string; workflowId: number; computingUnitId: number }
  ): Promise<AgentSession> {
    const response = await fetch(`${agentUrl}/api/agents`, {
      method: "POST",
      headers: createAuthHeaders(token),
      body: JSON.stringify({
        name: agent.name,
        modelType: agent.model,
        workflowId: agent.workflowId,
        computingUnitId: agent.computingUnitId,
      }),
    });
    const created = (await response.json()) as { id?: string };
    if (!response.ok || !created.id) {
      throw new Error(`creating an agent failed: ${response.status} ${JSON.stringify(created)}`);
    }
    const socket = new WebSocket(`${agentUrl.replace(/^http/, "ws")}/api/agents/${created.id}/react`);
    await new Promise<void>((resolve, reject) => {
      socket.onopen = () => resolve();
      socket.onerror = () => reject(new Error(`could not open the WebSocket of ${created.id}`));
    });
    return new AgentSession(agentUrl, created.id, socket);
  }

  /** Sends a prompt and resolves once the agent is available again. */
  async prompt(content: string, warehouseId?: number): Promise<Turn> {
    const start = this.events.length;
    const finished = new Promise<void>((resolve, reject) => {
      let generating = false;
      const timer = setTimeout(() => reject(new Error("the turn did not end in time")), TURN_TIMEOUT_MS);
      this.listener = event => {
        if (event.type === "WsServerStatusEvent" && event.state === AgentState.GENERATING) generating = true;
        if (event.type === "WsServerStatusEvent" && event.state === AgentState.AVAILABLE && generating) {
          clearTimeout(timer);
          resolve();
        }
        if (event.type === "WsServerErrorEvent") {
          clearTimeout(timer);
          reject(new Error(event.error));
        }
      };
    });
    this.socket.send(JSON.stringify(new WsClientPromptCommand(content, "chat", warehouseId)));
    await finished;

    // A step is sent again when it is marked as the turn's last, so keep the latest copy of each.
    const byId = new Map<string, ReActStep>();
    for (const event of this.events.slice(start)) {
      if (event.type === "WsServerStepEvent") byId.set(event.step.id, event.step);
    }
    const steps = [...byId.values()];
    return { prompt: content, steps, answer: steps.at(-1)?.content ?? "" };
  }

  async close(): Promise<void> {
    this.socket.close();
    await fetch(`${this.agentUrl}/api/agents/${this.agentId}`, { method: "DELETE" });
  }
}
