/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import type { ApiClient } from "../core/api-client";
import type { WorkflowContent } from "../../core/common/type/workflow";
import type { ReActStep, ToolOperatorAccess } from "../../core/workspace/service/agent/agent-types";
import { WorkflowRepository } from "../workflows/workflow-repository";
export type { ReActStep };
export type AgentState = "UNAVAILABLE" | "AVAILABLE" | "GENERATING" | "STOPPING";
export interface AgentSettings {
  maxOperatorResultCharLimit?: number;
  maxOperatorResultCellCharLimit?: number;
  operatorResultSerializationMode?: "tsv";
  toolTimeoutSeconds?: number;
  executionTimeoutMinutes?: number;
  disabledTools?: string[];
  maxSteps?: number;
  allowedOperatorTypes?: string[];
}
export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  state: AgentState;
  createdAt?: string;
  delegate?: { workflowId?: number; workflowName?: string; userInfo?: { uid: number; name: string } };
  settings?: AgentSettings;
}
export interface AgentConnection {
  readonly readyState: number;
  onopen: (() => void) | null;
  onmessage: ((event: { data: string }) => void) | null;
  onclose: (() => void) | null;
  onerror: (() => void) | null;
  send(data: string): void;
  close(): void;
}
export interface AgentClientOptions {
  workflowId: number;
  getWarehouseId?: () => number | undefined;
  getComputingUnitId?: () => number | undefined;
  onWorkflow?: (content: WorkflowContent) => void;
  onGenerating?: (generating: boolean) => void;
  createSocket?: (url: string) => AgentConnection;
  baseUrl?: string;
  pollIntervalMs?: number;
  reconnectMs?: number;
}
export interface AgentSnapshot {
  agents: readonly AgentInfo[];
  models: readonly { id: string; name: string }[];
  activeId?: string;
  state: AgentState;
  connection: "idle" | "connecting" | "connected" | "disconnected";
  steps: readonly ReActStep[];
  headId: string | null;
  loading: boolean;
  error?: string;
}
function errorMessage(reason: unknown) {
  return reason instanceof Error ? reason.message : "Agent request failed.";
}
function agentState(state: unknown): AgentState {
  return ["AVAILABLE", "GENERATING", "STOPPING", "UNAVAILABLE"].includes(String(state))
    ? (state as AgentState)
    : "UNAVAILABLE";
}
export function convertAgentStep(value: unknown): ReActStep {
  if (!value || typeof value !== "object") throw new Error("Malformed agent step.");
  const raw = value as Record<string, unknown>;
  if (
    typeof raw.messageId !== "string" ||
    typeof raw.stepId !== "number" ||
    typeof raw.content !== "string" ||
    !["user", "agent"].includes(String(raw.role))
  )
    throw new Error("Malformed agent step.");
  const id = typeof raw.id === "string" ? raw.id : raw.messageId + "-" + raw.stepId;
  const date = new Date(typeof raw.timestamp === "number" || typeof raw.timestamp === "string" ? raw.timestamp : 0);
  const access = new Map<number, ToolOperatorAccess>();
  if (raw.operatorAccess && typeof raw.operatorAccess === "object")
    for (const [key, value] of Object.entries(raw.operatorAccess)) {
      if (!value || typeof value !== "object") continue;
      const entry = value as Record<string, unknown>;
      const ids = (key: string): string[] =>
        Array.isArray(entry[key]) ? entry[key].filter((id: unknown): id is string => typeof id === "string") : [];
      access.set(Number(key), {
        viewedOperatorIds: ids("viewedOperatorIds"),
        addedOperatorIds: ids("addedOperatorIds"),
        modifiedOperatorIds: ids("modifiedOperatorIds"),
      });
    }
  return {
    ...raw,
    id,
    timestamp: date,
    messageId: raw.messageId,
    stepId: raw.stepId,
    content: raw.content,
    role: raw.role as "user" | "agent",
    isBegin: !!raw.isBegin,
    isEnd: !!raw.isEnd,
    operatorAccess: access.size ? access : undefined,
    toolCalls: Array.isArray(raw.toolCalls) ? raw.toolCalls : undefined,
    toolResults: Array.isArray(raw.toolResults)
      ? raw.toolResults.map(result => ({
          ...result,
          result: result.output ?? result.result,
          output: result.output ?? result.result,
        }))
      : undefined,
  } as ReActStep;
}
export function visibleAgentSteps(steps: readonly ReActStep[], headId: string | null): ReActStep[] {
  if (!headId || !steps.length) return [...steps];
  const map = new Map(steps.map(step => [step.id, step])),
    ancestors = new Set<string>();
  let current: string | undefined = headId;
  while (current && !ancestors.has(current)) {
    ancestors.add(current);
    current = map.get(current)?.parentId;
  }
  return steps.filter(step => ancestors.has(step.id));
}
function validSettings(settings: AgentSettings): void {
  for (const [key, value] of Object.entries(settings)) {
    if (["disabledTools", "allowedOperatorTypes"].includes(key)) {
      if (!Array.isArray(value) || value.some(v => typeof v !== "string")) throw new Error("Invalid " + key + ".");
    } else if (key === "operatorResultSerializationMode") {
      if (value !== "tsv") throw new Error("Invalid serialization mode.");
    } else if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value <= 0 ||
      (key === "maxSteps" && !Number.isInteger(value))
    )
      throw new Error("Invalid " + key + ".");
  }
}
/** Agent-service HTTP+WebSocket protocol, scoped to one live workflow workspace. */
export class AgentClient {
  private snapshot: AgentSnapshot = {
    agents: [],
    models: [],
    state: "UNAVAILABLE",
    connection: "idle",
    steps: [],
    headId: null,
    loading: false,
  };
  private listeners = new Set<() => void>();
  private socket?: AgentConnection;
  private generation = 0;
  private reconnect?: ReturnType<typeof setTimeout>;
  private polling?: ReturnType<typeof setInterval>;
  private pollController?: AbortController;
  private disposed = false;
  private wsWorkflow = false;
  private workflowKey = "";
  private loading?: AbortController;
  constructor(
    private readonly api: ApiClient,
    private readonly options: AgentClientOptions
  ) {
    if (!Number.isSafeInteger(options.workflowId) || options.workflowId < 0)
      throw new Error("Invalid workflow identifier.");
  }
  getSnapshot = () => this.snapshot;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  async load(): Promise<void> {
    this.loading?.abort();
    const controller = new AbortController();
    this.loading = controller;
    this.update({ loading: true, error: undefined });
    try {
      const [agentReply, modelReply] = await Promise.allSettled([
        this.api.get<{ agents: AgentInfo[] }>("agents", { signal: controller.signal }),
        this.api.get<{ data: { id: string }[] }>("models", { signal: controller.signal }),
      ]);
      if (controller.signal.aborted || this.disposed) return;
      if (agentReply.status === "rejected") throw agentReply.reason;
      const agents = agentReply.value;
      if (
        !Array.isArray(agents.agents) ||
        agents.agents.some(
          agent =>
            !agent ||
            typeof agent.id !== "string" ||
            typeof agent.name !== "string" ||
            typeof agent.modelType !== "string"
        )
      )
        throw new Error("Malformed agent list.");
      if (this.snapshot.activeId && !agents.agents.some(agent => agent.id === this.snapshot.activeId))
        this.deactivate();
      const models =
        modelReply.status === "fulfilled" && Array.isArray(modelReply.value?.data) ? modelReply.value.data : undefined;
      const catalogError =
        modelReply.status === "rejected"
          ? errorMessage(modelReply.reason)
          : models
            ? undefined
            : "Malformed model catalog.";
      this.update({
        agents: agents.agents.map(agent => ({ ...agent, state: agentState(agent.state) })),
        models: models
          ? models
              .filter(model => model && typeof model.id === "string")
              .map(model => ({ id: model.id, name: model.id }))
          : this.snapshot.models,
        loading: false,
        error: catalogError,
      });
    } catch (reason) {
      if (!controller.signal.aborted && !this.disposed) {
        this.update({ loading: false, error: errorMessage(reason) });
        throw reason;
      }
    }
  }
  async create(modelType: string, name?: string): Promise<AgentInfo> {
    if (!modelType.trim()) throw new Error("Select an agent model.");
    const computingUnitId = this.options.getComputingUnitId?.();
    const agent = await this.api.post<AgentInfo>("agents", {
      modelType,
      name,
      workflowId: this.options.workflowId,
      ...(computingUnitId !== undefined ? { computingUnitId } : {}),
    });
    if (!agent?.id) throw new Error("Malformed new agent.");
    this.update({ agents: [...this.snapshot.agents.filter(existing => existing.id !== agent.id), agent] });
    this.activate(agent.id);
    return agent;
  }
  activate(id: string): void {
    if (this.disposed) throw new Error("Agent client is closed.");
    const agent = this.snapshot.agents.find(agent => agent.id === id);
    if (!agent) throw new Error("Agent not found.");
    if (agent.delegate?.workflowId && agent.delegate.workflowId !== this.options.workflowId)
      throw new Error("This agent is working on a different workflow.");
    if (this.snapshot.activeId === id && this.socket) return;
    this.deactivate();
    this.update({ activeId: id, state: agentState(agent.state), steps: [], headId: null, error: undefined });
    this.setState(agentState(agent.state));
    this.dial(id);
    this.polling = setInterval(() => void this.pollWorkflow(), this.options.pollIntervalMs ?? 1000);
  }
  deactivate(): void {
    ++this.generation;
    if (this.reconnect) clearTimeout(this.reconnect);
    if (this.polling) clearInterval(this.polling);
    this.reconnect = undefined;
    this.polling = undefined;
    this.pollController?.abort();
    this.wsWorkflow = false;
    this.workflowKey = "";
    if (this.socket) {
      this.socket.onopen = this.socket.onmessage = this.socket.onclose = this.socket.onerror = null;
      this.socket.close();
      this.socket = undefined;
    }
    this.options.onGenerating?.(false);
    this.update({ connection: "idle", activeId: undefined, state: "UNAVAILABLE" });
  }
  send(content: string, messageSource: "chat" | "feedback" = "chat"): void {
    if (!content.trim()) throw new Error("Enter a message.");
    if (!this.socket || this.socket.readyState !== 1 || this.snapshot.state !== "AVAILABLE")
      throw new Error("Agent connection is not available.");
    const warehouseId = this.options.getWarehouseId?.();
    this.socket.send(
      JSON.stringify({
        type: "WsClientPromptCommand",
        content: content.trim(),
        messageSource,
        ...(warehouseId !== undefined ? { warehouseId } : {}),
      })
    );
    this.setState("GENERATING");
  }
  async stop(): Promise<void> {
    if (this.socket?.readyState === 1) this.socket.send(JSON.stringify({ type: "WsClientStopCommand" }));
    else await this.api.post(this.path("stop"), {}, this.headers());
    this.setState("STOPPING");
  }
  async clear(): Promise<void> {
    const id = this.snapshot.activeId;
    await this.api.post(this.path("clear"), {}, this.headers());
    if (this.snapshot.activeId === id) this.update({ steps: [], headId: null });
  }
  async remove(id: string): Promise<void> {
    const response = await this.api.delete<{ deleted: boolean }>(this.path("", id), this.headers());
    if (!response.deleted) throw new Error("The agent was not deleted.");
    if (this.snapshot.activeId === id) this.deactivate();
    this.update({ agents: this.snapshot.agents.filter(agent => agent.id !== id) });
  }
  async steps(): Promise<ReActStep[]> {
    const response = await this.api.get<{ steps: unknown[] }>(this.path("react-steps"), this.headers());
    if (!Array.isArray(response.steps)) throw new Error("Malformed agent steps.");
    return response.steps.map(convertAgentStep);
  }
  systemInfo(): Promise<{
    systemPrompt: string;
    tools: { name: string; description: string; inputSchema: unknown; enabled: boolean }[];
  }> {
    return this.api.get(this.path("system-info"), this.headers());
  }
  settings(): Promise<AgentSettings> {
    return this.api.get(this.path("settings"), this.headers());
  }
  operatorTypes(): Promise<{ type: string; description: string }[]> {
    return this.api.get(this.path("operator-types"), this.headers());
  }
  operatorResults(): Promise<{ results: Record<string, unknown> }> {
    return this.api.get(this.path("operator-results"), this.headers());
  }
  async updateSettings(settings: AgentSettings): Promise<AgentSettings> {
    validSettings(settings);
    return this.api.patch<AgentSettings>(this.path("settings"), settings, this.headers());
  }
  async saveSettings(settings: AgentSettings): Promise<AgentSettings> {
    return this.updateSettings(settings);
  }
  async stepsByOperators(operatorIds: readonly string[]): Promise<ReActStep[]> {
    const response = await this.api.post<{ steps: unknown[] }>(
      this.path("steps-by-operators"),
      { operatorIds: [...operatorIds] },
      this.headers()
    );
    return response.steps.map(convertAgentStep);
  }
  dispose(): void {
    this.loading?.abort();
    this.deactivate();
    this.disposed = true;
    this.listeners.clear();
  }
  private headers() {
    return { headers: { "X-Agent-Workflow-Id": String(this.options.workflowId) } };
  }
  private path(suffix: string, id = this.snapshot.activeId): string {
    if (!id) throw new Error("Select an agent.");
    return "agents/" + encodeURIComponent(id) + (suffix ? "/" + suffix : "");
  }
  private dial(id: string): void {
    const generation = ++this.generation;
    this.update({ connection: "connecting" });
    const url = new URL(
      "/api/agents/" + encodeURIComponent(id) + "/react",
      this.options.baseUrl ?? window.location.origin
    );
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    let socket: AgentConnection;
    try {
      socket = this.options.createSocket?.(url.toString()) ?? (new WebSocket(url) as unknown as AgentConnection);
    } catch (reason) {
      this.disconnected(id, generation, errorMessage(reason));
      return;
    }
    this.socket = socket;
    socket.onopen = () => {
      if (generation === this.generation) this.update({ connection: "connected", error: undefined });
    };
    socket.onmessage = event => {
      if (generation !== this.generation || this.disposed) return;
      try {
        this.receive(JSON.parse(event.data));
      } catch (reason) {
        this.update({
          error: errorMessage(reason).startsWith("Malformed") ? errorMessage(reason) : "Malformed agent message.",
        });
      }
    };
    socket.onerror = () => {
      if (generation === this.generation) this.update({ error: "The agent connection failed." });
    };
    socket.onclose = () => {
      if (generation === this.generation) {
        this.socket = undefined;
        this.disconnected(id, generation, "Agent disconnected. Reconnecting…");
      }
    };
  }
  private disconnected(id: string, generation: number, error: string): void {
    if (this.disposed || this.snapshot.activeId !== id || generation !== this.generation) return;
    this.wsWorkflow = false;
    this.update({ connection: "disconnected", error });
    this.reconnect = setTimeout(() => {
      if (!this.disposed && generation === this.generation) this.dial(id);
    }, this.options.reconnectMs ?? 3000);
  }
  private receive(message: Record<string, unknown>): void {
    if (!message || typeof message !== "object") throw new Error("Malformed agent message.");
    switch (message.type) {
      case "WsServerSnapshotEvent": {
        if (!Array.isArray(message.steps)) throw new Error("Malformed agent steps.");
        const steps = message.steps.map(convertAgentStep);
        this.update({
          steps,
          headId: typeof message.headId === "string" ? message.headId : null,
          connection: "connected",
          error: undefined,
        });
        this.setState(agentState(message.state));
        if (message.workflowContent) {
          this.wsWorkflow = true;
          this.applyWorkflow(message.workflowContent);
        }
        break;
      }
      case "WsServerStepEvent": {
        const step = convertAgentStep(message.step),
          steps = [...this.snapshot.steps],
          index = steps.findIndex(existing => existing.messageId === step.messageId && existing.stepId === step.stepId);
        if (index >= 0) steps[index] = step;
        else steps.push(step);
        this.update({ steps, headId: step.id });
        if (step.afterWorkflowContent) {
          this.wsWorkflow = true;
          this.applyWorkflow(step.afterWorkflowContent);
        }
        break;
      }
      case "WsServerStatusEvent":
        this.setState(agentState(message.state));
        break;
      case "WsServerErrorEvent": {
        const error = typeof message.error === "string" ? message.error : "Agent error occurred.";
        if (error === "Agent not found") {
          const id = this.snapshot.activeId;
          this.deactivate();
          this.update({ agents: this.snapshot.agents.filter(agent => agent.id !== id) });
        }
        this.update({ error });
        break;
      }
      default:
        throw new Error("Malformed agent message: unknown event.");
    }
  }
  private async pollWorkflow(): Promise<void> {
    if (
      this.disposed ||
      this.wsWorkflow ||
      !this.snapshot.activeId ||
      (this.pollController && !this.pollController.signal.aborted)
    )
      return;
    const generation = this.generation,
      controller = new AbortController();
    this.pollController = controller;
    try {
      const workflow = await new WorkflowRepository(this.api).get(this.options.workflowId, controller.signal);
      if (!controller.signal.aborted && !this.disposed && generation === this.generation && !this.wsWorkflow)
        this.applyWorkflow(workflow.content);
    } catch (reason) {
      if (!controller.signal.aborted && generation === this.generation)
        this.update({ error: "Unable to synchronize agent workflow: " + errorMessage(reason) });
    } finally {
      if (this.pollController === controller) this.pollController = undefined;
    }
  }
  private applyWorkflow(raw: unknown): void {
    if (!raw || typeof raw !== "object") throw new Error("Malformed agent workflow.");
    const content = raw as WorkflowContent;
    if (
      !Array.isArray(content.operators) ||
      !Array.isArray(content.links) ||
      !Array.isArray(content.commentBoxes) ||
      !content.operatorPositions ||
      !content.settings
    )
      throw new Error("Malformed agent workflow.");
    const key = JSON.stringify(content);
    if (key === this.workflowKey) return;
    this.options.onWorkflow?.(structuredClone(content));
    this.workflowKey = key;
  }
  private setState(state: AgentState) {
    this.update({ state });
    this.options.onGenerating?.(state === "GENERATING" || state === "STOPPING");
  }
  private update(patch: Partial<AgentSnapshot>) {
    if (this.disposed) return;
    this.snapshot = { ...this.snapshot, ...patch };
    this.listeners.forEach(listener => listener());
  }
}
