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

/**
 * Texera Agent Service - HTTP Server using Elysia.js
 *
 * Provides REST API endpoints and WebSocket for agent interaction.
 * Supports user delegation mode where agents act on behalf of authenticated users.
 * Uses RxJS for reactive workflow change handling (persistence and compilation).
 *
 * WebSocket endpoint: /api/agents/:id/react
 * - Send message: { type: "message", content: "..." }
 * - Stop: { type: "stop" }
 * - Receive steps: { type: "step", step: ReActStep }
 * - Receive state: { type: "state", state: "AVAILABLE" | "GENERATING" | ... }
 */

import { Elysia, t } from "elysia";
import { cors } from "@elysiajs/cors";
import { createOpenAI } from "@ai-sdk/openai";
import { TexeraAgent } from "./agent/texera-agent";
import { getBackendConfig } from "./api/backend-api";
import { extractUserFromToken, validateToken } from "./api/auth-api";
import { retrieveWorkflow } from "./api/workflow-api";
import { OperatorMetadataStore } from "./agent/tools/workflow-metadata-tools";
import { env } from "./config/env";
import type {
  AgentInfo,
  AgentDelegateConfig,
  CreateAgentRequest,
  UpdateAgentSettingsRequest,
  AgentSettingsApi,
  ReActStep,
} from "./types/agent";
import { OperatorResultSerializationMode } from "./types/agent";

// ============================================================================
// Agent Management
// ============================================================================

// Store for active agents (TexeraAgent now contains all necessary state)
const agentStore = new Map<string, TexeraAgent>();
let agentCounter = 0;

/**
 * Create a new agent with optional delegate configuration.
 * When delegate config is provided, workflow changes are automatically persisted.
 * The agent handles workflow change subscriptions internally.
 */
async function createAgentInstance(
  modelType: string,
  customName?: string,
  delegateConfig?: AgentDelegateConfig
): Promise<{ agentId: string; agent: TexeraAgent }> {
  const agentId = `agent-${++agentCounter}`;
  const config = getBackendConfig();

  // Use models endpoint as baseURL with /api path
  const openai = createOpenAI({
    baseURL: `${config.modelsEndpoint}/api`,
    apiKey: env.LLM_API_KEY,
  });

  // Send the full model name to LiteLLM (e.g., "gpt-5-mini-medium").
  // Reasoning effort variants are configured as separate model entries in litellm-config.yaml
  // with extra_body to inject reasoning_effort, bypassing LiteLLM's param validation.
  const agent = new TexeraAgent({
    model: openai.chat(modelType),
    modelType,
    agentId,
    agentName: customName || "Bob",
  });

  // Initialize agent (loads operator metadata from backend and rebuilds tools)
  await agent.initialize();

  // If in delegate mode with workflowId, load workflow and setup delegate config
  if (delegateConfig?.workflowId && delegateConfig.userToken) {
    try {
      const workflow = await retrieveWorkflow(delegateConfig.userToken, delegateConfig.workflowId);
      delegateConfig.workflowName = workflow.name;

      // Load workflow content into agent's workflow state
      const workflowState = agent.getWorkflowState();
      workflowState.setWorkflowContent(workflow.content);

      // Set delegate config on agent (this sets up workflow change handlers internally)
      agent.setDelegateConfig({
        userToken: delegateConfig.userToken,
        userInfo: delegateConfig.userInfo,
        workflowId: delegateConfig.workflowId,
        workflowName: delegateConfig.workflowName,
        computingUnitId: delegateConfig.computingUnitId,
      });

      console.log(`[Server] Loaded workflow ${delegateConfig.workflowId} for agent ${agentId}`);
    } catch (error) {
      console.warn(`[Server] Failed to load workflow ${delegateConfig.workflowId}:`, error);
    }
  }

  agentStore.set(agentId, agent);
  console.log(`[Server] Created new agent: ${agentId} (delegate: ${!!delegateConfig})`);

  return { agentId, agent };
}

/**
 * Get agent info for API response
 */
function getAgentInfo(agentId: string, agent: TexeraAgent): AgentInfo {
  // Get settings from agent and convert to API format
  const agentSettings = agent.getSettings();
  const settingsApi: AgentSettingsApi = {
    maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
    maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
    operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
    toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
    executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
    disabledTools: Array.from(agentSettings.disabledTools),
    maxSteps: agentSettings.maxSteps,
    allowedOperatorTypes: agentSettings.allowedOperatorTypes,
  };

  const delegateConfig = agent.getDelegateConfig();

  return {
    id: agentId,
    name: agent.agentName,
    modelType: agent.modelType,
    state: agent.getState(),
    createdAt: agent.createdAt,
    delegate: delegateConfig
      ? {
          userToken: "***", // Don't expose token
          userInfo: delegateConfig.userInfo,
          workflowId: delegateConfig.workflowId,
          workflowName: delegateConfig.workflowName,
        }
      : undefined,
    settings: settingsApi,
  };
}

/**
 * Get agent by ID or throw error
 */
function getAgent(agentId: string): TexeraAgent {
  const agent = agentStore.get(agentId);
  if (!agent) {
    throw new Error("Agent not found");
  }
  return agent;
}

// ============================================================================
// Agents Router (mounted at /agents)
// ============================================================================

const agentsRouter = new Elysia({ prefix: "/agents" })
  // List all agents
  .get("/", () => {
    const agentList = Array.from(agentStore.entries()).map(([id, agent]) => getAgentInfo(id, agent));
    return { agents: agentList };
  })

  // Create agent
  .post(
    "/",
    async ({ body }) => {
      const { modelType, name, userToken, workflowId, computingUnitId, settings } = body as CreateAgentRequest;

      if (!modelType) {
        throw new Error("modelType is required");
      }

      // If userToken provided, create delegate config
      let delegateConfig: AgentDelegateConfig | undefined;
      if (userToken) {
        if (!validateToken(userToken)) {
          throw new Error("Invalid or expired token");
        }

        const userInfo = extractUserFromToken(userToken);
        delegateConfig = {
          userToken,
          userInfo,
          workflowId,
          computingUnitId,
        };
      }

      const { agentId, agent } = await createAgentInstance(modelType, name, delegateConfig);

      // Apply initial settings if provided
      if (settings) {
        console.log(
          `[Server] Applying initial settings for agent ${agentId}: ` +
            `maxOperatorResultCharLimit=${settings.maxOperatorResultCharLimit}, ` +
            `maxOperatorResultCellCharLimit=${settings.maxOperatorResultCellCharLimit}`
        );
        agent.updateSettings({
          maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
          operatorResultSerializationMode: settings.operatorResultSerializationMode
            ? (settings.operatorResultSerializationMode as OperatorResultSerializationMode)
            : undefined,
          toolTimeoutMs: settings.toolTimeoutSeconds ? settings.toolTimeoutSeconds * 1000 : undefined,
          executionTimeoutMs: settings.executionTimeoutMinutes ? settings.executionTimeoutMinutes * 60000 : undefined,
          disabledTools: settings.disabledTools ? new Set(settings.disabledTools) : undefined,
          maxSteps: settings.maxSteps,
          allowedOperatorTypes: settings.allowedOperatorTypes,
        });
      }

      return getAgentInfo(agentId, agent);
    },
    {
      body: t.Object({
        modelType: t.String(),
        name: t.Optional(t.String()),
        userToken: t.Optional(t.String()),
        workflowId: t.Optional(t.Number()),
        computingUnitId: t.Optional(t.Number()),
        settings: t.Optional(
          t.Object({
            maxOperatorResultCharLimit: t.Optional(t.Number()),
            maxOperatorResultCellCharLimit: t.Optional(t.Number()),
            operatorResultSerializationMode: t.Optional(
              t.Literal("tsv")
            ),
            toolTimeoutSeconds: t.Optional(t.Number()),
            executionTimeoutMinutes: t.Optional(t.Number()),
            disabledTools: t.Optional(t.Array(t.String())),
            maxSteps: t.Optional(t.Number()),
            allowedOperatorTypes: t.Optional(t.Array(t.String())),
          })
        ),
      }),
    }
  )

  // Get agent by ID
  .get("/:id", ({ params: { id } }) => {
    const agent = getAgent(id);
    return {
      ...getAgentInfo(id, agent),
      workflow: agent.getWorkflowState().getWorkflowContent(),
      stepCount: agent.getReActSteps().length,
    };
  })

  // Delete agent
  .delete("/:id", ({ params: { id } }) => {
    const agent = agentStore.get(id);
    if (!agent) {
      throw new Error("Agent not found");
    }

    // Destroy agent (cleans up all subscriptions, websockets, and workflow state)
    agent.destroy();

    agentStore.delete(id);
    return { deleted: true };
  })

  // Get all ReActSteps (for polling fallback or initial load)
  .get("/:id/react-steps", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { steps: agent.getReActSteps(), state: agent.getState() };
  })

  // Get all visible operator results (summarized for annotation)
  .get("/:id/operator-results", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { results: getOperatorResultSummaries(agent) };
  })

  // Get ReActSteps filtered by operator IDs (for context preview)
  .post(
    "/:id/steps-by-operators",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const { operatorIds } = body;
      return { steps: agent.getReActStepsByOperatorIds(operatorIds || []) };
    },
    {
      body: t.Object({
        operatorIds: t.Array(t.String()),
      }),
    }
  )

  // Get system info (system prompt and tools)
  .get("/:id/system-info", ({ params: { id } }) => {
    const agent = getAgent(id);
    return agent.getSystemInfo();
  })

  // Stop agent
  .post("/:id/stop", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.stop();
    return { status: "stopping" };
  })

  // Clear messages
  .post("/:id/clear", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.clearHistory();
    return { status: "cleared" };
  })

  // Checkout to a specific step (move HEAD, restore workflow)
  .post("/:id/checkout", ({ params: { id }, body }) => {
    const agent = getAgent(id);
    const { stepId } = body as { stepId: string };
    if (!stepId) throw new Error("stepId is required");

    const success = agent.checkout(stepId);
    if (!success) throw new Error(`Step ${stepId} not found or checkout failed`);

    const allSteps = agent.getAllSteps();
    const workflowContent = agent.getWorkflowState().getWorkflowContent();

    // Broadcast HEAD change + all steps + workflow content + operator results
    broadcastToAgent(id, {
      type: "headChange",
      headId: stepId,
      steps: allSteps,
      workflowContent,
      operatorResults: getOperatorResultSummaries(agent),
    });

    return {
      status: "checked out",
      headId: stepId,
    };
  })

  // Get all available operator types (for operator configuration UI)
  .get("/:id/operator-types", ({ params: { id } }) => {
    const agent = getAgent(id);
    const metadataStore = agent.getMetadataStore();
    const allTypes = metadataStore.getAllOperatorTypes();
    return Object.entries(allTypes).map(([type, description]) => ({ type, description }));
  })

  // Get agent settings
  .get("/:id/settings", ({ params: { id } }) => {
    const agent = getAgent(id);
    const agentSettings = agent.getSettings();
    return {
      maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
      operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
      toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
      executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
      disabledTools: Array.from(agentSettings.disabledTools),
      maxSteps: agentSettings.maxSteps,
      allowedOperatorTypes: agentSettings.allowedOperatorTypes,
    };
  })

  // Update agent settings
  .patch(
    "/:id/settings",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const settings = body as UpdateAgentSettingsRequest;

      console.log(
        `[Server] Updating settings for agent ${id}: ` +
          `maxOperatorResultCharLimit=${settings.maxOperatorResultCharLimit}, ` +
          `maxOperatorResultCellCharLimit=${settings.maxOperatorResultCellCharLimit}`
      );

      agent.updateSettings({
        maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
        operatorResultSerializationMode: settings.operatorResultSerializationMode
          ? (settings.operatorResultSerializationMode as OperatorResultSerializationMode)
          : undefined,
        toolTimeoutMs: settings.toolTimeoutSeconds !== undefined ? settings.toolTimeoutSeconds * 1000 : undefined,
        executionTimeoutMs:
          settings.executionTimeoutMinutes !== undefined ? settings.executionTimeoutMinutes * 60000 : undefined,
        disabledTools: settings.disabledTools ? new Set(settings.disabledTools) : undefined,
        maxSteps: settings.maxSteps,
        allowedOperatorTypes: settings.allowedOperatorTypes,
      });

      // Return updated settings
      const agentSettings = agent.getSettings();
      return {
        maxOperatorResultCharLimit: agentSettings.maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit: agentSettings.maxOperatorResultCellCharLimit,
        operatorResultSerializationMode: agentSettings.operatorResultSerializationMode,
        toolTimeoutSeconds: Math.round(agentSettings.toolTimeoutMs / 1000),
        executionTimeoutMinutes: Math.round(agentSettings.executionTimeoutMs / 60000),
        disabledTools: Array.from(agentSettings.disabledTools),
        maxSteps: agentSettings.maxSteps,
        allowedOperatorTypes: agentSettings.allowedOperatorTypes,
      };
    },
    {
      body: t.Object({
        maxOperatorResultCharLimit: t.Optional(t.Number()),
        maxOperatorResultCellCharLimit: t.Optional(t.Number()),
        operatorResultSerializationMode: t.Optional(
          t.Literal("tsv")
        ),
        toolTimeoutSeconds: t.Optional(t.Number()),
        executionTimeoutMinutes: t.Optional(t.Number()),
        maxSteps: t.Optional(t.Number()),
        disabledTools: t.Optional(t.Array(t.String())),
        allowedOperatorTypes: t.Optional(t.Array(t.String())),
      }),
    }
  );

// ============================================================================
// WebSocket Message Types
// ============================================================================

interface WsMessage {
  type: "message" | "stop";
  content?: string;
  /** Optional operator IDs for context filtering - only messages that affected these operators will be included */
  contextOperatorIds?: string[];
  /** Source of the message: "chat" (agent panel) or "feedback" (operator feedback panel) */
  messageSource?: "chat" | "feedback";
}

interface OperatorResultSummaryWs {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: { portIndex: number; rows: number; columns: number }[];
  outputColumns?: number;
  error?: string;
  warnings?: string[];
  consoleLogCount?: number;
  totalRowCount?: number;
  sampleRecords?: Record<string, any>[];
  resultStatistics?: Record<string, string>;
}

interface WsOutgoingMessage {
  type: "step" | "state" | "error" | "complete" | "init" | "headChange";
  step?: ReActStep;
  state?: string;
  error?: string;
  steps?: ReActStep[];
  headId?: string;
  operatorResults?: Record<string, OperatorResultSummaryWs>;
  workflowContent?: any;
}

/**
 * Compute operator result summaries from an agent's result store.
 */
function getOperatorResultSummaries(agent: TexeraAgent): Record<string, OperatorResultSummaryWs> {
  const store = agent.getOperatorResultStore();
  const visible = store.getAllVisible();
  const results: Record<string, OperatorResultSummaryWs> = {};
  for (const [opId, entry] of visible) {
    const info = entry.operatorInfo;
    results[opId] = {
      state: info.state,
      inputTuples: info.inputTuples,
      outputTuples: info.outputTuples,
      inputPortShapes: info.inputPortShapes,
      outputColumns:
        info.result && info.result.length > 0
          ? Object.keys(info.result[0]).filter(k => k !== "__row_index__").length
          : undefined,
      error: info.error,
      warnings: info.warnings,
      consoleLogCount: info.consoleLogs?.length,
      totalRowCount: info.totalRowCount,
      sampleRecords: info.result,
      resultStatistics: info.resultStatistics,
    };
  }
  return results;
}

/**
 * Broadcast a message to all WebSocket clients connected to an agent
 */
function broadcastToAgent(agentId: string, message: WsOutgoingMessage): void {
  const agent = agentStore.get(agentId);
  if (!agent) return;

  const jsonMessage = JSON.stringify(message);
  for (const ws of agent.getWebsockets()) {
    try {
      ws.send(jsonMessage);
    } catch (error) {
      console.error(`[WS] Failed to send message to client:`, error);
      agent.removeWebsocket(ws);
    }
  }
}

// ============================================================================
// Main Application
// ============================================================================

const app = new Elysia()
  .use(cors())
  // Health check (at root, not under prefix)
  .get("/health", () => ({
    status: "ok",
    timestamp: new Date().toISOString(),
  }))
  // Mount agents router under API prefix
  .group(env.API_PREFIX, app => app.use(agentsRouter))
  // WebSocket endpoint for real-time ReActSteps streaming
  .ws(`${env.API_PREFIX}/agents/:id/react`, {
    open(ws) {
      const agentId = (ws.data as any).params?.id;
      console.log(`[WS] Client connected to agent ${agentId}`);

      const agent = agentStore.get(agentId);
      if (!agent) {
        ws.send(JSON.stringify({ type: "error", error: "Agent not found" }));
        ws.close();
        return;
      }

      // Add this websocket to the agent's set
      agent.addWebsocket(ws);

      // Send initial state, visible steps (based on HEAD), and operator results
      const initMessage: WsOutgoingMessage = {
        type: "init",
        state: agent.getState(),
        steps: agent.getAllSteps(),
        headId: agent.getHead(),
        operatorResults: getOperatorResultSummaries(agent),
      };
      ws.send(JSON.stringify(initMessage));
    },

    async message(ws, messageData) {
      const agentId = (ws.data as any).params?.id;
      const agent = agentStore.get(agentId);

      if (!agent) {
        ws.send(JSON.stringify({ type: "error", error: "Agent not found" }));
        return;
      }

      let msg: WsMessage;
      try {
        msg = typeof messageData === "string" ? JSON.parse(messageData) : (messageData as WsMessage);
      } catch {
        ws.send(JSON.stringify({ type: "error", error: "Invalid message format" }));
        return;
      }

      if (msg.type === "stop") {
        agent.stop();
        // Broadcast STOPPING state immediately to all connected clients
        broadcastToAgent(agentId, { type: "state", state: "STOPPING" });
        return;
      }

      if (msg.type === "message") {
        if (!msg.content || typeof msg.content !== "string") {
          ws.send(JSON.stringify({ type: "error", error: "Message content is required" }));
          return;
        }

        console.log(`[WS] Agent ${agentId} received message: ${msg.content.substring(0, 50)}...`);
        if (msg.contextOperatorIds && msg.contextOperatorIds.length > 0) {
          console.log(`[WS] Context filter with operators: [${msg.contextOperatorIds.join(", ")}]`);
        }

        // Set up step callback to stream steps in real-time
        // Include operatorResults when the step has tool calls (which may modify workflow/execution state)
        agent.setStepCallback((step: ReActStep) => {
          const hasToolCalls = step.toolCalls && step.toolCalls.length > 0;
          broadcastToAgent(agentId, {
            type: "step",
            step,
            ...(hasToolCalls ? { operatorResults: getOperatorResultSummaries(agent) } : {}),
          });
        });

        // Broadcast GENERATING state immediately before starting processing
        // The agent will set its internal state in sendMessage, but we want frontend to know immediately
        broadcastToAgent(agentId, { type: "state", state: "GENERATING" });

        try {
          const result = await agent.sendMessage(msg.content, msg.contextOperatorIds, msg.messageSource);

          // Clear the callback
          agent.setStepCallback(null);

          // Get the last step (which now has isEnd: true) and broadcast it
          // This ensures the frontend receives the final step with isEnd: true
          const allSteps = agent.getReActSteps();
          const lastStep = allSteps[allSteps.length - 1];
          if (lastStep && lastStep.isEnd) {
            broadcastToAgent(agentId, { type: "step", step: lastStep });
          }

          // Broadcast completion with latest operator results
          broadcastToAgent(agentId, {
            type: "complete",
            state: agent.getState(),
            operatorResults: getOperatorResultSummaries(agent),
          });

          console.log(`[WS] Agent ${agentId} completed with ${result.messages.length} steps`);
        } catch (error: any) {
          agent.setStepCallback(null);
          broadcastToAgent(agentId, { type: "error", error: error.message });
        }
      }
    },

    close(ws) {
      const agentId = (ws.data as any).params?.id;
      console.log(`[WS] Client disconnected from agent ${agentId}`);

      const agent = agentStore.get(agentId);
      if (agent) {
        agent.removeWebsocket(ws);
      }
    },
  })
  // Error handling
  .onError(({ error, set }) => {
    console.error("[Server] Error:", error);

    const errorMessage = error instanceof Error ? error.message : String(error);

    if (errorMessage === "Agent not found") {
      set.status = 404;
      return { error: "Agent not found" };
    }

    if (errorMessage === "Invalid or expired token") {
      set.status = 401;
      return { error: "Invalid or expired token" };
    }

    if (errorMessage === "modelType is required") {
      set.status = 400;
      return { error: "modelType is required" };
    }

    set.status = 500;
    return { error: errorMessage || "Internal server error" };
  })
  .listen(env.PORT);

// ============================================================================
// Startup Message - Using Elysia's routes property
// ============================================================================

function printStartupMessage() {
  const LINE = "=".repeat(60);
  console.log(LINE);
  console.log("Texera Agent Service (Elysia.js + RxJS)");
  console.log(LINE);
  console.log(`Server running at http://localhost:${env.PORT}`);
  console.log("");

  // Print routes from Elysia's routes property
  console.log("Registered Routes:");
  const routes = app.routes;

  // Group routes by type (HTTP vs WebSocket)
  const httpRoutes = routes.filter(r => r.method !== "WS");
  const wsRoutes = routes.filter(r => r.method === "WS");

  // Print HTTP routes
  for (const route of httpRoutes) {
    const method = route.method.padEnd(6);
    console.log(`  ${method} ${route.path}`);
  }

  // Print WebSocket routes
  if (wsRoutes.length > 0) {
    console.log("");
    console.log("WebSocket Endpoints:");
    for (const route of wsRoutes) {
      console.log(`  WS     ${route.path}`);
    }
    console.log("         Send: { type: 'message', content: '...' }");
    console.log("         Send: { type: 'stop' }");
    console.log("         Recv: { type: 'step' | 'state' | 'complete' | 'error' | 'init', ... }");
  }

  console.log("");
  console.log("Environment:");
  console.log(`  LLM_API_KEY: ${env.LLM_API_KEY === "dummy" ? "dummy (default)" : "set"}`);
  console.log(`  LLM_ENDPOINT: ${getBackendConfig().modelsEndpoint}`);
  console.log(`  WORKFLOW_COMPILING_SERVICE_ENDPOINT: ${getBackendConfig().compileEndpoint}`);
  console.log(`  WORKFLOW_EXECUTION_SERVICE_ENDPOINT: ${getBackendConfig().executionEndpoint}`);
  console.log(`  TEXERA_DASHBOARD_SERVICE_ENDPOINT: ${getBackendConfig().apiEndpoint}`);
  console.log("");
  console.log("Features:");
  console.log("  - Auto-persistence with debounce (500ms)");
  console.log("  - Tools compile workflow on-demand for fresh schemas");
  console.log(LINE);
}

// Initialize global metadata store at startup
async function initializeServices() {
  try {
    console.log("[Server] Initializing global operator metadata store...");
    const metadataStore = await OperatorMetadataStore.initializeGlobal();
    console.log(`[Server] Loaded ${metadataStore.getOperatorCount()} operators into global metadata store`);
  } catch (error) {
    console.warn("[Server] Failed to initialize global metadata store:", error);
    console.warn("[Server] Agents will initialize metadata individually on creation");
  }
}

// Run startup initialization
initializeServices().then(() => {
  printStartupMessage();
});

export default app;
