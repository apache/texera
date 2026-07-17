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

import { Elysia, t } from "elysia";
import { cors } from "@elysiajs/cors";
import { createOpenAI } from "@ai-sdk/openai";
import { TexeraAgent } from "./agent/texera-agent";
import { getVisibleResultHeaders } from "./agent/tools/tools-utility";
import { getBackendConfig } from "./api/backend-api";
import { extractBearerToken, extractUserFromToken, validateToken } from "./api/auth-api";
import { retrieveWorkflow } from "./api/workflow-api";
import { WorkflowSystemMetadata } from "./agent/util/workflow-system-metadata";
import { env } from "./config/env";
import { createLogger } from "./logger";

const log = createLogger("Server");
const wsLog = createLogger("WS");
import type {
  AgentInfo,
  AgentDelegateConfig,
  CreateAgentRequest,
  UpdateAgentSettingsRequest,
  AgentSettingsApi,
  ReActStep,
} from "./types/agent";
import { AgentState, OperatorResultSerializationMode } from "./types/agent";
import type { WsClientCommand, WsServerEvent } from "./types/ws";
import { WsServerSnapshotEvent, WsServerStepEvent, WsServerStatusEvent, WsServerErrorEvent } from "./types/ws";
import type { OperatorResultSummary } from "./types/execution";

const agentStore = new Map<string, TexeraAgent>();
let agentCounter = 0;

async function createAgentInstance(
  modelType: string,
  delegateConfig: AgentDelegateConfig,
  customName?: string
): Promise<{ agentId: string; agent: TexeraAgent }> {
  const agentId = `agent-${++agentCounter}`;
  const config = getBackendConfig();

  const openai = createOpenAI({
    baseURL: `${config.modelsEndpoint}/api`,
    // The LLM gateway (access-control-service) enforces a REGULAR/ADMIN-role
    // JWT (apache/texera#5421) and injects the LiteLLM master key downstream,
    // so the delegating user's JWT is the only credential this service sends.
    apiKey: delegateConfig.userToken,
  });

  // Reasoning effort variants are configured as separate model entries in litellm-config.yaml
  // with extra_body to inject reasoning_effort, bypassing LiteLLM's param validation.
  const agent = new TexeraAgent({
    model: openai.chat(modelType),
    modelType,
    agentId,
    agentName: customName || "Bob",
  });

  await agent.initialize();

  if (delegateConfig.workflowId) {
    try {
      const workflow = await retrieveWorkflow(delegateConfig.userToken, delegateConfig.workflowId);
      delegateConfig.workflowName = workflow.name;

      const workflowState = agent.getWorkflowState();
      workflowState.setWorkflowContent(workflow.content);

      agent.setDelegateConfig({
        userToken: delegateConfig.userToken,
        userInfo: delegateConfig.userInfo,
        workflowId: delegateConfig.workflowId,
        workflowName: delegateConfig.workflowName,
        computingUnitId: delegateConfig.computingUnitId,
      });

      log.info({ agentId, workflowId: delegateConfig.workflowId }, "loaded workflow for agent");
    } catch (error) {
      log.warn({ agentId, workflowId: delegateConfig.workflowId, err: error }, "failed to load workflow");
    }
  }

  agentStore.set(agentId, agent);
  log.info({ agentId, userId: delegateConfig.userInfo?.uid }, "created agent");

  return { agentId, agent };
}

function getAgentInfo(agentId: string, agent: TexeraAgent): AgentInfo {
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
          userToken: "***",
          userInfo: delegateConfig.userInfo,
          workflowId: delegateConfig.workflowId,
          workflowName: delegateConfig.workflowName,
          computingUnitId: delegateConfig.computingUnitId,
        }
      : undefined,
    settings: settingsApi,
  };
}

function getAgent(agentId: string): TexeraAgent {
  const agent = agentStore.get(agentId);
  if (!agent) {
    throw new Error("Agent not found");
  }
  return agent;
}

// Status codes for handler-thrown errors; anything unlisted is a 500.
const ERROR_STATUS: Record<string, number> = {
  "Agent not found": 404,
  "Invalid or expired token": 401,
  "Authorization header with a Bearer token is required": 401,
  "modelType is required": 400,
};

const agentsRouter = new Elysia({ prefix: "/agents" })
  // Error handler must live on the same Elysia instance whose routes throw, or
  // its scope will not see the errors. Elysia 1.x defaults to local scoping for
  // .onError, so attach here rather than on the outer app.
  .onError(({ code, error, set }) => {
    log.error({ err: error }, "request error");
    const errorMessage = error instanceof Error ? error.message : String(error);
    // Body schema violations and malformed JSON are client errors, not 500s.
    if (code === "VALIDATION" || code === "PARSE") {
      set.status = 400;
      return { error: errorMessage || "Invalid request body" };
    }
    set.status = ERROR_STATUS[errorMessage] ?? 500;
    return { error: errorMessage || "Internal server error" };
  })
  .get("/", () => {
    const agentList = Array.from(agentStore.entries()).map(([id, agent]) => getAgentInfo(id, agent));
    return { agents: agentList };
  })

  .post(
    "/",
    async ({ body, headers }) => {
      const { modelType, name, workflowId, computingUnitId, settings } = body as CreateAgentRequest;

      if (!modelType) {
        throw new Error("modelType is required");
      }

      // The agent always calls the LLM gateway as the delegating user, so an
      // agent without a user token would be unable to generate anything. The
      // token travels in the Authorization header, never in the payload.
      const userToken = extractBearerToken(headers.authorization);
      if (!userToken) {
        throw new Error("Authorization header with a Bearer token is required");
      }
      if (!validateToken(userToken)) {
        throw new Error("Invalid or expired token");
      }

      const userInfo = extractUserFromToken(userToken);
      const delegateConfig: AgentDelegateConfig = {
        userToken,
        userInfo,
        workflowId,
        computingUnitId,
      };

      const { agentId, agent } = await createAgentInstance(modelType, delegateConfig, name);

      if (settings) {
        log.info(
          {
            agentId,
            maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
            maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
          },
          "applying initial agent settings"
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
        workflowId: t.Optional(t.Number()),
        computingUnitId: t.Optional(t.Number()),
        settings: t.Optional(
          t.Object({
            maxOperatorResultCharLimit: t.Optional(t.Number()),
            maxOperatorResultCellCharLimit: t.Optional(t.Number()),
            operatorResultSerializationMode: t.Optional(t.Literal("tsv")),
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

  .get("/:id", ({ params: { id } }) => {
    const agent = getAgent(id);
    return {
      ...getAgentInfo(id, agent),
      workflow: agent.getWorkflowState().getWorkflowContent(),
      stepCount: agent.getReActSteps().length,
    };
  })

  .delete("/:id", ({ params: { id }, set }) => {
    const agent = agentStore.get(id);
    if (!agent) {
      set.status = 404;
      return { error: "Agent not found" };
    }

    agent.destroy();
    agentStore.delete(id);
    return { deleted: true };
  })

  .get("/:id/react-steps", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { steps: agent.getReActSteps(), state: agent.getState() };
  })

  .get("/:id/operator-results", ({ params: { id } }) => {
    const agent = getAgent(id);
    return { results: getOperatorResultSummaries(agent) };
  })

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

  .get("/:id/system-info", ({ params: { id } }) => {
    const agent = getAgent(id);
    return agent.getSystemInfo();
  })

  .post("/:id/stop", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.stop();
    return { status: "stopping" };
  })

  .post("/:id/clear", ({ params: { id } }) => {
    const agent = getAgent(id);
    agent.clearHistory();
    return { status: "cleared" };
  })

  .get("/:id/operator-types", ({ params: { id } }) => {
    const agent = getAgent(id);
    const metadataStore = agent.getMetadataStore();
    const allTypes = metadataStore.getAllOperatorTypes();
    return Object.entries(allTypes).map(([type, description]) => ({ type, description }));
  })

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

  .patch(
    "/:id/settings",
    ({ params: { id }, body }) => {
      const agent = getAgent(id);
      const settings = body as UpdateAgentSettingsRequest;

      log.info(
        {
          agentId: id,
          maxOperatorResultCharLimit: settings.maxOperatorResultCharLimit,
          maxOperatorResultCellCharLimit: settings.maxOperatorResultCellCharLimit,
        },
        "updating agent settings"
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
        operatorResultSerializationMode: t.Optional(t.Literal("tsv")),
        toolTimeoutSeconds: t.Optional(t.Number()),
        executionTimeoutMinutes: t.Optional(t.Number()),
        maxSteps: t.Optional(t.Number()),
        disabledTools: t.Optional(t.Array(t.String())),
        allowedOperatorTypes: t.Optional(t.Array(t.String())),
      }),
    }
  );

function getOperatorResultSummaries(agent: TexeraAgent): Record<string, OperatorResultSummary> {
  const resultState = agent.getWorkflowResultState();
  const visible = resultState.getAllVisible();
  const results: Record<string, OperatorResultSummary> = {};
  for (const [opId, entry] of visible) {
    const info = entry.operatorInfo;
    results[opId] = {
      state: info.state,
      inputTuples: info.inputTuples,
      outputTuples: info.outputTuples,
      inputPortShapes: info.inputPortShapes,
      outputColumns: info.result && info.result.length > 0 ? getVisibleResultHeaders(info.result[0]).length : undefined,
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

// Send a single server event to one client. Each event is constructed with
// `new WsServer*Event(...)`, so the `type` tag is never hand-written here.
function sendEventToClient(ws: { send(data: string): void }, event: WsServerEvent): void {
  ws.send(JSON.stringify(event));
}

// Broadcast a server event to every client attached to the agent.
function broadcastToAgentClients(agentId: string, event: WsServerEvent): void {
  const agent = agentStore.get(agentId);
  if (!agent) return;

  const serializedEvent = JSON.stringify(event);
  for (const ws of agent.getClients()) {
    try {
      ws.send(serializedEvent);
    } catch (error) {
      wsLog.error({ agentId, err: error }, "failed to send event to a client");
      agent.removeClient(ws);
    }
  }
}

export function buildApp() {
  return new Elysia()
    .use(cors())
    .group(env.API_PREFIX, app =>
      app
        .get("/healthcheck", () => ({
          status: "ok",
          timestamp: new Date().toISOString(),
        }))
        .use(agentsRouter)
    )
    .ws(`${env.API_PREFIX}/agents/:id/react`, {
      open(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client connected");

        const agent = agentStore.get(agentId);
        if (!agent) {
          sendEventToClient(ws, new WsServerErrorEvent("Agent not found"));
          ws.close();
          return;
        }

        agent.addClient(ws);

        sendEventToClient(ws, new WsServerSnapshotEvent(agent.getState(), agent.getAllSteps(), agent.getHead()));
      },

      async message(ws, messageData) {
        const agentId = (ws.data as any).params?.id;
        const agent = agentStore.get(agentId);

        if (!agent) {
          sendEventToClient(ws, new WsServerErrorEvent("Agent not found"));
          return;
        }

        let msg: WsClientCommand;
        try {
          msg = typeof messageData === "string" ? JSON.parse(messageData) : (messageData as WsClientCommand);
        } catch {
          sendEventToClient(ws, new WsServerErrorEvent("Invalid message format"));
          return;
        }

        switch (msg.type) {
          case "WsClientStopCommand":
            agent.stop();
            broadcastToAgentClients(agentId, new WsServerStatusEvent(AgentState.STOPPING));
            return;

          case "WsClientPromptCommand": {
            if (!msg.content || typeof msg.content !== "string") {
              sendEventToClient(ws, new WsServerErrorEvent("Message content is required"));
              return;
            }

            wsLog.info({ agentId, preview: msg.content.substring(0, 50) }, "received command");

            agent.setStepCallback((step: ReActStep) => {
              broadcastToAgentClients(agentId, new WsServerStepEvent(step));
            });

            broadcastToAgentClients(agentId, new WsServerStatusEvent(AgentState.GENERATING));

            try {
              const result = await agent.sendMessage(msg.content, msg.messageSource);

              agent.setStepCallback(null);

              const allSteps = agent.getReActSteps();
              const lastStep = allSteps[allSteps.length - 1];
              if (lastStep && lastStep.isEnd) {
                broadcastToAgentClients(agentId, new WsServerStepEvent(lastStep));
              }

              wsLog.info({ agentId, steps: result.messages.length }, "agent run complete");
            } catch (error: any) {
              agent.setStepCallback(null);
              broadcastToAgentClients(agentId, new WsServerErrorEvent(error.message));
            } finally {
              // The run is over (success or failure) and TexeraAgent.sendMessage has
              // reset the agent to its resting state (AVAILABLE) in its own finally.
              // This status frame is the run-end signal (it also unsticks the client
              // from GENERATING after errors).
              broadcastToAgentClients(agentId, new WsServerStatusEvent(agent.getState()));
            }
            return;
          }

          default:
            // Frames are parsed from untrusted JSON; reject unknown discriminators
            // explicitly instead of silently no-op'ing, so client/server mismatches
            // are easy to diagnose.
            sendEventToClient(ws, new WsServerErrorEvent(`Unknown message type: ${(msg as { type?: unknown }).type}`));
        }
      },

      close(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client disconnected");

        const agent = agentStore.get(agentId);
        if (agent) {
          agent.removeClient(ws);
        }
      },
    })
    .onError(({ error, set }) => {
      // Catch-all for non-router routes such as /api/healthcheck and the websocket route.
      log.error({ err: error }, "request error");
      set.status = 500;
      return { error: error instanceof Error ? error.message : String(error) };
    });
}

// Reset module-level state. Used by tests to start each case from a clean store.
export function _resetAgentStoreForTests(): void {
  agentStore.clear();
  agentCounter = 0;
}

// Look up an agent instance by id. Used by tests to stub agent behavior (e.g.
// `sendMessage`) when exercising the WebSocket handlers.
export function _getAgentForTests(agentId: string): TexeraAgent | undefined {
  return agentStore.get(agentId);
}

function printStartupMessage(app: ReturnType<typeof buildApp>) {
  const LINE = "=".repeat(60);
  console.log(LINE);
  console.log("Texera Agent Service (Elysia.js + RxJS)");
  console.log(LINE);
  console.log(`Server running at http://localhost:${env.PORT}`);
  console.log("");

  console.log("Registered Routes:");
  const routes = app.routes;

  const httpRoutes = routes.filter(r => r.method !== "WS");
  const wsRoutes = routes.filter(r => r.method === "WS");

  for (const route of httpRoutes) {
    const method = route.method.padEnd(6);
    console.log(`  ${method} ${route.path}`);
  }

  if (wsRoutes.length > 0) {
    console.log("");
    console.log("WebSocket Endpoints:");
    for (const route of wsRoutes) {
      console.log(`  WS     ${route.path}`);
    }
    console.log("         Send: { type: 'WsClientPromptCommand', content: '...' }");
    console.log("         Send: { type: 'WsClientStopCommand' }");
    console.log(
      "         Recv: { type: 'WsServerSnapshotEvent' | 'WsServerStepEvent' | 'WsServerStatusEvent' | 'WsServerErrorEvent', ... }"
    );
  }

  console.log("");
  console.log("Environment:");
  console.log(`  LLM_ENDPOINT: ${getBackendConfig().modelsEndpoint}`);
  console.log(`  WORKFLOW_COMPILING_SERVICE_ENDPOINT: ${getBackendConfig().compileEndpoint}`);
  console.log(`  TEXERA_DASHBOARD_SERVICE_ENDPOINT: ${getBackendConfig().apiEndpoint}`);
  console.log("");
  console.log("Features:");
  console.log("  - Auto-persistence with debounce (500ms)");
  console.log(LINE);
}

async function initializeServices() {
  try {
    log.info("initializing global workflow system metadata");
    const metadata = await WorkflowSystemMetadata.initializeGlobal();
    log.info({ operatorCount: metadata.getOperatorCount() }, "loaded operators into global metadata");
  } catch (error) {
    log.warn({ err: error }, "failed to initialize global metadata; agents will initialize individually");
  }
}

export async function start() {
  await initializeServices();
  const app = buildApp().listen(env.PORT);
  printStartupMessage(app);
  return app;
}

// Run the server only when this file is the entry point, not when it is
// imported by tests or other modules.
if (import.meta.main) start();
