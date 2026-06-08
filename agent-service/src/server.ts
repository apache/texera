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
import { randomUUID } from "crypto";
import { TexeraAgent } from "./agent/texera-agent";
import { getVisibleResultHeaders } from "./agent/tools/tools-utility";
import { getBackendConfig } from "./api/backend-api";
import { validateToken, getUidFromToken } from "./api/auth-api";
import { type AgentMetadata, type AgentMetadataStore, PostgresAgentMetadataStore } from "./api/agent-metadata-store";
import { WorkflowSystemMetadata } from "./agent/util/workflow-system-metadata";
import { env } from "./config/env";
import { createLogger } from "./logger";

const log = createLogger("Server");
const wsLog = createLogger("WS");
import type {
  AgentInfo,
  AgentTaskContext,
  CreateAgentRequest,
  UpdateAgentSettingsRequest,
  ReActStep,
} from "./types/agent";
import { OperatorResultSerializationMode } from "./types/agent";

const agentStore = new Map<string, TexeraAgent>();
let agentMetadataStore: AgentMetadataStore = new PostgresAgentMetadataStore();

// Bearer token from the Authorization header (HTTP) or the access-token query
// parameter (WebSocket, since browsers cannot set headers on the WS handshake).
function extractBearerToken(
  headers: Record<string, string | undefined> | undefined,
  query: Record<string, string | undefined> | undefined
): string | undefined {
  const auth = headers?.authorization;
  if (auth && auth.startsWith("Bearer ")) {
    const token = auth.slice("Bearer ".length).trim();
    if (token) return token;
  }
  const q = query?.["access-token"];
  return typeof q === "string" && q.length > 0 ? q : undefined;
}

// Enforces authentication and per-user isolation. Throws errors the router's
// onError maps to 401/403.
async function authorizeAgentAccess(agentId: string, token: string | undefined): Promise<AgentMetadata> {
  if (!token || !validateToken(token)) {
    throw new Error("Unauthorized");
  }
  const metadata = await agentMetadataStore.getAgent(agentId);
  if (!metadata) {
    throw new Error("Agent not found");
  }
  if (getUidFromToken(token) !== metadata.ownerUid) {
    throw new Error("Forbidden");
  }
  return metadata;
}

async function createAgentInstance(options: {
  modelType: string;
  name?: string;
  agentId?: string;
  createdAt?: Date;
  config?: AgentMetadata["config"];
  reactSteps?: ReActStep[];
}): Promise<{ agentId: string; agent: TexeraAgent }> {
  const agentId = options.agentId ?? randomUUID();
  const config = getBackendConfig();

  const openai = createOpenAI({
    baseURL: `${config.modelsEndpoint}/api`,
    apiKey: env.LLM_API_KEY,
  });

  // Reasoning effort variants are configured as separate model entries in litellm-config.yaml
  // with extra_body to inject reasoning_effort, bypassing LiteLLM's param validation.
  const agent = new TexeraAgent({
    model: openai.chat(options.modelType),
    modelType: options.modelType,
    agentId,
    agentName: options.name || "Bob",
    createdAt: options.createdAt,
    persistedConfig: options.config,
    reactSteps: options.reactSteps,
  });

  await agent.initialize();

  agentStore.set(agentId, agent);
  log.info({ agentId }, "created agent");

  return { agentId, agent };
}

function getAgentInfo(agentId: string, agent: TexeraAgent): AgentInfo {
  return {
    id: agentId,
    name: agent.agentName,
    modelType: agent.modelType,
    state: agent.getState(),
    createdAt: agent.createdAt,
    settings: agent.getSettingsApi(),
  };
}

async function getAgent(agentId: string, metadata?: AgentMetadata): Promise<TexeraAgent> {
  const existing = agentStore.get(agentId);
  if (existing) {
    return existing;
  }

  const persisted = metadata ?? (await agentMetadataStore.getAgent(agentId));
  if (!persisted) {
    throw new Error("Agent not found");
  }

  const { agent } = await createAgentInstance({
    agentId: persisted.id,
    modelType: persisted.modelType,
    name: persisted.name,
    createdAt: persisted.createdAt,
    config: persisted.config,
    reactSteps: persisted.reactSteps,
  });
  return agent;
}

async function persistAgentConfig(agentId: string, agent: TexeraAgent): Promise<void> {
  await agentMetadataStore.updateAgentConfig(agentId, agent.getPersistedConfig());
}

async function persistAgentReActSteps(agentId: string, agent: TexeraAgent): Promise<void> {
  await agentMetadataStore.updateAgentReActSteps(agentId, agent.getAllSteps());
}

const agentsRouter = new Elysia({ prefix: "/agents" })
  // Error handler must live on the same Elysia instance whose routes throw, or
  // its scope will not see the errors. Elysia 1.x defaults to local scoping for
  // .onError, so attach here rather than on the outer app.
  .onError(({ error, set }) => {
    log.error({ err: error }, "request error");
    const errorMessage = error instanceof Error ? error.message : String(error);
    if (errorMessage === "Agent not found") {
      set.status = 404;
      return { error: "Agent not found" };
    }
    if (errorMessage === "Invalid or expired token") {
      set.status = 401;
      return { error: "Invalid or expired token" };
    }
    if (errorMessage === "Unauthorized") {
      set.status = 401;
      return { error: "Unauthorized" };
    }
    if (errorMessage === "Forbidden") {
      set.status = 403;
      return { error: "Forbidden" };
    }
    if (errorMessage === "modelType is required") {
      set.status = 400;
      return { error: "modelType is required" };
    }
    if (errorMessage === "workflowId is required" || errorMessage === "computingUnitId is required") {
      set.status = 400;
      return { error: errorMessage };
    }
    set.status = 500;
    return { error: errorMessage || "Internal server error" };
  })
  // Enforce ownership for every /:id route in one place. List and create carry
  // no :id and are authorized in their own handlers.
  .onBeforeHandle(async ({ params, headers, query }) => {
    const id = (params as Record<string, string | undefined>)?.id;
    if (!id) return;
    await authorizeAgentAccess(id, extractBearerToken(headers as any, query as any));
  })
  .get("/", async ({ headers, query }) => {
    const token = extractBearerToken(headers as any, query as any);
    if (!token || !validateToken(token)) {
      throw new Error("Unauthorized");
    }
    const uid = getUidFromToken(token);
    if (uid === undefined) {
      throw new Error("Unauthorized");
    }
    const ownedAgents = await agentMetadataStore.listAgentsByOwner(uid);
    const visible = await Promise.all(
      ownedAgents.map(async metadata => [metadata.id, await getAgent(metadata.id, metadata)] as const)
    );
    return { agents: visible.map(([id, agent]) => getAgentInfo(id, agent)) };
  })

  .post(
    "/",
    async ({ body, headers, query }) => {
      const { modelType, name, settings } = body as CreateAgentRequest;

      if (!modelType) {
        throw new Error("modelType is required");
      }

      const token = extractBearerToken(headers as any, query as any);
      if (!token) {
        throw new Error("Unauthorized");
      }
      if (!validateToken(token)) {
        throw new Error("Invalid or expired token");
      }

      const uid = getUidFromToken(token);
      if (uid === undefined) {
        throw new Error("Unauthorized");
      }

      const { agentId, agent } = await createAgentInstance({ modelType, name });

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

      try {
        await agentMetadataStore.createAgent({
          id: agentId,
          ownerUid: uid,
          name: agent.agentName,
          modelType,
          createdAt: agent.createdAt,
          config: agent.getPersistedConfig(),
          reactSteps: agent.getAllSteps(),
        });
      } catch (error) {
        agent.destroy();
        agentStore.delete(agentId);
        throw error;
      }

      return getAgentInfo(agentId, agent);
    },
    {
      body: t.Object({
        modelType: t.String(),
        name: t.Optional(t.String()),
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

  .get("/:id", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    return {
      ...getAgentInfo(id, agent),
      workflow: agent.getWorkflowState().getWorkflowContent(),
      stepCount: agent.getReActSteps().length,
    };
  })

  .delete("/:id", async ({ params: { id } }) => {
    const agent = agentStore.get(id);

    await agentMetadataStore.deleteAgent(id);
    agent?.destroy();
    agentStore.delete(id);
    return { deleted: true };
  })

  .get("/:id/react-steps", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    return { steps: agent.getReActSteps(), state: agent.getState() };
  })

  .get("/:id/operator-results", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    return { results: getOperatorResultSummaries(agent) };
  })

  .post(
    "/:id/steps-by-operators",
    async ({ params: { id }, body }) => {
      const agent = await getAgent(id);
      const { operatorIds } = body;
      return { steps: agent.getReActStepsByOperatorIds(operatorIds || []) };
    },
    {
      body: t.Object({
        operatorIds: t.Array(t.String()),
      }),
    }
  )

  .get("/:id/system-info", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    return agent.getSystemInfo();
  })

  .post("/:id/stop", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    agent.stop();
    return { status: "stopping" };
  })

  .post("/:id/clear", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    agent.clearHistory();
    await persistAgentReActSteps(id, agent);
    return { status: "cleared" };
  })

  .post("/:id/checkout", async ({ params: { id }, body }) => {
    const agent = await getAgent(id);
    const { stepId } = body as { stepId: string };
    if (!stepId) throw new Error("stepId is required");

    const success = agent.checkout(stepId);
    if (!success) throw new Error(`Step ${stepId} not found or checkout failed`);

    const allSteps = agent.getAllSteps();
    const workflowContent = agent.getWorkflowState().getWorkflowContent();

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

  .get("/:id/operator-types", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    const metadataStore = agent.getMetadataStore();
    const allTypes = metadataStore.getAllOperatorTypes();
    return Object.entries(allTypes).map(([type, description]) => ({ type, description }));
  })

  .get("/:id/settings", async ({ params: { id } }) => {
    const agent = await getAgent(id);
    return agent.getSettingsApi();
  })

  .patch(
    "/:id/settings",
    async ({ params: { id }, body }) => {
      const agent = await getAgent(id);
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

      await persistAgentConfig(id, agent);

      return agent.getSettingsApi();
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

interface WsMessage {
  type: "message" | "stop";
  content?: string;
  messageSource?: "chat" | "feedback";
  userToken?: string;
  workflowId?: number;
  workflowName?: string;
  computingUnitId?: number;
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

function parseRequiredNumber(value: unknown, fieldName: "workflowId" | "computingUnitId"): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${fieldName} is required`);
  }
  return parsed;
}

async function buildTaskContext(
  agentId: string,
  msg: WsMessage,
  headers: Record<string, string | undefined> | undefined,
  query: Record<string, string | undefined> | undefined
): Promise<AgentTaskContext> {
  const token = msg.userToken || extractBearerToken(headers, query);
  await authorizeAgentAccess(agentId, token);

  return {
    userToken: token!,
    workflowId: parseRequiredNumber(msg.workflowId, "workflowId"),
    workflowName: typeof msg.workflowName === "string" ? msg.workflowName : undefined,
    computingUnitId: parseRequiredNumber(msg.computingUnitId, "computingUnitId"),
  };
}

function getOperatorResultSummaries(agent: TexeraAgent): Record<string, OperatorResultSummaryWs> {
  const resultState = agent.getWorkflowResultState();
  const visible = resultState.getAllVisible();
  const results: Record<string, OperatorResultSummaryWs> = {};
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

function broadcastToAgent(agentId: string, message: WsOutgoingMessage): void {
  const agent = agentStore.get(agentId);
  if (!agent) return;

  const jsonMessage = JSON.stringify(message);
  for (const ws of agent.getWebsockets()) {
    try {
      ws.send(jsonMessage);
    } catch (error) {
      wsLog.error({ agentId, err: error }, "failed to send message to client");
      agent.removeWebsocket(ws);
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
      async open(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client connected");

        // Browsers cannot set headers on a WS handshake, so the token is read
        // from the access-token query parameter (consistent with the other
        // Texera websocket clients).
        let agent: TexeraAgent;
        try {
          const token = extractBearerToken((ws.data as any).headers, (ws.data as any).query);
          const metadata = await authorizeAgentAccess(agentId, token);
          agent = await getAgent(agentId, metadata);
        } catch (error) {
          const message = error instanceof Error ? error.message : "Unauthorized";
          ws.send(JSON.stringify({ type: "error", error: message }));
          ws.close();
          return;
        }

        agent.addWebsocket(ws);

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
        let agent: TexeraAgent;

        try {
          agent = await getAgent(agentId);
        } catch {
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
          broadcastToAgent(agentId, { type: "state", state: "STOPPING" });
          return;
        }

        if (msg.type === "message") {
          if (!msg.content || typeof msg.content !== "string") {
            ws.send(JSON.stringify({ type: "error", error: "Message content is required" }));
            return;
          }

          wsLog.info({ agentId, preview: msg.content.substring(0, 50) }, "received message");

          let taskContext: AgentTaskContext;
          try {
            taskContext = await buildTaskContext(agentId, msg, (ws.data as any).headers, (ws.data as any).query);
          } catch (error: any) {
            ws.send(JSON.stringify({ type: "error", error: error.message }));
            return;
          }

          agent.setStepCallback((step: ReActStep) => {
            const hasToolCalls = step.toolCalls && step.toolCalls.length > 0;
            broadcastToAgent(agentId, {
              type: "step",
              step,
              ...(hasToolCalls ? { operatorResults: getOperatorResultSummaries(agent) } : {}),
            });
            void persistAgentReActSteps(agentId, agent).catch(error => {
              wsLog.error({ agentId, err: error }, "failed to persist ReAct steps");
            });
          });

          broadcastToAgent(agentId, { type: "state", state: "GENERATING" });

          try {
            const result = await agent.sendMessage(msg.content, taskContext, msg.messageSource);

            agent.setStepCallback(null);

            const allSteps = agent.getReActSteps();
            const lastStep = allSteps[allSteps.length - 1];
            if (lastStep && lastStep.isEnd) {
              broadcastToAgent(agentId, { type: "step", step: lastStep });
            }
            try {
              await persistAgentReActSteps(agentId, agent);
            } catch (error) {
              wsLog.error({ agentId, err: error }, "failed to persist ReAct steps");
            }

            broadcastToAgent(agentId, {
              type: "complete",
              state: agent.getState(),
              operatorResults: getOperatorResultSummaries(agent),
            });

            wsLog.info({ agentId, steps: result.messages.length }, "agent run complete");
          } catch (error: any) {
            agent.setStepCallback(null);
            broadcastToAgent(agentId, { type: "error", error: error.message });
          }
        }
      },

      close(ws) {
        const agentId = (ws.data as any).params?.id;
        wsLog.info({ agentId }, "client disconnected");

        const agent = agentStore.get(agentId);
        if (agent) {
          agent.removeWebsocket(ws);
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
}

export function _setAgentMetadataStoreForTests(store: AgentMetadataStore): void {
  agentMetadataStore = store;
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
    console.log("         Send: { type: 'message', content: '...' }");
    console.log("         Send: { type: 'stop' }");
    console.log("         Recv: { type: 'step' | 'state' | 'complete' | 'error' | 'init', ... }");
  }

  console.log("");
  console.log("Environment:");
  console.log(`  LLM_API_KEY: ${env.LLM_API_KEY === "dummy" ? "dummy (default)" : "set"}`);
  console.log(`  LLM_ENDPOINT: ${getBackendConfig().modelsEndpoint}`);
  console.log(`  WORKFLOW_COMPILING_SERVICE_ENDPOINT: ${getBackendConfig().compileEndpoint}`);
  console.log(`  TEXERA_DASHBOARD_SERVICE_ENDPOINT: ${getBackendConfig().apiEndpoint}`);
  console.log("");
  console.log("Features:");
  console.log("  - Agent metadata and ReAct state persistence");
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
if (import.meta.main) {
  start();
}
