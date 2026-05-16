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

import { generateText, tool, type ModelMessage, type LanguageModel, stepCountIs } from "ai";
import { z } from "zod";
import { Subscription } from "rxjs";
import { debounceTime } from "rxjs/operators";
import { WorkflowState } from "./workflow-state";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";
import { WorkflowResultState } from "./workflow-result-state";
import { formatOperatorResult } from "./tools/result-formatting";
import type { AgentSettings, ReActStep, TokenUsage, UserInfo, FileContext } from "../types/agent";
import {
  AgentState as AgentStateEnum,
  DEFAULT_AGENT_SETTINGS,
  OperatorResultSerializationMode,
  INITIAL_STEP_ID,
} from "../types/agent";
import { buildSystemPrompt } from "./prompts";
import {
  createAddOperatorTool,
  createModifyOperatorTool,
  createDeleteOperatorTool,
  TOOL_NAME_ADD_OPERATOR,
  TOOL_NAME_MODIFY_OPERATOR,
  TOOL_NAME_DELETE_OPERATOR,
  type ToolContext,
} from "./tools/workflow-crud-tools";
import {
  createExecuteOperatorTool,
  executeOperatorAndFormat,
  TOOL_NAME_EXECUTE_OPERATOR,
  type ExecutionConfig,
} from "./tools/workflow-execution-tools";
import { assembleContext } from "./util/context-utils";
import { compileWorkflowAsync, type WorkflowCompilationResponse } from "../api/compile-api";
import { env } from "../config/env";
import { createLogger } from "../logger";
import type { Logger } from "pino";

const PERSIST_DEBOUNCE_MS = 500;

/**
 * Derive a concise, human-readable workflow name from the navigateToWorkflow summary.
 * Looks for a bolded filename or the first meaningful sentence.
 */
function extractWorkflowName(summary: string): string | null {
  // Try to extract a bold filename: **filename.csv** or **Name Analysis**
  const boldMatch = summary.match(/\*\*([^*]{3,60})\*\*/);
  if (boldMatch) {
    const candidate = boldMatch[1].replace(/\.(csv|json|txt|xlsx?|parquet)$/i, "").trim();
    if (candidate.length >= 3) return candidate.substring(0, 60);
  }
  // Fall back to first sentence (≤ 50 chars)
  const firstLine = summary.split("\n")[0].replace(/[*`]/g, "").trim();
  if (firstLine.length >= 5 && firstLine.length <= 60) return firstLine;
  return null;
}

export interface TexeraAgentConfig {
  model: LanguageModel;
  modelType: string;
  agentId: string;
  agentName?: string;
  systemPrompt?: string;
}

export interface AgentMessageResult {
  response: string;
  messages: ModelMessage[];
  usage: TokenUsage;
  stopped: boolean;
  error?: string;
}

type ReActStepCallback = (step: ReActStep) => void;

/**
 * A single Texera agent instance.
 *
 * Owns the conversation (ReAct step tree with HEAD/checkout semantics), the
 * workflow being edited (`WorkflowState`), cached operator execution results
 * (`WorkflowResultState`), and the tool surface exposed to the LLM. Each call
 * to `sendMessage` drives one multi-step generation via the Vercel AI SDK,
 * streaming step updates to subscribed websockets.
 */
export class TexeraAgent {
  readonly agentId: string;
  readonly agentName: string;
  readonly modelType: string;
  readonly createdAt: Date;

  private state: AgentStateEnum = AgentStateEnum.AVAILABLE;
  private workflowState: WorkflowState;
  private metadataStore: WorkflowSystemMetadata;
  private head: string = INITIAL_STEP_ID;
  private stepsById: Map<string, ReActStep> = new Map();
  private stepCounter = 0;
  private workflowResultState: WorkflowResultState;

  private websockets: Set<any> = new Set();

  private model: LanguageModel;
  private systemPrompt: string;
  private settings: AgentSettings;

  private reActStepsByMessageId: Map<string, ReActStep[]> = new Map();

  private currentMessageId: string | undefined = undefined;

  private delegateConfig?: {
    userToken: string;
    userInfo?: UserInfo;
    workflowId?: number;
    workflowName?: string;
    computingUnitId?: number;
  };

  private stepCallback: ReActStepCallback | null = null;
  private navigateCallback: ((url: string) => void) | null = null;
  private shouldStopGeneration = false;
  private navigationFiredThisTurn = false;
  private currentFileContext: FileContext | undefined = undefined;
  // Persists the last uploaded file across turns so the agent can use it in follow-up messages.
  private lastSeenFileContext: FileContext | undefined = undefined;
  private operatorTypeAddCount: Map<string, number> = new Map();
  private operatorModifyCount: Map<string, number> = new Map();
  private listCallCount: Map<string, number> = new Map();

  private messageCounter = 0;

  private tools: Record<string, any>;

  private abortController: AbortController | null = null;

  private workflowChangeSubscription: Subscription | null = null;

  private log: Logger;

  constructor(config: TexeraAgentConfig) {
    this.agentId = config.agentId;
    this.agentName = config.agentName || `Agent-${config.agentId}`;
    this.modelType = config.modelType;
    this.createdAt = new Date();
    this.model = config.model;
    this.systemPrompt = config.systemPrompt || "";
    this.log = createLogger("TexeraAgent", { agentId: this.agentId });

    this.workflowState = new WorkflowState();
    this.metadataStore = WorkflowSystemMetadata.getInstance();
    this.workflowResultState = new WorkflowResultState(() => this.getAncestorPath());

    const initialStep: ReActStep = {
      id: INITIAL_STEP_ID,
      messageId: "initial",
      stepId: -1,
      timestamp: Date.now(),
      role: "user",
      content: "",
      isBegin: true,
      isEnd: true,
      parentId: undefined,
    };
    this.stepsById.set(INITIAL_STEP_ID, initialStep);

    this.settings = {
      ...DEFAULT_AGENT_SETTINGS,
      systemPrompt: this.systemPrompt,
    };

    this.tools = this.createTools();
  }

  async initialize(): Promise<void> {
    try {
      if (!this.metadataStore.isInitialized()) {
        await this.metadataStore.initializeFromBackend();
      }

      this.rebuildSystemPrompt();

      this.tools = this.createTools();
      this.log.info({ operatorCount: this.metadataStore.getOperatorCount() }, "agent initialized");
    } catch (error) {
      this.log.error({ err: error }, "failed to initialize metadata");
    }
  }

  private rebuildSystemPrompt(): void {
    this.systemPrompt = buildSystemPrompt(this.metadataStore, this.settings.allowedOperatorTypes);
    this.settings.systemPrompt = this.systemPrompt;
  }

  private buildExecutionConfig(): ExecutionConfig | undefined {
    if (!this.delegateConfig) return undefined;
    return {
      userToken: this.delegateConfig.userToken,
      workflowId: this.delegateConfig.workflowId ?? 0,
      computingUnitId: this.delegateConfig.computingUnitId,
      maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: this.settings.maxOperatorResultCellCharLimit,
      executionTimeoutMs: this.settings.executionTimeoutMs,
    };
  }

  /** Lazily creates a workflow the first time the agent needs to build operators. */
  private async ensureWorkflow(): Promise<void> {
    if (this.delegateConfig?.workflowId || !this.delegateConfig?.userToken) return;
    try {
      const { createWorkflow } = await import("../api/workflow-api");
      // Name the workflow after the file being analyzed, or fall back to a generic name
      const fc = this.lastSeenFileContext ?? this.currentFileContext;
      const workflowName = fc?.fileName
        ? fc.fileName.replace(/\.[^.]+$/, "").replace(/_/g, " ")  // strip extension, underscores → spaces
        : "Agent Workflow";
      const wid = await createWorkflow(this.delegateConfig.userToken, workflowName);
      // Store workflowName so auto-persist uses it instead of "Agent Workflow"
      this.delegateConfig = { ...this.delegateConfig, workflowId: wid, workflowName };
      this.setupWorkflowChangeHandlers();
      this.log.info({ wid }, "lazily created workflow for agent");
    } catch (e: any) {
      this.log.warn({ err: e?.message }, "failed to lazily create workflow");
    }
  }

  private createTools(): Record<string, any> {
    const operatorSchemas = new Map<string, any>();
    for (const type of Object.keys(this.metadataStore.getAllOperatorTypes())) {
      const jsonSchema = this.metadataStore.getSchema(type);
      const additionalMetadata = this.metadataStore.getAdditionalMetadata(type);
      if (jsonSchema) {
        operatorSchemas.set(type, { jsonSchema, additionalMetadata });
      }
    }

    // Expose executeOperator whenever a delegate config (user token + workflow) is present.
    // If no computingUnitId is set, the tool auto-discovers a running unit at call time.
    const getExecutionConfig = this.delegateConfig ? () => this.buildExecutionConfig()! : undefined;

    const context: ToolContext = {
      metadataStore: this.metadataStore,
      settings: {
        maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
        toolTimeoutMs: this.settings.toolTimeoutMs,
        executionTimeoutMs: this.settings.executionTimeoutMs,
      },
      // Fall back to the last uploaded file if no file was attached to the current message
      getFileContext: () => this.currentFileContext ?? this.lastSeenFileContext,
      operatorTypeAddCount: this.operatorTypeAddCount,
      operatorModifyCount: this.operatorModifyCount,
      ensureWorkflow: () => this.ensureWorkflow(),
      resetWorkflow: () => {
        // Unlink the current workflow so ensureWorkflow() will create a fresh one.
        if (this.delegateConfig) {
          this.delegateConfig = { ...this.delegateConfig, workflowId: undefined, workflowName: undefined };
        }
        // Clear all operators and links from the in-memory state.
        this.workflowState.setWorkflowContent({ operators: [], links: [], operatorPositions: {}, commentBoxes: [], settings: { dataTransferBatchSize: 400 } });
        this.log.info("reset workflow for new file analysis");
      },
      abort: () => {
        this.shouldStopGeneration = true;
        this.abortController?.abort();
      },
    };

    const tools: Record<string, any> = {
      [TOOL_NAME_DELETE_OPERATOR]: createDeleteOperatorTool(this.workflowState, context),
      [TOOL_NAME_ADD_OPERATOR]: createAddOperatorTool(this.workflowState, operatorSchemas, context),
      [TOOL_NAME_MODIFY_OPERATOR]: createModifyOperatorTool(this.workflowState, context),
    };

    if (getExecutionConfig) {
      tools[TOOL_NAME_EXECUTE_OPERATOR] = createExecuteOperatorTool(
        this.workflowState,
        getExecutionConfig,
        (opId, operatorInfo) => {
          this.workflowResultState.set(opId, this.head, operatorInfo);
        }
      );
    }

    // Content-discovery and navigation tools — available whenever a user token exists.
    if (this.delegateConfig?.userToken) {
      const userToken = this.delegateConfig.userToken;
      const dashboardEndpoint = env.TEXERA_DASHBOARD_SERVICE_ENDPOINT;
      const fileEndpoint = env.FILE_SERVICE_ENDPOINT;
      const navCb = () => this.navigateCallback;

      tools["listWorkflows"] = tool({
        description:
          "List the user's existing workflows. Use this when the user mentions a previous workflow " +
          "or wants to open, continue, or reference one they built before. " +
          "Returns wid (workflow ID), name, and last-modified date.",
        inputSchema: z.object({
          query: z.string().optional().describe("Optional name filter (case-insensitive substring match)"),
        }),
        execute: async ({ query }: { query?: string }) => {
          const key = `listWorkflows:${query ?? ""}`;
          const prev = this.listCallCount.get(key) ?? 0;
          if (prev >= 2) {
            this.shouldStopGeneration = true;
            this.abortController?.abort();
            return query
              ? `No workflow named "${query}" was found after searching twice. Navigate to "workflows" to let the user browse and open manually.`
              : `Workflow list already retrieved twice. Stop repeating — present the results to the user or navigate to "workflows".`;
          }
          this.listCallCount.set(key, prev + 1);
          try {
            const res = await fetch(`${dashboardEndpoint}/api/workflow/list`, {
              headers: { Authorization: `Bearer ${userToken}` },
            });
            if (!res.ok) return `Failed to list workflows: ${res.status}`;
            const items = (await res.json()) as Array<{ workflow: { wid: number; name: string; lastModifiedTime: number } }>;
            const filtered = query
              ? items.filter(i => i.workflow.name.toLowerCase().includes(query.toLowerCase()))
              : items;
            if (!filtered.length)
              return query
                ? `No workflows matching "${query}". Try a different search or use navigate("workflows") to browse all.`
                : "No workflows found.";

            // Sort by most recently modified
            const sorted = [...filtered].sort((a, b) => (b.workflow.lastModifiedTime ?? 0) - (a.workflow.lastModifiedTime ?? 0));
            const lines = sorted.slice(0, 20).map((i, idx) => {
              const wf = i.workflow;
              const age = wf.lastModifiedTime
                ? new Date(wf.lastModifiedTime).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
                : "unknown";
              return `${idx + 1}. **${wf.name}** — modified ${age} (wid: ${wf.wid})`;
            });
            const shownNote = sorted.length > 20 ? `, showing 20 most recent` : "";
            return `📋 **Your workflows** (${filtered.length} total${shownNote}):\n\n${lines.join("\n")}\n\nTo open one, say "open wid [number]".`;
          } catch (e: any) {
            return `Error listing workflows: ${e.message}`;
          }
        },
      });

      tools["listDatasets"] = tool({
        description:
          "List the user's uploaded datasets/files. Use this when the user wants to use a file " +
          "they uploaded previously, or to discover what data is available. " +
          "Returns dataset name, ID, and creation date.",
        inputSchema: z.object({
          query: z.string().optional().describe("Optional name filter (case-insensitive substring match)"),
        }),
        execute: async ({ query }: { query?: string }) => {
          const key = `listDatasets:${query ?? ""}`;
          const prev = this.listCallCount.get(key) ?? 0;
          if (prev >= 2) {
            this.shouldStopGeneration = true;
            this.abortController?.abort();
            return query
              ? `No dataset named "${query}" was found after searching twice. Navigate to "datasets" to let the user browse manually.`
              : `Dataset list already retrieved twice. Stop repeating — present the results to the user or navigate to "datasets".`;
          }
          this.listCallCount.set(key, prev + 1);
          try {
            const res = await fetch(`${fileEndpoint}/api/dataset/list`, {
              headers: { Authorization: `Bearer ${userToken}` },
            });
            if (!res.ok) return `Failed to list datasets: ${res.status}`;
            const items = (await res.json()) as Array<{
              dataset: { did: number; name: string; description: string; creationTime: number };
              ownerEmail: string;
            }>;

            // Resolve display name and file path for each dataset
            const resolveEntry = async (
              ds: { did: number; name: string; description: string; creationTime: number },
              ownerEmail: string
            ): Promise<{ displayName: string; filePath: string; date: string }> => {
              const date = ds.creationTime
                ? new Date(ds.creationTime).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
                : "unknown date";
              const desc = ds.description ?? "";

              // New format: stored at upload time
              if (desc.startsWith("agent-upload:")) {
                const safeFileName = desc.slice("agent-upload:".length);
                return { displayName: safeFileName, filePath: `/${ownerEmail}/${ds.name}/v1/${safeFileName}`, date };
              }

              // Fetch file list from version/latest
              try {
                const vRes = await fetch(`${fileEndpoint}/api/dataset/${ds.did}/version/latest`, {
                  headers: { Authorization: `Bearer ${userToken}` },
                });
                if (vRes.ok) {
                  const vData = (await vRes.json()) as {
                    fileNodes?: Array<{ name: string; parentDir: string; type: string }>;
                  };
                  const fileNode = (vData.fileNodes ?? []).find(n => n.type === "file");
                  if (fileNode) {
                    return { displayName: fileNode.name, filePath: `${fileNode.parentDir}/${fileNode.name}`, date };
                  }
                }
              } catch { /* ignore */ }

              // Last resort: strip timestamp prefix from dataset name
              const bare = ds.name.replace(/^agent_upload_\d+_/, "").replace(/_/g, " ").replace(/\s+csv$/i, ".csv");
              return { displayName: bare, filePath: "", date };
            };

            // Resolve all entries
            const resolved = await Promise.all(
              items.map(i => resolveEntry(i.dataset, i.ownerEmail ?? "unknown").then(e => ({ ...e, did: i.dataset.did })))
            );

            // Apply query filter on display name
            const filteredResolved = query
              ? resolved.filter(e => e.displayName.toLowerCase().includes(query.toLowerCase()))
              : resolved;

            if (!filteredResolved.length)
              return query
                ? `No files matching "${query}". Try a different search term.`
                : "No uploaded files found.";

            // Deduplicate by display name — keep the most recent copy of each file
            const byName = new Map<string, typeof filteredResolved[0]>();
            const countByName = new Map<string, number>();
            for (const e of filteredResolved.sort((a, b) => b.did - a.did)) {
              countByName.set(e.displayName, (countByName.get(e.displayName) ?? 0) + 1);
              if (!byName.has(e.displayName)) byName.set(e.displayName, e);
            }

            const unique = Array.from(byName.values()).slice(0, 20);
            const totalMsg = filteredResolved.length !== byName.size
              ? ` (${filteredResolved.length} total versions)`
              : "";

            const lines = unique.map((e, idx) => {
              const copies = countByName.get(e.displayName) ?? 1;
              const copiesNote = copies > 1 ? ` (${copies} versions)` : "";
              const pathNote = e.filePath ? ` — filePath: \`${e.filePath}\`` : "";
              return `${idx + 1}. **${e.displayName}**${copiesNote} — uploaded ${e.date}${pathNote}`;
            });

            return `📁 **Your uploaded files** (${byName.size} unique${totalMsg}):\n\n${lines.join("\n")}\n\nTo load a file, say "load [filename]".`;
          } catch (e: any) {
            return `Error listing datasets: ${e.message}`;
          }
        },
      });

      tools["navigate"] = tool({
        description:
          "Navigate the user's browser to any page in the Texera app. " +
          "Choose the destination that best matches what the user asked for. " +
          "This is a terminal action — call it last, after all data retrieval is done.",
        inputSchema: z.object({
          destination: z
            .enum([
              "dashboard",   // home / landing page
              "workflows",   // my workflows list
              "datasets",    // my datasets list (overview)
              "dataset",     // open a SPECIFIC dataset detail page — requires datasetId
              "compute",     // computing units — use this for: "computing unit", "compute", "start a worker", "my compute resources"
              "quota",       // storage/usage quota — use this for: "usage", "quota", "storage limit", "how much space"
              "projects",    // my projects
              "discussion",  // discussion / forum
              "hub",         // public hub (browse shared workflows)
              "workflow",    // open THIS agent's current workflow (never pass a workflowId here — use navigateToWorkflow instead)
            ])
            .describe("Page to navigate to. Use 'workflow' to open the current agent workflow. Use 'dataset' with datasetId to open a specific file. Use 'compute' for computing units."),
          datasetId: z.number().optional().describe("Dataset ID — required when destination='dataset'"),
          datasetVersionId: z.number().optional().describe("Dataset version ID (dvid) — use when navigating to 'dataset' to pre-select the version"),
          message: z.string().describe("One sentence telling the user where they are going"),
        }),
        execute: async ({
          destination,
          datasetId: navDid,
          datasetVersionId: navDvid,
          message,
        }: {
          destination: string;
          datasetId?: number;
          datasetVersionId?: number;
          message: string;
        }) => {
          if (this.navigationFiredThisTurn) {
            return "Already navigated this turn. Do not call navigate again.";
          }
          this.navigationFiredThisTurn = true;
          this.shouldStopGeneration = true;

          // For "workflow" destination always use the agent's own workflow — never a hallucinated ID
          const ownWorkflowId = this.delegateConfig?.workflowId;

          const urls: Record<string, string> = {
            dashboard:  "/dashboard/home",
            workflows:  "/dashboard/user/workflow",
            datasets:   "/dashboard/user/dataset",
            dataset:    (navDid && navDid > 0) ? `/dashboard/user/dataset/${navDid}${navDvid ? `?dvid=${navDvid}` : ""}` : "/dashboard/user/dataset",
            compute:    "/dashboard/user/compute",
            quota:      "/dashboard/user/quota",
            projects:   "/dashboard/user/project",
            discussion: "/dashboard/user/discussion",
            hub:        "/dashboard/hub/workflow/result",
            workflow:   ownWorkflowId ? `/dashboard/user/workflow/${ownWorkflowId}` : "/dashboard/user/workflow",
          };
          const url = urls[destination] ?? "/dashboard/home";
          const cb = navCb();
          if (cb) cb(url);
          this.abortController?.abort();
          return `Navigating to ${destination}. ${message}`;
        },
      });
    }

    // createComputingUnit — provisions a new local computing unit for the user.
    if (this.delegateConfig?.userToken) {
      const cuToken = this.delegateConfig.userToken;
      const cuEndpoint = env.COMPUTING_UNIT_MANAGING_SERVICE_ENDPOINT;
      const cuHeaders = { Authorization: `Bearer ${cuToken}`, "Content-Type": "application/json" };
      const navCbForCU = () => this.navigateCallback;

      tools["createComputingUnit"] = tool({
        description:
          "Create a new computing unit for the user. " +
          "If the user did not provide a name, ask for one before calling this tool. " +
          "After creating, navigate to the compute page so the user can see it.",
        inputSchema: z.object({
          name: z.string().describe("A human-friendly name for the computing unit (ask the user if not provided)"),
        }),
        execute: async ({ name }: { name: string }) => {
          try {
            // Create the unit
            const createRes = await fetch(`${cuEndpoint}/api/computing-unit/create`, {
              method: "POST",
              headers: cuHeaders,
              body: JSON.stringify({
                name,
                cpuLimit: "NaN",
                memoryLimit: "NaN",
                gpuLimit: "NaN",
                jvmMemorySize: "NaN",
                shmSize: "NaN",
                uri: env.COMPUTING_UNIT_WSAPI_URI,
                unitType: "local",
              }),
            });

            if (!createRes.ok) {
              const text = await createRes.text();
              return `[ERROR] Failed to create computing unit: ${createRes.status} — ${text}`;
            }

            const created = (await createRes.json()) as { computingUnit?: { cuid: number; name: string } };
            const cuid = created.computingUnit?.cuid;
            if (!cuid) return `[ERROR] Computing unit created but no ID returned.`;

            // Poll up to 15s for Running status
            for (let i = 0; i < 15; i++) {
              await new Promise(r => setTimeout(r, 1000));
              try {
                const pollRes = await fetch(`${cuEndpoint}/api/computing-unit/${cuid}`, { headers: cuHeaders });
                if (pollRes.ok) {
                  const u = (await pollRes.json()) as { status?: string };
                  if (u.status === "Running") break;
                }
              } catch { /* ignore poll errors */ }
            }

            // Navigate to compute page and stop generation
            this.shouldStopGeneration = true;
            this.navigationFiredThisTurn = true;
            const cb = navCbForCU();
            if (cb) cb("/dashboard/user/compute");
            this.abortController?.abort();

            return `✅ Computing unit **"${name}"** created (ID: ${cuid}) and is now running. Taking you to the Compute page.`;
          } catch (e: any) {
            return `[ERROR] ${e.message}`;
          }
        },
      });
    }

    // navigateToWorkflow — always available when a user is logged in.
    // workflowId is read lazily at execution time so that ensureWorkflow() called
    // mid-turn (by addOperator) can create the workflow first and still be navigated to.
    if (this.delegateConfig?.userToken) {
      tools["navigateToWorkflow"] = tool({
        description:
          "Navigate the user's browser to the workspace. " +
          "Only call this AFTER executing operators and seeing real results. " +
          "The summary MUST include: (1) what operators were built, (2) actual numbers " +
          "from execution (row counts, metric values, column names), and (3) 2-3 specific " +
          "follow-up questions the user can ask. Format with line breaks between sections.",
        inputSchema: z.object({
          summary: z
            .string()
            .describe(
              "Structured summary with three sections separated by newlines: " +
              "Section 1 — what was built (operator names and types). " +
              "Section 2 — actual results from execution (real numbers, not descriptions). " +
              "Section 3 — 2-3 specific next steps the user can ask for."
            ),
        }),
        execute: async ({ summary }: { summary: string }) => {
          // Read workflowId lazily — ensureWorkflow() may have set it mid-turn.
          const workflowId = this.delegateConfig?.workflowId;
          if (!workflowId) {
            return "No workflow is linked to this agent yet. Build a workflow first (add and execute operators), then call navigateToWorkflow.";
          }

          // Guard against repeated calls in the same turn.
          if (this.navigationFiredThisTurn) {
            return "Navigation already sent this turn. Stop calling navigateToWorkflow — the user is being taken to the workflow now.";
          }
          this.navigationFiredThisTurn = true;
          this.shouldStopGeneration = true;

          // Auto-rename the workflow based on the summary before navigating.
          const workflowName = extractWorkflowName(summary);
          if (workflowName && this.delegateConfig?.userToken) {
            try {
              await fetch(`${env.TEXERA_DASHBOARD_SERVICE_ENDPOINT}/api/workflow/update/name`, {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  Authorization: `Bearer ${this.delegateConfig.userToken}`,
                },
                body: JSON.stringify({ wid: workflowId, name: workflowName }),
              });
            } catch {
              /* rename is best-effort — don't block navigation */
            }
          }

          const cb = this.navigateCallback;
          if (cb) {
            cb(`/dashboard/user/workflow/${workflowId}`);
          }
          this.abortController?.abort();
          return `Navigating to workflow #${workflowId}. ${summary}`;
        },
      });
    }

    return tools;
  }

  getState(): AgentStateEnum {
    return this.state;
  }

  getWorkflowState(): WorkflowState {
    return this.workflowState;
  }

  getMetadataStore(): WorkflowSystemMetadata {
    return this.metadataStore;
  }

  getHead(): string {
    return this.head;
  }

  getAncestorPath(stepId?: string): string[] {
    const target = stepId ?? this.head;
    const chain: string[] = [];
    let current: string | undefined = target;
    while (current) {
      chain.unshift(current);
      current = this.stepsById.get(current)?.parentId;
    }
    return chain;
  }

  getStepsById(): Map<string, ReActStep> {
    return this.stepsById;
  }

  getWorkflowResultState(): WorkflowResultState {
    return this.workflowResultState;
  }

  getWebsockets(): Set<any> {
    return this.websockets;
  }

  addWebsocket(ws: any): void {
    this.websockets.add(ws);
  }

  removeWebsocket(ws: any): void {
    this.websockets.delete(ws);
  }

  getReActSteps(): ReActStep[] {
    const all: ReActStep[] = [];
    for (const steps of this.reActStepsByMessageId.values()) {
      all.push(...steps);
    }
    return all;
  }

  getVisibleReActSteps(): ReActStep[] {
    const path = this.getAncestorPath();
    return path
      .filter(id => id !== INITIAL_STEP_ID)
      .map(id => this.stepsById.get(id)!)
      .filter(Boolean);
  }

  getAllSteps(): ReActStep[] {
    return Array.from(this.stepsById.values()).filter(s => s.id !== INITIAL_STEP_ID);
  }

  checkout(stepId: string): boolean {
    const step = this.stepsById.get(stepId);
    if (!step && stepId !== INITIAL_STEP_ID) return false;
    this.head = stepId;
    if (step?.afterWorkflowContent) {
      this.workflowState.setWorkflowContent(step.afterWorkflowContent);
    }
    return true;
  }

  setStepCallback(callback: ReActStepCallback | null): void {
    this.stepCallback = callback;
  }

  setNavigateCallback(callback: ((url: string) => void) | null): void {
    this.navigateCallback = callback;
  }

  private generateStepId(): string {
    return `step-${this.agentId}-${++this.stepCounter}-${Date.now()}`;
  }

  private addStep(step: ReActStep): void {
    let steps = this.reActStepsByMessageId.get(step.messageId);
    if (!steps) {
      steps = [];
      this.reActStepsByMessageId.set(step.messageId, steps);
    }
    steps.push(step);
    this.stepsById.set(step.id, step);
    if (this.stepCallback) {
      this.stepCallback(step);
    }
  }

  getSystemInfo(): {
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
  } {
    const toolsInfo = Object.entries(this.tools).map(([name, toolDef]) => {
      const description = toolDef.description || "";
      const inputSchema = toolDef.parameters || {};
      const enabled = !this.settings.disabledTools.has(name);

      return {
        name,
        description,
        inputSchema,
        enabled,
      };
    });

    return {
      systemPrompt: this.systemPrompt,
      tools: toolsInfo,
    };
  }

  getSettings(): AgentSettings {
    return { ...this.settings };
  }

  updateSettings(updates: {
    maxOperatorResultCharLimit?: number;
    maxOperatorResultCellCharLimit?: number;
    operatorResultSerializationMode?: OperatorResultSerializationMode;
    toolTimeoutMs?: number;
    executionTimeoutMs?: number;
    disabledTools?: Set<string>;
    maxSteps?: number;
    allowedOperatorTypes?: string[];
  }): void {
    let promptNeedsRebuild = false;

    if (updates.maxOperatorResultCharLimit !== undefined) {
      this.settings.maxOperatorResultCharLimit = updates.maxOperatorResultCharLimit;
    }
    if (updates.maxOperatorResultCellCharLimit !== undefined) {
      this.settings.maxOperatorResultCellCharLimit = updates.maxOperatorResultCellCharLimit;
    }
    if (updates.operatorResultSerializationMode !== undefined) {
      this.settings.operatorResultSerializationMode = updates.operatorResultSerializationMode;
    }
    if (updates.toolTimeoutMs !== undefined) {
      this.settings.toolTimeoutMs = updates.toolTimeoutMs;
    }
    if (updates.executionTimeoutMs !== undefined) {
      this.settings.executionTimeoutMs = updates.executionTimeoutMs;
    }
    if (updates.disabledTools !== undefined) {
      this.settings.disabledTools = updates.disabledTools;
    }
    if (updates.maxSteps !== undefined) {
      this.settings.maxSteps = updates.maxSteps;
    }
    if (updates.allowedOperatorTypes !== undefined) {
      this.settings.allowedOperatorTypes = updates.allowedOperatorTypes;
      promptNeedsRebuild = true;
    }

    if (promptNeedsRebuild) {
      this.rebuildSystemPrompt();
    }

    this.tools = this.createTools();
    this.log.info(
      {
        maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
        maxOperatorResultCellCharLimit: this.settings.maxOperatorResultCellCharLimit,
      },
      "settings updated"
    );
  }

  async refreshWorkflowFromBackend(): Promise<void> {
    // HEAD at a real step means the workflow is determined by that step's snapshot;
    // only reload from backend when HEAD is the initial sentinel.
    if (this.head !== INITIAL_STEP_ID) {
      return;
    }

    if (!this.delegateConfig?.workflowId || !this.delegateConfig?.userToken) {
      return;
    }

    try {
      const { retrieveWorkflow } = await import("../api/workflow-api");
      const workflow = await retrieveWorkflow(this.delegateConfig.userToken, this.delegateConfig.workflowId);
      this.workflowState.setWorkflowContent(workflow.content);
      this.log.debug({ workflowId: this.delegateConfig.workflowId }, "refreshed workflow from backend");
    } catch (error) {
      this.log.warn({ err: error }, "failed to refresh workflow from backend");
    }
  }

  setDelegateConfig(config: {
    userToken: string;
    userInfo?: UserInfo;
    workflowId?: number;
    workflowName?: string;
    computingUnitId?: number;
  }): void {
    this.delegateConfig = config;
    // Rebuild tools with the new delegate config (unlocks navigate, listDatasets, etc.)
    this.tools = this.createTools();
    this.setupWorkflowChangeHandlers();
  }

  getDelegateConfig():
    | { userToken: string; userInfo?: UserInfo; workflowId?: number; workflowName?: string; computingUnitId?: number }
    | undefined {
    return this.delegateConfig;
  }

  private setupWorkflowChangeHandlers(): void {
    if (this.workflowChangeSubscription) {
      this.workflowChangeSubscription.unsubscribe();
    }

    const subscription = new Subscription();
    const workflowChanged$ = this.workflowState.getWorkflowChangedStream();

    if (this.delegateConfig?.workflowId && this.delegateConfig.userToken) {
      const persistSubscription = workflowChanged$.pipe(debounceTime(PERSIST_DEBOUNCE_MS)).subscribe(async () => {
        if (!this.delegateConfig?.workflowId || !this.delegateConfig.userToken) {
          return;
        }

        try {
          const { persistWorkflow } = await import("../api/workflow-api");
          const workflowContent = this.workflowState.getWorkflowContent();
          await persistWorkflow(
            this.delegateConfig.userToken,
            this.delegateConfig.workflowId,
            this.delegateConfig.workflowName || "Agent Workflow",
            workflowContent
          );
          this.log.debug({ workflowId: this.delegateConfig.workflowId }, "auto-persisted workflow");
        } catch (error) {
          this.log.error({ err: error }, "failed to auto-persist workflow");
        }
      });

      subscription.add(persistSubscription);
    }

    this.workflowChangeSubscription = subscription;
    this.workflowState.addSubscription(subscription);
  }

  async sendMessage(
    userMessage: string,
    messageSource?: "chat" | "feedback",
    fileContext?: FileContext
  ): Promise<AgentMessageResult> {
    const messageId = `msg-${this.agentId}-${++this.messageCounter}-${Date.now()}`;
    let stepIndex = 0;

    await this.refreshWorkflowFromBackend();

    this.abortController = new AbortController();

    this.state = AgentStateEnum.GENERATING;

    this.currentMessageId = messageId;
    this.shouldStopGeneration = false;
    this.navigationFiredThisTurn = false;
    this.currentFileContext = fileContext;
    // Remember the file context across turns for follow-up messages like "load it"
    if (fileContext) this.lastSeenFileContext = fileContext;
    this.operatorTypeAddCount = new Map();
    this.operatorModifyCount = new Map();
    this.listCallCount = new Map();

    try {
      let beforeStepContent = this.workflowState.getWorkflowContent();

      const estimatedInputTokens = Math.ceil(userMessage.length / 4);
      const userStepId = this.generateStepId();
      const userStep: ReActStep = {
        id: userStepId,
        parentId: this.head,
        messageId,
        stepId: 0,
        timestamp: Date.now(),
        role: "user",
        content: userMessage,
        isBegin: true,
        isEnd: true,
        messageSource,
        fileContext,
        beforeWorkflowContent: beforeStepContent,
        afterWorkflowContent: beforeStepContent,
        usage: {
          inputTokens: estimatedInputTokens,
          outputTokens: 0,
          totalTokens: estimatedInputTokens,
        },
      };
      this.addStep(userStep);
      this.head = userStepId;

      let isFirstStep = true;
      let lastPreparedMessages: ModelMessage[] | undefined;

      // Pass only the current user turn; prepareStep rebuilds full context each step
      // (historical interactions + DAG + this message).
      const currentUserMessage: ModelMessage[] = [{ role: "user", content: userMessage }];
      const result = await generateText({
        model: this.model,
        system: this.systemPrompt,
        messages: currentUserMessage,
        tools: this.tools,
        temperature: 0.2,
        stopWhen: (ctx: any) => stepCountIs(this.settings.maxSteps)(ctx) || this.shouldStopGeneration,
        prepareStep: async ({ stepNumber, messages: currentMessages }) => {
          let compilationResult: WorkflowCompilationResponse | null = null;
          if (this.workflowState.getAllOperators().length > 0) {
            try {
              const logicalPlan = this.workflowState.toLogicalPlan();
              compilationResult = await compileWorkflowAsync(logicalPlan);
            } catch (e: any) {
              this.log.warn({ err: e?.message || e }, "compilation failed; proceeding without schemas");
            }
          }

          const visibleSteps = this.getVisibleReActSteps();
          const processed = assembleContext(
            visibleSteps,
            this.workflowState,
            this.getFormattedResultsForDAG(),
            false,
            compilationResult
          );
          lastPreparedMessages = processed;

          // After navigation fires: force text-only response, no more tool calls.
          if (this.navigationFiredThisTurn) {
            const stopMessages: ModelMessage[] = [
              ...processed,
              {
                role: "user",
                content:
                  "STOP CALLING TOOLS. Navigation is complete — the user is already being taken to the workflow. " +
                  "Do NOT call navigateToWorkflow or any other tool again. " +
                  "Respond with a single short text message summarising what was done.",
              },
            ];
            return { messages: stopMessages, toolChoice: "none" as const };
          }

          // Every 5 steps, inject a progress-check prompt so the LLM narrates
          // what it has done and what it is about to do next.
          if (stepNumber > 0 && stepNumber % 5 === 0) {
            const checkIn: ModelMessage = {
              role: "user",
              content:
                "Progress check: in 1–2 sentences describe what you have built so far " +
                "and what your very next action will be. Then immediately continue working.",
            };
            return { messages: [...processed, checkIn] };
          }

          return { messages: processed };
        },
        abortSignal: this.abortController?.signal,
        // reasoning_effort is configured per-model in litellm-config.yaml via extra_body
        // to bypass LiteLLM's param validation — do not pass it here.
        providerOptions: {
          openai: { parallelToolCalls: false },
          anthropic: { disableParallelToolUse: true },
          mistral: { parallelToolCalls: false },
        },
        onStepFinish: async ({ text, toolCalls, toolResults, usage }) => {
          stepIndex++;

          const formattedToolCalls = toolCalls?.map(tc => ({
            toolName: tc.toolName,
            toolCallId: tc.toolCallId,
            input: tc.input,
          }));

          // Log tool calls so issues are visible in the agent log.
          if (formattedToolCalls?.length) {
            for (const tc of formattedToolCalls) {
              const result = toolResults?.find(r => r.toolCallId === tc.toolCallId);
              const isError = !!(result?.output as any)?.error;
              this.log.info(
                { toolName: tc.toolName, operatorType: (tc.input as any)?.operatorType, isError },
                isError ? "tool call failed" : "tool call succeeded"
              );
            }
          }

          const formattedToolResults = toolResults?.map(tr => ({
            toolCallId: tr.toolCallId,
            output: tr.output,
            isError: !!(tr.output as any)?.error,
          }));

          const afterStepContent = this.workflowState.getWorkflowContent();

          const agentStepId = this.generateStepId();
          const agentStep: ReActStep = {
            id: agentStepId,
            parentId: this.head,
            messageId,
            stepId: stepIndex,
            timestamp: Date.now(),
            role: "agent",
            // Surface results from tools that the LLM should present but often loops on instead.
            content: (() => {
              // Navigation tools — append their summary/message
              const navCall = toolCalls?.find(
                tc => tc.toolName === "navigateToWorkflow" || tc.toolName === "navigate"
              );
              if (navCall) {
                const extra: string =
                  (navCall.input as any)?.summary ?? (navCall.input as any)?.message ?? "";
                if (extra) return text ? `${text}\n\n${extra}` : extra;
              }

              // List tools — append the actual list so the LLM doesn't need to call again
              const listCall = toolCalls?.find(
                tc => tc.toolName === "listWorkflows" || tc.toolName === "listDatasets"
              );
              if (listCall && toolResults) {
                const idx = (toolCalls ?? []).indexOf(listCall);
                const listOut = String(toolResults[idx]?.output ?? "");
                const isUseful =
                  listOut.length > 0 &&
                  !listOut.startsWith("[ERROR]") &&
                  !listOut.includes("Stop repeating") &&
                  !listOut.includes("already retrieved");
                if (isUseful) return text ? `${text}\n\n${listOut}` : listOut;
              }

              return text || "";
            })(),
            isBegin: isFirstStep,
            isEnd: false,
            toolCalls: formattedToolCalls,
            toolResults: formattedToolResults,
            usage: usage
              ? {
                  inputTokens: usage.inputTokens,
                  outputTokens: usage.outputTokens,
                  totalTokens: usage.totalTokens,
                }
              : undefined,
            inputMessages: lastPreparedMessages,
            beforeWorkflowContent: beforeStepContent,
            afterWorkflowContent: afterStepContent,
          };
          lastPreparedMessages = undefined;
          this.addStep(agentStep);
          this.head = agentStepId;

          const execConfig = this.buildExecutionConfig();
          if (execConfig && toolCalls && toolResults) {
            const EXECUTE_AFTER_TOOLS = new Set([TOOL_NAME_ADD_OPERATOR, TOOL_NAME_MODIFY_OPERATOR]);

            for (let i = 0; i < toolCalls.length; i++) {
              const tc = toolCalls[i];
              const tr = toolResults[i];
              if (!EXECUTE_AFTER_TOOLS.has(tc.toolName)) continue;

              const resultText = typeof tr?.output === "string" ? tr.output : String(tr?.output ?? "");
              if (resultText.startsWith("[ERROR]")) continue;

              const operatorId = (tc.input as any)?.operatorId;
              if (!operatorId) continue;

              try {
                await executeOperatorAndFormat(this.workflowState, execConfig, operatorId, {
                  abortSignal: this.abortController?.signal,
                  onResult: (opId, operatorInfo) => {
                    this.workflowResultState.set(opId, this.head, operatorInfo);
                  },
                });
              } catch (e: any) {
                this.log.warn({ operatorId, err: e?.message || e }, "post-step execution failed");
              }
            }
          }

          beforeStepContent = afterStepContent;
          isFirstStep = false;

          // Abort if navigateToWorkflow fired (terminal action).
          const navigated = toolCalls?.some(tc => tc.toolName === "navigateToWorkflow");
          if (navigated) {
            this.abortController?.abort();
          }

          // Abort if any loop detector fired this step (addOperator or modifyOperator repetition).
          if (!navigated && toolResults) {
            const loopDetected = toolResults.some(
              tr => typeof tr.output === "string" && tr.output.includes("Generation is stopping")
            );
            if (loopDetected) {
              this.abortController?.abort();
            }
          }
        },
      });

      const msgSteps = this.reActStepsByMessageId.get(messageId);
      if (msgSteps && msgSteps.length > 0) {
        const lastStep = msgSteps[msgSteps.length - 1];
        if (lastStep.role === "agent") {
          lastStep.isEnd = true;

          // If the last step has no text content, the agent was cut off mid-task
          // (hit maxSteps or a loop guard). Append an explanation.
          if (!lastStep.content && lastStep.toolCalls?.length) {
            // Check if the last tool result has a meaningful message to surface
            const lastResult = lastStep.toolResults?.[lastStep.toolResults.length - 1];
            const lastOut = typeof lastResult?.output === "string" ? lastResult.output : "";

            if (lastOut.includes("Stop repeating") || lastOut.includes("Already searched") || lastOut.includes("Already navigated")) {
              // Loop detector fired — extract the helpful part of the message
              lastStep.content = lastOut.replace(/Generation is stopping\.\s*/g, "").replace(/\[ERROR\]\s*/g, "").trim();
            } else {
              const ops = this.workflowState.getAllOperators();
              if (ops.length > 0) {
                const opSummary = ops.map(o => `${o.operatorID} (${o.operatorType})`).join(", ");
                lastStep.content =
                  `⚠️ I reached the step limit before finishing. ` +
                  `Here's what I built so far: **${opSummary}**. ` +
                  `You can ask me to continue, simplify the request, or describe what to fix.`;
              } else {
                lastStep.content =
                  `⚠️ I ran out of steps before completing your request. ` +
                  `Please rephrase or break it into smaller steps.`;
              }
            }
          }
        }
      }

      const finalUsage = (result as any).totalUsage || result.usage;
      const usage: TokenUsage = {
        inputTokens: finalUsage?.inputTokens ?? finalUsage?.promptTokens ?? 0,
        outputTokens: finalUsage?.outputTokens ?? finalUsage?.completionTokens ?? 0,
        totalTokens: finalUsage?.totalTokens ?? 0,
      };

      return {
        response: result.text,
        messages: result.response.messages,
        usage,
        stopped: false,
      };
    } catch (error: any) {
      const isAborted = error.name === "AbortError" || this.abortController?.signal.aborted;

      if (isAborted) {
        stepIndex++;
        const stoppedStepId = this.generateStepId();
        const stoppedStep: ReActStep = {
          id: stoppedStepId,
          parentId: this.head,
          messageId,
          stepId: stepIndex,
          timestamp: Date.now(),
          role: "agent",
          content: "Generation stopped by user.",
          isBegin: false,
          isEnd: true,
        };
        this.addStep(stoppedStep);
        this.head = stoppedStepId;

        return {
          response: "",
          messages: [],
          usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
          stopped: true,
        };
      }

      stepIndex++;
      const errorStepId = this.generateStepId();
      const errorStep: ReActStep = {
        id: errorStepId,
        parentId: this.head,
        messageId,
        stepId: stepIndex,
        timestamp: Date.now(),
        role: "agent",
        content: `Error: ${error.message || String(error)}`,
        isBegin: false,
        isEnd: true,
      };
      this.addStep(errorStep);
      this.head = errorStepId;

      return {
        response: "",
        messages: [],
        usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        stopped: false,
        error: error.message || String(error),
      };
    } finally {
      this.abortController = null;
      this.currentMessageId = undefined;
      this.state = AgentStateEnum.AVAILABLE;
    }
  }

  private getFormattedResultsForDAG(): Map<string, string> {
    const result = new Map<string, string>();
    const visible = this.workflowResultState.getAllVisible();
    for (const [operatorId, entry] of visible) {
      result.set(operatorId, formatOperatorResult(operatorId, entry.operatorInfo, this.workflowState));
    }
    return result;
  }

  stop(): void {
    this.state = AgentStateEnum.STOPPING;
    if (this.abortController) {
      this.abortController.abort();
    }
  }

  clearHistory(): void {
    this.reActStepsByMessageId.clear();
    this.stepsById.clear();
    this.currentMessageId = undefined;
    this.head = INITIAL_STEP_ID;
    const initialStep: ReActStep = {
      id: INITIAL_STEP_ID,
      messageId: "initial",
      stepId: -1,
      timestamp: Date.now(),
      role: "user",
      content: "",
      isBegin: true,
      isEnd: true,
    };
    this.stepsById.set(INITIAL_STEP_ID, initialStep);
  }

  private getOperatorIdsFromStep(step: ReActStep): { added: string[]; modified: string[] } {
    const added: string[] = [];
    const modified: string[] = [];

    if (!step.toolResults) {
      return { added, modified };
    }

    for (const result of step.toolResults) {
      if (result.isError || !result.output) continue;

      const toolCall = step.toolCalls?.find(tc => tc.toolCallId === result.toolCallId);
      const toolName = toolCall?.toolName || "";

      const outputStr = typeof result.output === "string" ? result.output : JSON.stringify(result.output);

      const addedMatch = outputStr.match(/Added operator ([a-zA-Z0-9_-]+)/);
      if (addedMatch && (toolName === "addOperator" || toolName.toLowerCase().includes("add"))) {
        added.push(addedMatch[1]);
        continue;
      }

      const modifiedMatch = outputStr.match(/Operator ([a-zA-Z0-9_-]+) modified/);
      if (modifiedMatch && (toolName === "modifyOperator" || toolName.toLowerCase().includes("modify"))) {
        modified.push(modifiedMatch[1]);
        continue;
      }

      try {
        const output = JSON.parse(outputStr);
        if (output.operatorId) {
          if (toolName === "addOperator" || toolName === "addCodeOperator") {
            added.push(output.operatorId);
          } else if (toolName === "modifyOperator" || toolName === "modifyCodeOperator") {
            modified.push(output.operatorId);
          }
        }
      } catch {}
    }

    return { added, modified };
  }

  public getReActStepsByOperatorIds(operatorIds: string[]): ReActStep[] {
    const allSteps = this.getReActSteps();
    if (!operatorIds || operatorIds.length === 0) {
      return allSteps;
    }

    const operatorIdSet = new Set(operatorIds);
    const relevantSteps: ReActStep[] = [];

    for (const step of allSteps) {
      const { added, modified } = this.getOperatorIdsFromStep(step);

      const affectsOperator = [...added, ...modified].some(id => operatorIdSet.has(id));

      if (affectsOperator) {
        relevantSteps.push(step);
      }
    }

    return relevantSteps;
  }

  destroy(): void {
    if (this.workflowChangeSubscription) {
      this.workflowChangeSubscription.unsubscribe();
      this.workflowChangeSubscription = null;
    }

    this.workflowState.destroy();

    this.websockets.clear();

    this.reActStepsByMessageId.clear();
    this.stepsById.clear();
    this.currentMessageId = undefined;
  }
}
