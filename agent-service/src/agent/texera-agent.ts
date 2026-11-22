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

import { ToolLoopAgent, type ModelMessage, type LanguageModel } from "ai";
import { Subscription } from "rxjs";
import { debounceTime } from "rxjs/operators";
import { WorkflowState } from "./workflow-state";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";
import { WorkflowResultState } from "./workflow-result-state";
import type { AgentSettings, ReActStep, TokenUsage, UserInfo } from "../types/agent";
import type { OperatorExecutionSummary } from "../types/execution";
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
import {
  createGetOperatorResultTool,
  createGetWorkflowTool,
  TOOL_NAME_GET_OPERATOR_RESULT,
  TOOL_NAME_GET_WORKFLOW,
  type WorkflowReadContext,
} from "./tools/workflow-read-tools";
import { unexecutedToolCallsMessage } from "./util/context-utils";
import { compileWorkflowAsync } from "../api/compile-api";
import { createLogger } from "../logger";
import type { Logger } from "pino";
import { isErrorResult } from "./tools/tools-utility";

const PERSIST_DEBOUNCE_MS = 500;

/** The tools `settings.disabledTools` cannot switch off. */
const ALWAYS_ENABLED_TOOLS: ReadonlySet<string> = new Set([TOOL_NAME_GET_WORKFLOW, TOOL_NAME_GET_OPERATOR_RESULT]);

export interface TexeraAgentConfig {
  model: LanguageModel;
  modelType: string;
  agentId: string;
  agentName?: string;
  systemPrompt?: string;
  /** JWT of the user the agent acts for, sent on backend calls that are not tied to a workflow. */
  userToken?: string;
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
 * What the agent says when the step limit ends a turn. Such a turn stops on a tool call, with no
 * closing answer from the model, so without this the chat would only go quiet.
 */
function stepLimitNotice(maxSteps: number): string {
  const limit = maxSteps === 1 ? "1 step" : `${maxSteps} steps`;
  return (
    `I stopped because I reached the limit of ${limit} for one message, so the task may be unfinished. ` +
    "Send another message to continue, or raise the limit in the agent's settings."
  );
}

/**
 * A single Texera agent instance.
 *
 * Owns the conversation (ReAct step tree with HEAD tracking, each step keeping
 * the model messages it added), the workflow being edited (`WorkflowState`),
 * cached operator execution results (`WorkflowResultState`), and the tool
 * surface exposed to the LLM. Each call to `sendMessage` drives one multi-step
 * run of the AI SDK's `ToolLoopAgent` over those messages, streaming step
 * updates to subscribed clients.
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

  private clients: Set<any> = new Set();

  private model: LanguageModel;
  private readonly userToken?: string;
  private systemPrompt: string;
  private settings: AgentSettings;

  private reActStepsByMessageId: Map<string, ReActStep[]> = new Map();

  /**
   * The model messages each step added, by step id: a user step's message, an agent step's
   * assistant message and tool results, or the note a stopped or failed turn ends with.
   */
  private messagesByStep: Map<string, ModelMessage[]> = new Map();

  /**
   * The id the agent step now being generated will take. A step is recorded when it ends, but an
   * add or modify call executes its operator before that, and the result is filed under this step.
   */
  private stepInProgress: string | undefined = undefined;

  private currentMessageId: string | undefined = undefined;

  private delegateConfig?: {
    userToken: string;
    userInfo?: UserInfo;
    workflowId: number;
    workflowName?: string;
    computingUnitId?: number;
    warehouseId?: number;
  };

  private stepCallback: ReActStepCallback | null = null;

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
    this.userToken = config.userToken;
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
      workflowId: this.delegateConfig.workflowId,
      computingUnitId: this.delegateConfig.computingUnitId,
      warehouseId: this.delegateConfig.warehouseId,
      maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: this.settings.maxOperatorResultCellCharLimit,
      executionTimeoutMs: this.settings.executionTimeoutMs,
    };
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

    const getExecutionConfig = this.delegateConfig ? () => this.buildExecutionConfig()! : undefined;

    const context: ToolContext = {
      metadataStore: this.metadataStore,
      settings: {
        maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
        toolTimeoutMs: this.settings.toolTimeoutMs,
        executionTimeoutMs: this.settings.executionTimeoutMs,
      },
      // An add or modify call changes the workflow, so the result of executing the changed operator
      // holds from the step that made the call on, not from the head that step started at.
      executeOperator: getExecutionConfig
        ? (operatorId, options) =>
            executeOperatorAndFormat(this.workflowState, getExecutionConfig(), operatorId, {
              ...options,
              onResult: (opId, operatorInfo) => {
                this.recordResult(opId, this.stepInProgress ?? this.head, operatorInfo);
              },
            })
        : undefined,
    };

    // Nothing writes the workflow into the conversation: the model sees it whole only through
    // getWorkflow, and reads a stored result again through getOperatorResult.
    const readContext: WorkflowReadContext = {
      workflowState: this.workflowState,
      compileWorkflow: async () =>
        await compileWorkflowAsync(
          this.workflowState.toLogicalPlan(),
          this.delegateConfig?.userToken ?? this.userToken
        ),
      getResult: operatorId => {
        const entry = this.workflowResultState.get(operatorId);
        if (!entry) return undefined;
        const outdated =
          entry.fingerprint !== undefined && entry.fingerprint !== this.workflowState.executionFingerprint(operatorId);
        return { summary: entry.operatorInfo, outdated };
      },
      getResultCharLimit: () => this.settings.maxOperatorResultCharLimit,
    };

    const tools: Record<string, any> = {
      [TOOL_NAME_GET_WORKFLOW]: createGetWorkflowTool(readContext),
      [TOOL_NAME_GET_OPERATOR_RESULT]: createGetOperatorResultTool(readContext),
      [TOOL_NAME_DELETE_OPERATOR]: createDeleteOperatorTool(this.workflowState, context),
      [TOOL_NAME_ADD_OPERATOR]: createAddOperatorTool(this.workflowState, operatorSchemas, context),
      [TOOL_NAME_MODIFY_OPERATOR]: createModifyOperatorTool(this.workflowState, context),
    };

    if (getExecutionConfig) {
      tools[TOOL_NAME_EXECUTE_OPERATOR] = createExecuteOperatorTool(
        this.workflowState,
        getExecutionConfig,
        (opId, operatorInfo) => {
          this.recordResult(opId, this.head, operatorInfo);
        }
      );
    }

    return tools;
  }

  /** Files an execution's result with what it depended on, so a later change marks it outdated. */
  private recordResult(operatorId: string, stepId: string, operatorInfo: OperatorExecutionSummary): void {
    this.workflowResultState.set(operatorId, stepId, operatorInfo, this.workflowState.executionFingerprint(operatorId));
  }

  /**
   * Whether the model may call a tool: every tool except those in `settings.disabledTools`. The two
   * read tools cannot be switched off, since nothing else shows the model the workflow or a result.
   */
  private isToolEnabled(name: string): boolean {
    return ALWAYS_ENABLED_TOOLS.has(name) || !this.settings.disabledTools.has(name);
  }

  private getEnabledTools(): Record<string, any> {
    return Object.fromEntries(Object.entries(this.tools).filter(([name]) => this.isToolEnabled(name)));
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

  getWorkflowResultState(): WorkflowResultState {
    return this.workflowResultState;
  }

  getClients(): Set<any> {
    return this.clients;
  }

  addClient(ws: any): void {
    this.clients.add(ws);
  }

  removeClient(ws: any): void {
    this.clients.delete(ws);
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

  setStepCallback(callback: ReActStepCallback | null): void {
    this.stepCallback = callback;
  }

  private generateStepId(): string {
    return `step-${this.agentId}-${++this.stepCounter}-${Date.now()}`;
  }

  /** Records a step and the model messages it adds to the conversation. */
  private addStep(step: ReActStep, messages: ModelMessage[] = []): void {
    let steps = this.reActStepsByMessageId.get(step.messageId);
    if (!steps) {
      steps = [];
      this.reActStepsByMessageId.set(step.messageId, steps);
    }
    steps.push(step);
    this.stepsById.set(step.id, step);
    this.messagesByStep.set(step.id, messages);
    if (this.stepCallback) {
      this.stepCallback(step);
    }
  }

  /**
   * The conversation as the model reads it: the messages of the steps on the path from the root to
   * HEAD, in order. A branch HEAD has left is not on that path, so the model never sees it.
   */
  private getConversation(): ModelMessage[] {
    return this.getVisibleReActSteps().flatMap(step => this.messagesByStep.get(step.id) ?? []);
  }

  getSystemInfo(): {
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
  } {
    const toolsInfo = Object.entries(this.tools).map(([name, toolDef]) => {
      const description = toolDef.description || "";
      const inputSchema = toolDef.parameters || {};
      const enabled = this.isToolEnabled(name);

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
      this.settings.maxSteps = Math.max(1, Math.trunc(updates.maxSteps));
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
    workflowId: number;
    workflowName?: string;
    computingUnitId?: number;
    warehouseId?: number;
  }): void {
    this.delegateConfig = config;

    this.tools = this.createTools();

    this.setupWorkflowChangeHandlers();
  }

  /**
   * Point the delegate at the warehouse the workspace has selected now. The rest
   * of the config is fixed at creation; this one travels per prompt because the
   * user can pick (or first load) a warehouse after the agent exists (#7751).
   */
  setDelegateWarehouse(warehouseId: number | undefined): void {
    if (!this.delegateConfig || this.delegateConfig.warehouseId === warehouseId) {
      return;
    }
    this.delegateConfig = { ...this.delegateConfig, warehouseId };
  }

  getDelegateConfig():
    | {
        userToken: string;
        userInfo?: UserInfo;
        workflowId: number;
        workflowName?: string;
        computingUnitId?: number;
        warehouseId?: number;
      }
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

  async sendMessage(userMessage: string, messageSource?: "chat" | "feedback"): Promise<AgentMessageResult> {
    const messageId = `msg-${this.agentId}-${++this.messageCounter}-${Date.now()}`;
    let stepIndex = 0;
    // Ends the turn with a step that says how it ended: at the step limit, stopped, or failed.
    const addClosingStep = (content: string): void => {
      stepIndex++;
      const step: ReActStep = {
        id: this.generateStepId(),
        parentId: this.head,
        messageId,
        stepId: stepIndex,
        timestamp: Date.now(),
        role: "agent",
        content,
        isBegin: false,
        isEnd: true,
      };
      // The next turn reads how this one ended, as the chat shows it.
      this.addStep(step, [{ role: "assistant", content }]);
      this.head = step.id;
    };

    await this.refreshWorkflowFromBackend();

    this.abortController = new AbortController();

    this.state = AgentStateEnum.GENERATING;

    this.currentMessageId = messageId;

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
        beforeWorkflowContent: beforeStepContent,
        afterWorkflowContent: beforeStepContent,
        usage: {
          inputTokens: estimatedInputTokens,
          outputTokens: 0,
          totalTokens: estimatedInputTokens,
        },
      };
      this.addStep(userStep, [{ role: "user", content: userMessage }]);
      this.head = userStepId;

      // The limit for this turn: one saved while the turn is in flight applies from the next
      // message on.
      const maxSteps = this.settings.maxSteps;
      // Whether the limit is what ended the turn. The AI SDK asks `stopWhen` only after a step
      // whose tool calls were executed, when the loop would otherwise go on, so its answer says
      // so exactly. The last step's finish reason does not: it is the provider's word, and the
      // SDK goes on after a tool call that a provider reported as a plain "stop".
      let stoppedAtStepLimit = false;

      let isFirstStep = true;
      let lastPreparedMessages: ModelMessage[] | undefined;

      const loop = new ToolLoopAgent({
        model: this.model,
        instructions: this.systemPrompt,
        tools: this.getEnabledTools(),
        temperature: 0.2,
        stopWhen: ({ steps }) => {
          stoppedAtStepLimit = steps.length >= maxSteps;
          return stoppedAtStepLimit;
        },
        // Each step reads the SDK's own messages unchanged: the previous step's input and response.
        // So every model call begins with the previous call's input, and a provider's prompt cache
        // can reuse it. The model reads the workflow and its results through the read tools.
        prepareStep: async ({ messages }) => {
          this.stepInProgress = this.generateStepId();
          lastPreparedMessages = messages;
          return undefined;
        },
        // reasoning_effort is configured per-model in litellm-config.yaml via extra_body
        // to bypass LiteLLM's param validation — do not pass it here.
        providerOptions: {
          openai: { parallelToolCalls: false },
          anthropic: { disableParallelToolUse: true },
          mistral: { parallelToolCalls: false },
        },
      });

      const result = await loop.generate({
        messages: this.getConversation(),
        abortSignal: this.abortController?.signal,
        onStepEnd: async ({ text, toolCalls, content, usage, response, finishReason }) => {
          stepIndex++;

          const formattedToolCalls = toolCalls?.map(tc => ({
            toolName: tc.toolName,
            toolCallId: tc.toolCallId,
            input: tc.input,
          }));

          const formattedToolResults = content
            .filter(part => part.type === "tool-result" || part.type === "tool-error")
            .map(part => ({
              toolCallId: part.toolCallId,
              output:
                part.type === "tool-error"
                  ? part.error instanceof Error
                    ? part.error.message
                    : String(part.error)
                  : part.output,
              isError: part.type === "tool-error" || isErrorResult(part.output),
            }));
          const toolResultsByCallId = new Map(formattedToolResults.map(result => [result.toolCallId, result]));

          const afterStepContent = this.workflowState.getWorkflowContent();

          const agentStepId = this.stepInProgress ?? this.generateStepId();
          this.stepInProgress = undefined;
          const agentStep: ReActStep = {
            id: agentStepId,
            parentId: this.head,
            messageId,
            stepId: stepIndex,
            timestamp: Date.now(),
            role: "agent",
            content: text || "",
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
          const unexecutedCalls = unexecutedToolCallsMessage(toolCalls, toolResultsByCallId, finishReason);
          this.addStep(agentStep, unexecutedCalls ? [...response.messages, unexecutedCalls] : response.messages);
          this.head = agentStepId;

          beforeStepContent = afterStepContent;
          isFirstStep = false;
        },
      });

      // A stop that lands on the last allowed step lets `loop.generate` return normally, since the
      // stop condition ends the loop before another model call can see the abort. Such a turn ends
      // like a stop on an earlier step, in the catch below.
      this.abortController?.signal.throwIfAborted();

      // A turn the limit cut short has no closing answer, so the agent says why it stopped.
      if (stoppedAtStepLimit) addClosingStep(stepLimitNotice(maxSteps));

      const msgSteps = this.reActStepsByMessageId.get(messageId);
      if (msgSteps && msgSteps.length > 0) {
        const lastStep = msgSteps[msgSteps.length - 1];
        if (lastStep.role === "agent") {
          lastStep.isEnd = true;
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
      const errorMessage = error?.message || String(error);
      const isAborted = error?.name === "AbortError" || this.abortController?.signal.aborted;

      if (isAborted) {
        addClosingStep("Generation stopped by user.");
        return {
          response: "",
          messages: [],
          usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
          stopped: true,
        };
      }

      addClosingStep(`Error: ${errorMessage}`);
      return {
        response: "",
        messages: [],
        usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        stopped: false,
        error: errorMessage,
      };
    } finally {
      this.abortController = null;
      this.stepInProgress = undefined;
      this.currentMessageId = undefined;
      this.state = AgentStateEnum.AVAILABLE;
    }
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
    this.messagesByStep.clear();
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

    this.clients.clear();

    this.reActStepsByMessageId.clear();
    this.stepsById.clear();
    this.messagesByStep.clear();
    this.currentMessageId = undefined;
  }
}
