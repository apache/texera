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
 * Texera Agent - Core agent implementation using Vercel AI SDK.
 */

import { generateText, type ModelMessage, type LanguageModel, stepCountIs } from "ai";
import { Subscription } from "rxjs";
import { debounceTime } from "rxjs/operators";
import { WorkflowState } from "./workflow-state";
import { OperatorMetadataStore } from "./tools/workflow-metadata-tools";
import { OperatorResultStore } from "./operator-result-store";
import { formatOperatorResult, type FormatOptions } from "./tools/result-formatting";
import type {
  AgentSettings,
  ReActStep,
  TokenUsage,
  UserInfo,
} from "../types/agent";
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

// ============================================================================
// Constants
// ============================================================================

/** Debounce interval for auto-persistence (ms) */
const PERSIST_DEBOUNCE_MS = 500;

// ============================================================================
// Agent Configuration
// ============================================================================

export interface TexeraAgentConfig {
  /** Language model to use */
  model: LanguageModel;
  /** Model type identifier (e.g., "gpt-4-turbo") */
  modelType: string;
  /** Agent ID */
  agentId: string;
  /** Agent display name */
  agentName?: string;
  /** Custom system prompt (optional; a general-mode prompt is rebuilt once metadata loads) */
  systemPrompt?: string;
}

// ============================================================================
// Agent Result Types
// ============================================================================

export interface AgentMessageResult {
  /** Final response text */
  response: string;
  /** Full conversation messages from this interaction */
  messages: ModelMessage[];
  /** Token usage statistics */
  usage: TokenUsage;
  /** Whether the agent was stopped early */
  stopped: boolean;
  /** Error message if any */
  error?: string;
}

// ============================================================================
// Texera Agent Class
// ============================================================================

/** Callback for receiving ReActStep updates */
type ReActStepCallback = (step: ReActStep) => void;

/**
 * TexeraAgent is the core agent implementation.
 * It maintains workflow state and processes user messages using the Vercel AI SDK.
 */
export class TexeraAgent {
  readonly agentId: string;
  readonly agentName: string;
  readonly modelType: string;
  readonly createdAt: Date;

  // State
  private state: AgentStateEnum = AgentStateEnum.AVAILABLE;
  private workflowState: WorkflowState;
  // Uses global singleton - initialized once at server startup
  private metadataStore: OperatorMetadataStore;
  // Step-level versioning: HEAD pointer and step map
  private head: string = INITIAL_STEP_ID;
  private stepsById: Map<string, ReActStep> = new Map();
  private stepCounter = 0;
  // Versioned operator result store
  private operatorResultStore: OperatorResultStore;

  // Server-managed state (for HTTP/WebSocket handling)
  /** Active WebSocket connections for this agent */
  private websockets: Set<any> = new Set();

  // Configuration
  private model: LanguageModel;
  private systemPrompt: string;
  private settings: AgentSettings;

  // ReActSteps grouped by messageId
  private reActStepsByMessageId: Map<string, ReActStep[]> = new Map();

  // Current messageId during an ongoing generateText call; undefined otherwise
  private currentMessageId: string | undefined = undefined;

  // Delegate configuration for backend operations
  private delegateConfig?: {
    userToken: string;
    userInfo?: UserInfo;
    workflowId: number;
    workflowName?: string;
    computingUnitId?: number;
  };

  // Callback for streaming ReActSteps
  private stepCallback: ReActStepCallback | null = null;

  // Message counter for generating unique IDs
  private messageCounter = 0;

  // Tools
  private tools: Record<string, any>;

  // Abort controller for stopping the agent
  private abortController: AbortController | null = null;

  // RxJS subscriptions for workflow change handling (persistence + compilation)
  private workflowChangeSubscription: Subscription | null = null;

  constructor(config: TexeraAgentConfig) {
    this.agentId = config.agentId;
    this.agentName = config.agentName || `Agent-${config.agentId}`;
    this.modelType = config.modelType;
    this.createdAt = new Date();
    this.model = config.model;
    this.systemPrompt = config.systemPrompt || "";

    // Initialize state
    this.workflowState = new WorkflowState();
    // Always use global singleton metadata store
    this.metadataStore = OperatorMetadataStore.getInstance();
    // Initialize versioned operator result store (uses ancestor path from step tree)
    this.operatorResultStore = new OperatorResultStore(() => this.getAncestorPath());

    // Create and register the initial step (root of the step tree)
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

    // Initialize settings with defaults
    this.settings = {
      ...DEFAULT_AGENT_SETTINGS,
      systemPrompt: this.systemPrompt,
    };

    // Initialize tools - will have operator schemas if metadata store is already initialized
    this.tools = this.createTools();
  }

  /**
   * Initialize the agent by loading operator metadata from the backend.
   * If the metadata store is already initialized (e.g., global singleton),
   * this just rebuilds the tools and system prompt with the existing metadata.
   */
  async initialize(): Promise<void> {
    try {
      // Only fetch from backend if not already initialized
      if (!this.metadataStore.isInitialized()) {
        await this.metadataStore.initializeFromBackend();
      }

      // Rebuild system prompt with loaded operator metadata
      this.rebuildSystemPrompt();

      // Rebuild tools with loaded metadata
      this.tools = this.createTools();
      console.log(
        `[TexeraAgent ${this.agentId}] Initialized with ${this.metadataStore.getOperatorCount()} operators`
      );
    } catch (error) {
      console.error(`[TexeraAgent ${this.agentId}] Failed to initialize metadata:`, error);
      // Continue with empty metadata - tools will still work but addOperator will fail
    }
  }

  /**
   * Rebuild system prompt (single general-mode template with operator schemas).
   */
  private rebuildSystemPrompt(): void {
    this.systemPrompt = buildSystemPrompt(this.metadataStore, this.settings.allowedOperatorTypes);
    this.settings.systemPrompt = this.systemPrompt;
  }

  // ============================================================================
  // Execution Config
  // ============================================================================

  /**
   * Build execution config from current delegate config and settings.
   * Returns undefined if no delegate config (execution not available).
   */
  private buildExecutionConfig(): ExecutionConfig | undefined {
    if (!this.delegateConfig) return undefined;
    return {
      userToken: this.delegateConfig.userToken,
      workflowId: this.delegateConfig.workflowId,
      computingUnitId: this.delegateConfig.computingUnitId,
      maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
      maxOperatorResultCellCharLimit: this.settings.maxOperatorResultCellCharLimit,
      executionTimeoutMs: this.settings.executionTimeoutMs,
    };
  }

  // ============================================================================
  // Tool Creation
  // ============================================================================

  private createTools(): Record<string, any> {
    // Get operator schemas map for addOperator tool
    // Each entry needs both jsonSchema and additionalMetadata for port info
    const operatorSchemas = new Map<string, any>();
    for (const type of Object.keys(this.metadataStore.getAllOperatorTypes())) {
      const jsonSchema = this.metadataStore.getSchema(type);
      const additionalMetadata = this.metadataStore.getAdditionalMetadata(type);
      if (jsonSchema) {
        operatorSchemas.set(type, { jsonSchema, additionalMetadata });
      }
    }

    const getExecutionConfig = this.delegateConfig
      ? () => this.buildExecutionConfig()!
      : undefined;

    // Build tool context — execution is handled in post-step phase, not inside tools
    const context: ToolContext = {
      metadataStore: this.metadataStore,
      settings: {
        maxOperatorResultCharLimit: this.settings.maxOperatorResultCharLimit,
        toolTimeoutMs: this.settings.toolTimeoutMs,
        executionTimeoutMs: this.settings.executionTimeoutMs,
      },
    };

    // General-mode tools: addOperator, modifyOperator, deleteOperator.
    // Links are created automatically via inputOperatorIds in addOperator.
    const tools: Record<string, any> = {
      [TOOL_NAME_DELETE_OPERATOR]: createDeleteOperatorTool(this.workflowState, context),
      [TOOL_NAME_ADD_OPERATOR]: createAddOperatorTool(this.workflowState, operatorSchemas, context),
      [TOOL_NAME_MODIFY_OPERATOR]: createModifyOperatorTool(this.workflowState, context),
    };

    // Add execution tools if delegateConfig is available (requires user token and workflow ID)
    if (getExecutionConfig) {
      tools[TOOL_NAME_EXECUTE_OPERATOR] = createExecuteOperatorTool(
        this.workflowState,
        getExecutionConfig,
        (opId, operatorInfo) => {
          this.operatorResultStore.set(opId, this.head, operatorInfo);
        }
      );
    }

    return tools;
  }

  // ============================================================================
  // State Access
  // ============================================================================

  getState(): AgentStateEnum {
    return this.state;
  }

  getWorkflowState(): WorkflowState {
    return this.workflowState;
  }

  getMetadataStore(): OperatorMetadataStore {
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

  getOperatorResultStore(): OperatorResultStore {
    return this.operatorResultStore;
  }

  // ============================================================================
  // WebSocket Management (for server use)
  // ============================================================================

  /**
   * Get all active WebSocket connections for this agent.
   */
  getWebsockets(): Set<any> {
    return this.websockets;
  }

  /**
   * Add a WebSocket connection to this agent.
   */
  addWebsocket(ws: any): void {
    this.websockets.add(ws);
  }

  /**
   * Remove a WebSocket connection from this agent.
   */
  removeWebsocket(ws: any): void {
    this.websockets.delete(ws);
  }

  /**
   * Get all ReActSteps across all messages (flat list, all branches).
   */
  getReActSteps(): ReActStep[] {
    const all: ReActStep[] = [];
    for (const steps of this.reActStepsByMessageId.values()) {
      all.push(...steps);
    }
    return all;
  }

  // ============================================================================
  // HEAD-based visibility
  // ============================================================================

  /**
   * Get ReActSteps visible from the current HEAD.
   *
   * Walks the ancestor path from root to HEAD and returns all steps on that path
   * (excluding the sentinel initial step).
   */
  getVisibleReActSteps(): ReActStep[] {
    const path = this.getAncestorPath();
    return path
      .filter(id => id !== INITIAL_STEP_ID)
      .map(id => this.stepsById.get(id)!)
      .filter(Boolean);
  }

  /**
   * Get ALL steps (including those not on the current HEAD path).
   * Used by the timeline UI to show full history even after checkout.
   */
  getAllSteps(): ReActStep[] {
    return Array.from(this.stepsById.values()).filter(s => s.id !== INITIAL_STEP_ID);
  }

  /**
   * Checkout to a specific step: move HEAD, restore workflow state.
   * Returns false if the step doesn't exist.
   */
  checkout(stepId: string): boolean {
    const step = this.stepsById.get(stepId);
    if (!step && stepId !== INITIAL_STEP_ID) return false;
    this.head = stepId;
    if (step?.afterWorkflowContent) {
      this.workflowState.setWorkflowContent(step.afterWorkflowContent);
    }
    return true;
  }

  /**
   * Set a callback to receive ReActStep updates in real-time.
   * @param callback - Function to call when a new step is added
   */
  setStepCallback(callback: ReActStepCallback | null): void {
    this.stepCallback = callback;
  }

  /**
   * Generate a unique step ID.
   */
  private generateStepId(): string {
    return `step-${this.agentId}-${++this.stepCounter}-${Date.now()}`;
  }

  /**
   * Add a ReActStep and notify the callback if set.
   */
  private addStep(step: ReActStep): void {
    // Store by message ID (existing)
    let steps = this.reActStepsByMessageId.get(step.messageId);
    if (!steps) {
      steps = [];
      this.reActStepsByMessageId.set(step.messageId, steps);
    }
    steps.push(step);
    // Store by step ID (new)
    this.stepsById.set(step.id, step);
    // Notify callback
    if (this.stepCallback) {
      this.stepCallback(step);
    }
  }

  /**
   * Get system info including system prompt and tool definitions.
   * This is used by the frontend to display agent configuration.
   */
  getSystemInfo(): {
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
  } {
    const toolsInfo = Object.entries(this.tools).map(([name, toolDef]) => {
      // Extract description and parameters from the tool definition
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

  /**
   * Get the current agent settings.
   */
  getSettings(): AgentSettings {
    return { ...this.settings };
  }

  /**
   * Update agent settings.
   * Only provided values will be updated.
   */
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

    // Rebuild tools with updated settings
    this.tools = this.createTools();
    console.log(
      `[TexeraAgent ${this.agentId}] Settings updated: ` +
        `maxOperatorResultCharLimit=${this.settings.maxOperatorResultCharLimit}, ` +
        `maxOperatorResultCellCharLimit=${this.settings.maxOperatorResultCellCharLimit}`
    );
  }

  // ============================================================================
  // Message Processing
  // ============================================================================

  /**
   * Load workflow from backend and refresh state.
   * Called before processing each message to ensure we have the latest workflow.
   */
  async refreshWorkflowFromBackend(): Promise<void> {
    // If HEAD points to a real step (not the initial dummy), the workflow is determined
    // by that step's snapshot. Only load from backend when HEAD is the initial state.
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
      console.log(`[TexeraAgent ${this.agentId}] Refreshed workflow ${this.delegateConfig.workflowId} from backend`);
    } catch (error) {
      console.warn(`[TexeraAgent ${this.agentId}] Failed to refresh workflow from backend:`, error);
    }
  }

  /**
   * Set the delegate configuration for backend operations.
   * This also rebuilds tools to include the workflow metadata in tool context,
   * and sets up workflow change handlers for persistence.
   */
  setDelegateConfig(config: {
    userToken: string;
    userInfo?: UserInfo;
    workflowId: number;
    workflowName?: string;
    computingUnitId?: number;
  }): void {
    this.delegateConfig = config;

    // Rebuild tools with updated workflow metadata in context and execution tools
    this.tools = this.createTools();

    // Setup workflow change handlers (persistence + compilation)
    this.setupWorkflowChangeHandlers();
  }

  /**
   * Get the delegate configuration.
   */
  getDelegateConfig():
    | { userToken: string; userInfo?: UserInfo; workflowId: number; workflowName?: string; computingUnitId?: number }
    | undefined {
    return this.delegateConfig;
  }

  /**
   * Setup RxJS-based workflow change handling.
   * Sets up auto-persistence with debounce.
   */
  private setupWorkflowChangeHandlers(): void {
    // Cleanup previous subscription if any
    if (this.workflowChangeSubscription) {
      this.workflowChangeSubscription.unsubscribe();
    }

    const subscription = new Subscription();
    const workflowChanged$ = this.workflowState.getWorkflowChangedStream();

    // Auto-persistence with debounce (only if in delegate mode)
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
          console.log(`[TexeraAgent ${this.agentId}] Auto-persisted workflow ${this.delegateConfig.workflowId}`);
        } catch (error) {
          console.error(`[TexeraAgent ${this.agentId}] Failed to auto-persist workflow:`, error);
        }
      });

      subscription.add(persistSubscription);
    }

    // Track the subscription
    this.workflowChangeSubscription = subscription;
    this.workflowState.addSubscription(subscription);
  }

  /**
   * Process a user message and return the agent's response.
   * ReActSteps are accumulated internally and streamed via the callback if set.
   * Before processing, loads the latest workflow from backend.
   *
   * @param userMessage - The user's message
   * @param contextOperatorIds - Optional operator IDs to filter context.
   *                             If provided, relevant ReActSteps are prepended as text context.
   */
  async sendMessage(userMessage: string, contextOperatorIds?: string[], messageSource?: "chat" | "feedback"): Promise<AgentMessageResult> {
    const messageId = `msg-${this.agentId}-${++this.messageCounter}-${Date.now()}`;
    let stepIndex = 0;

    // Load latest workflow from backend
    await this.refreshWorkflowFromBackend();

    // Build the actual message to send - prepend relevant context if operator IDs provided
    let actualUserMessage = userMessage;
    if (contextOperatorIds && contextOperatorIds.length > 0) {
      const contextText = this.buildContextText(contextOperatorIds);
      if (contextText) {
        actualUserMessage = `${contextText}\n\n${userMessage}`;
        console.log(
          `[TexeraAgent ${this.agentId}] Prepended context for operators [${contextOperatorIds.join(", ")}]`
        );
      }
    }

    // Create new abort controller for this message
    this.abortController = new AbortController();

    // Set state to generating
    this.state = AgentStateEnum.GENERATING;

    this.currentMessageId = messageId;

    try {
      // Capture workflow state before the user step
      let beforeStepContent = this.workflowState.getWorkflowContent();

      // Create user message step (stepId 0) with versioning fields
      const estimatedInputTokens = Math.ceil(actualUserMessage.length / 4);
      const userStepId = this.generateStepId();
      const userStep: ReActStep = {
        id: userStepId,
        parentId: this.head,
        messageId,
        stepId: 0,
        timestamp: Date.now(),
        role: "user",
        content: userMessage,
        actualContent: actualUserMessage !== userMessage ? actualUserMessage : undefined,
        isBegin: true,
        isEnd: true,
        messageSource,
        beforeWorkflowContent: beforeStepContent,
        afterWorkflowContent: beforeStepContent, // no change for user step
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

      // Pass only the current user message to generateText.
      // prepareStep will build the full context (historical interactions + DAG + this message).
      const currentUserMessage: ModelMessage[] = [{ role: "user", content: actualUserMessage }];
      const result = await generateText({
        model: this.model,
        system: this.systemPrompt,
        messages: currentUserMessage,
        tools: this.tools,
        temperature: 0.2,
        stopWhen: stepCountIs(this.settings.maxSteps),
        prepareStep: async ({ stepNumber, messages: currentMessages }) => {
              // Compile the workflow to get operator schemas and errors.
              // This gives the LLM column-level type information for each operator.
              let compilationResult: WorkflowCompilationResponse | null = null;
              if (this.workflowState.getAllOperators().length > 0) {
                try {
                  const logicalPlan = this.workflowState.toLogicalPlan();
                  compilationResult = await compileWorkflowAsync(logicalPlan);
                } catch (e: any) {
                  console.warn(`[TexeraAgent ${this.agentId}] Compilation failed, proceeding without schemas:`, e?.message || e);
                }
              }

              // Assemble context: completed tasks + ongoing task + current workflow DAG.
              const visibleSteps = this.getVisibleReActSteps();
              const processed = assembleContext(
                visibleSteps, this.workflowState, this.getFormattedResultsForDAG(), false, compilationResult
              );
              lastPreparedMessages = processed;
              return { messages: processed };
            },
        abortSignal: this.abortController?.signal,
        // Note: reasoning_effort is NOT passed here — it's configured per-model in
        // litellm-config.yaml via extra_body to bypass LiteLLM's param validation.
        providerOptions: {
          openai: { parallelToolCalls: false },
          anthropic: { disableParallelToolUse: true },
          mistral: { parallelToolCalls: false },
        },
        onStepFinish: async ({ text, toolCalls, toolResults, usage }) => {
          stepIndex++; // Increment first since user message is step 0

          // Build tool calls array
          const formattedToolCalls = toolCalls?.map(tc => ({
            toolName: tc.toolName,
            toolCallId: tc.toolCallId,
            input: tc.input,
          }));

          // Build tool results array - check for error field to determine if it's an error
          const formattedToolResults = toolResults?.map(tr => ({
            toolCallId: tr.toolCallId,
            output: tr.output,
            isError: !!(tr.output as any)?.error,
          }));

          // Capture workflow snapshot AFTER tool calls (but before post-step execution)
          const afterStepContent = this.workflowState.getWorkflowContent();

          // Create agent step with versioning fields and advance HEAD
          const agentStepId = this.generateStepId();
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
          this.addStep(agentStep);
          this.head = agentStepId;

          // Post-step auto-execution: execute operators that were successfully added/modified.
          // Results are stored at the new HEAD (this step's ID).
          const execConfig = this.buildExecutionConfig();
          if (execConfig && toolCalls && toolResults) {
            const EXECUTE_AFTER_TOOLS = new Set([
              TOOL_NAME_ADD_OPERATOR, TOOL_NAME_MODIFY_OPERATOR,
            ]);

            for (let i = 0; i < toolCalls.length; i++) {
              const tc = toolCalls[i];
              const tr = toolResults[i];
              if (!EXECUTE_AFTER_TOOLS.has(tc.toolName)) continue;

              // Skip failed tool calls
              const resultText = typeof tr?.output === "string" ? tr.output : String(tr?.output ?? "");
              if (resultText.startsWith("[ERROR]")) continue;

              const operatorId = (tc.input as any)?.operatorId;
              if (!operatorId) continue;

              try {
                await executeOperatorAndFormat(
                  this.workflowState,
                  execConfig,
                  operatorId,
                  {
                    abortSignal: this.abortController?.signal,
                    onResult: (opId, operatorInfo) => {
                      this.operatorResultStore.set(opId, this.head, operatorInfo);
                    },
                  }
                );
              } catch (e: any) {
                // Non-fatal: log but don't fail the step
                console.warn(`[PostStepExecution] Failed to execute ${operatorId}:`, e?.message || e);
              }
            }
          }

          // Update before snapshot for next step
          beforeStepContent = afterStepContent;
          isFirstStep = false;
          // Note: per-step usage is stored in each ReActStep.usage
          // Final totals are computed from result.totalUsage after generateText completes
        },
      });

      // Mark the last step as isEnd: true (instead of creating a duplicate final step)
      const msgSteps = this.reActStepsByMessageId.get(messageId);
      if (msgSteps && msgSteps.length > 0) {
        const lastStep = msgSteps[msgSteps.length - 1];
        if (lastStep.role === "agent") {
          lastStep.isEnd = true;
        }
      }

      // Prefer totalUsage (aggregate) over the final step's usage
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
      // Check if this was an abort (user requested stop)
      const isAborted = error.name === "AbortError" || this.abortController?.signal.aborted;

      if (isAborted) {
        // Handle stop gracefully
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

      // Handle actual error - add error step
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

  /**
   * Build a Map<operatorId, formattedString> from the versioned result store
   * using the current HEAD. Used for DAG serialization in no-action-detail filter.
   */
  private getFormattedResultsForDAG(): Map<string, string> {
    const result = new Map<string, string>();
    const visible = this.operatorResultStore.getAllVisible();
    const formatOpts: FormatOptions = {
      maxCharLimit: this.settings.maxOperatorResultCharLimit,
    };
    for (const [operatorId, entry] of visible) {
      result.set(operatorId, formatOperatorResult(operatorId, entry.operatorInfo, this.workflowState, formatOpts));
    }
    return result;
  }

  /**
   * Stop the current message processing immediately.
   * This aborts any ongoing LLM calls and tool executions.
   */
  stop(): void {
    this.state = AgentStateEnum.STOPPING;
    if (this.abortController) {
      this.abortController.abort();
    }
  }

  /**
   * Clear conversation history and ReActSteps.
   */
  clearHistory(): void {
    this.reActStepsByMessageId.clear();
    this.stepsById.clear();
    this.currentMessageId = undefined;
    this.head = INITIAL_STEP_ID;
    // Re-create initial step
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

  // ============================================================================
  // Context Filtering Methods (ReActStep-based)
  // ============================================================================

  /**
   * Extract operator IDs that were affected by a ReActStep.
   * Looks at tool results to find operator IDs (added/modified).
   *
   * @param step - The ReActStep to analyze
   * @returns Object with added and modified operator IDs
   */
  private getOperatorIdsFromStep(step: ReActStep): { added: string[]; modified: string[] } {
    const added: string[] = [];
    const modified: string[] = [];

    if (!step.toolResults) {
      return { added, modified };
    }

    for (const result of step.toolResults) {
      if (result.isError || !result.output) continue;

      // Find the corresponding tool call to determine the operation type
      const toolCall = step.toolCalls?.find(tc => tc.toolCallId === result.toolCallId);
      const toolName = toolCall?.toolName || "";

      const outputStr = typeof result.output === "string" ? result.output : JSON.stringify(result.output);

      // Extract operator ID from string patterns like:
      // - "Added operator op-123, number of input ports: 1, number of output ports: 1"
      // - "Operator op-123 modified"
      // Pattern matches operator IDs like "operator-uuid-123" or "op-123"
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

      // Try JSON parsing as fallback for structured outputs
      try {
        const output = JSON.parse(outputStr);
        if (output.operatorId) {
          if (toolName === "addOperator" || toolName === "addCodeOperator") {
            added.push(output.operatorId);
          } else if (toolName === "modifyOperator" || toolName === "modifyCodeOperator") {
            modified.push(output.operatorId);
          }
        }
      } catch {
        // Not JSON, already handled string patterns above
      }
    }

    return { added, modified };
  }

  /**
   * Get ReActSteps that affected the specified operator IDs.
   *
   * @param operatorIds - The operator IDs to filter by
   * @returns Array of relevant ReActSteps
   */
  public getReActStepsByOperatorIds(operatorIds: string[]): ReActStep[] {
    const allSteps = this.getReActSteps();
    if (!operatorIds || operatorIds.length === 0) {
      return allSteps;
    }

    const operatorIdSet = new Set(operatorIds);
    const relevantSteps: ReActStep[] = [];

    for (const step of allSteps) {
      const { added, modified } = this.getOperatorIdsFromStep(step);

      // Check if any of the step's operators match the requested IDs
      const affectsOperator = [...added, ...modified].some(id => operatorIdSet.has(id));

      if (affectsOperator) {
        relevantSteps.push(step);
      }
    }

    return relevantSteps;
  }

  /**
   * Build context text from relevant ReActSteps for the specified operator IDs.
   * This text is prepended to the user message.
   *
   * @param operatorIds - The operator IDs to filter by
   * @returns Context text string, or empty string if no relevant context
   */
  private buildContextText(operatorIds: string[]): string {
    const relevantSteps = this.getReActStepsByOperatorIds(operatorIds);

    if (relevantSteps.length === 0) {
      return "";
    }

    // Serialize relevant steps to text
    const stepsText = relevantSteps
      .map(step => {
        let text = "";

        // User message
        if (step.role === "user") {
          text = `[User]: ${step.content}`;
        } else {
          // Agent step
          text = `[Agent Step ${step.stepId}]`;
          if (step.content) {
            text += `\nThinking: ${step.content}`;
          }
          if (step.toolCalls && step.toolCalls.length > 0) {
            for (const tc of step.toolCalls) {
              text += `\nTool: ${tc.toolName}`;
              // Only include minimal input info to keep context concise
              if (tc.input) {
                const inputStr = JSON.stringify(tc.input);
                if (inputStr.length < 200) {
                  text += ` - Input: ${inputStr}`;
                }
              }
            }
          }
          if (step.toolResults && step.toolResults.length > 0) {
            for (const tr of step.toolResults) {
              if (!tr.isError && tr.output) {
                const outputStr = typeof tr.output === "string" ? tr.output : JSON.stringify(tr.output);
                // Truncate long outputs
                const truncated = outputStr.length > 300 ? outputStr.substring(0, 300) + "..." : outputStr;
                text += `\nResult: ${truncated}`;
              }
            }
          }
        }

        return text;
      })
      .join("\n\n");

    return `Here is the relevant conversation history for the operators you're asking about:\n\n${stepsText}\n\n---`;
  }

  /**
   * Cleanup and disconnect any resources.
   * This properly cleans up RxJS subscriptions via workflowState.destroy().
   */
  destroy(): void {
    // Cleanup workflow change subscription
    if (this.workflowChangeSubscription) {
      this.workflowChangeSubscription.unsubscribe();
      this.workflowChangeSubscription = null;
    }

    // Cleanup workflow state (unsubscribes all RxJS subscriptions, completes subjects)
    this.workflowState.destroy();

    // Clear websocket connections
    this.websockets.clear();

    // Clear conversation history and step tree
    this.reActStepsByMessageId.clear();
    this.stepsById.clear();
    this.currentMessageId = undefined;
  }
}
