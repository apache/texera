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
import { WorkflowState } from "../workflow/workflow-state";
import { OperatorMetadataStore } from "../tools/metadata-tools";
import { AgentActionManager, INITIAL_ACTION_ID } from "./agent-action-manager";
import { OperatorResultStore } from "./operator-result-store";
import { formatOperatorResult, type FormatOptions } from "../tools/result-formatting";
import type {
  AgentSettings,
  ReActStep,
  AgentMessageStats,
  TokenUsage,
  AgentAction,
  UserInfo,
  TraceContent,
} from "../types/agent";
import {
  AgentState as AgentStateEnum,
  DEFAULT_AGENT_SETTINGS,
  OperatorResultSerializationMode,
  AgentMode,
  ExecutionBackend,
  REPLAY_SKIP_TOOLS,
} from "../types/agent";
import {
  BASE_SYSTEM_PROMPT,
  buildGeneralModeSystemPrompt,
  buildCodeModeSystemPrompt,
  EXAMPLES_STANDARD,
  EXAMPLES_CARRY_METADATA,
  EXAMPLES_FINE_GRAINED,
  EXAMPLES_PARALLEL,
  EXAMPLES_RESULT_PARAM,
  EXAMPLES_PARALLEL_RESULT_PARAM,
  EXAMPLES_NO_ACTION_DETAIL,
  EXAMPLES_NO_ACTION_DETAIL_CARRY_METADATA,
  EXAMPLES_NO_ACTION_DETAIL_CARRY_METADATA_PARALLEL,
} from "./prompts";
import {
  createDeleteOperatorTool,
  TOOL_NAME_DELETE_OPERATOR,
  type ToolContext,
} from "../tools/workflow-tools";
import { ParallelCallCoordinator } from "../tools/parallel-call-coordinator";
import {
  createAddOperatorTool,
  createModifyOperatorTool,
  TOOL_NAME_ADD_OPERATOR,
  TOOL_NAME_MODIFY_OPERATOR,
} from "../tools/general-op-tools";
import {
  createCreateOrModifyOperatorTool,
  TOOL_NAME_CREATE_OR_MODIFY_OPERATOR,
} from "../tools/code-op-tools";
import {
  createExecuteOperatorTool,
  executeOperatorAndFormat,
  TOOL_NAME_EXECUTE_OPERATOR,
  type ExecutionConfig,
} from "../tools/execution-tools";
import { trimNonFrontierResults } from "./context-optimization";
import { assembleContext } from "./context-assembler";

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
  /** Custom system prompt (optional, defaults to BASE_SYSTEM_PROMPT) */
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
  /** Message statistics */
  stats: AgentMessageStats;
  /** Whether the agent was stopped early */
  stopped: boolean;
  /** Error message if any */
  error?: string;
}

// ============================================================================
// Texera Agent Class
// ============================================================================

/** Callback for receiving ReActStep updates */
export type ReActStepCallback = (step: ReActStep) => void;

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
  // Agent action manager for tracking workflow modifications
  private agentActionManager: AgentActionManager;
  // Versioned operator result store
  private operatorResultStore: OperatorResultStore;

  // Server-managed state (for HTTP/WebSocket handling)
  /** RxJS subscription for agent action streaming */
  private agentActionSubscription: Subscription | null = null;
  /** Active WebSocket connections for this agent */
  private websockets: Set<any> = new Set();

  // Configuration
  private model: LanguageModel;
  private systemPrompt: string;
  private settings: AgentSettings;

  // ReActSteps grouped by messageId
  private reActStepsByMessageId: Map<string, ReActStep[]> = new Map();

  // messageId → (startActionId, endActionId) — which HEAD range a message spans
  private messageActionRange: Map<string, { startActionId: string; endActionId: string }> = new Map();

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
    this.systemPrompt = config.systemPrompt || BASE_SYSTEM_PROMPT;

    // Initialize state
    this.workflowState = new WorkflowState();
    // Always use global singleton metadata store
    this.metadataStore = OperatorMetadataStore.getInstance();
    // Initialize agent action manager
    this.agentActionManager = new AgentActionManager();
    // Initialize versioned operator result store
    this.operatorResultStore = new OperatorResultStore(this.agentActionManager);

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

      // Rebuild system prompt based on agent mode
      this.rebuildSystemPrompt();

      // Rebuild tools with loaded metadata
      this.tools = this.createTools();
      console.log(
        `[TexeraAgent ${this.agentId}] Initialized in ${this.settings.agentMode} mode with ${this.metadataStore.getOperatorCount()} operators`
      );
    } catch (error) {
      console.error(`[TexeraAgent ${this.agentId}] Failed to initialize metadata:`, error);
      // Continue with empty metadata - tools will still work but addOperator will fail
    }
  }

  /**
   * Rebuild system prompt based on current agent mode and settings.
   * GENERAL mode: includes operator schemas in the prompt
   * CODE mode: uses structured prompt with examples
   * fineGrainedPrompt: uses stricter atomic operation constraints
   */
  private rebuildSystemPrompt(): void {
    if (this.settings.agentMode === AgentMode.GENERAL) {
      this.systemPrompt = buildGeneralModeSystemPrompt(this.metadataStore, this.settings.allowedOperatorTypes);
    } else {
      let examples: string;
      if (this.settings.noActionDetail && this.settings.carryMetadata && this.settings.parallelToolCalls) {
        examples = EXAMPLES_NO_ACTION_DETAIL_CARRY_METADATA_PARALLEL;
      } else if (this.settings.noActionDetail && this.settings.carryMetadata) {
        examples = EXAMPLES_NO_ACTION_DETAIL_CARRY_METADATA;
      } else if (this.settings.noActionDetail) {
        examples = EXAMPLES_NO_ACTION_DETAIL;
      } else if (this.settings.carryMetadata) {
        examples = EXAMPLES_CARRY_METADATA;
      } else if (this.settings.fineGrainedPrompt) {
        examples = EXAMPLES_FINE_GRAINED;
      } else if (this.settings.parallelToolCalls && this.settings.optionalResultRetrieval) {
        examples = EXAMPLES_PARALLEL_RESULT_PARAM;
      } else if (this.settings.parallelToolCalls) {
        examples = EXAMPLES_PARALLEL;
      } else if (this.settings.optionalResultRetrieval) {
        examples = EXAMPLES_RESULT_PARAM;
      } else {
        examples = EXAMPLES_STANDARD;
      }
      this.systemPrompt = buildCodeModeSystemPrompt(examples, this.settings.noActionDetail);
    }
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
      serializationMode: this.settings.operatorResultSerializationMode,
      executionTimeoutMs: this.settings.executionTimeoutMs,
      cacheEnabled: this.settings.cacheEnabled,
      executionBackend: this.settings.executionBackend,
      noExecutionMetadata: this.settings.noExecutionMetadata,
      carryMetadata: this.settings.carryMetadata,
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
      // Coordinate parallel tool calls with inter-operator dependencies
      parallelCoordinator: this.settings.parallelToolCalls
        ? new ParallelCallCoordinator(this.settings.toolTimeoutMs)
        : undefined,
    };

    // Common tools for both modes
    const tools: Record<string, any> = {
      [TOOL_NAME_DELETE_OPERATOR]: createDeleteOperatorTool(this.workflowState, context),
    };

    // Mode-specific tools
    if (this.settings.agentMode === AgentMode.CODE) {
      // CODE mode: Use unified coding tool (creates or modifies operators)
      tools[TOOL_NAME_CREATE_OR_MODIFY_OPERATOR] = createCreateOrModifyOperatorTool(this.workflowState, operatorSchemas, context);
    } else {
      // GENERAL mode: Use workflow tools (addOperator, modifyOperator)
      // Links are created automatically via inputOperatorIds in addOperator
      tools[TOOL_NAME_ADD_OPERATOR] = createAddOperatorTool(this.workflowState, operatorSchemas, context);
      tools[TOOL_NAME_MODIFY_OPERATOR] = createModifyOperatorTool(this.workflowState, context);
    }

    // Add execution tools if delegateConfig is available (requires user token and workflow ID)
    // In CODE mode, execution is handled inline — no separate executeOperator needed
    // When noActionDetail is on, executeOperator is also not needed
    if (getExecutionConfig && !this.settings.simplifiedTools && !this.settings.noActionDetail && this.settings.agentMode !== AgentMode.CODE) {
      tools[TOOL_NAME_EXECUTE_OPERATOR] = createExecuteOperatorTool(
        this.workflowState,
        getExecutionConfig,
        (opId, operatorInfo) => {
          const actionId = this.agentActionManager.getHead();
          this.operatorResultStore.set(opId, actionId, operatorInfo);
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

  getAgentActionManager(): AgentActionManager {
    return this.agentActionManager;
  }

  getOperatorResultStore(): OperatorResultStore {
    return this.operatorResultStore;
  }

  /**
   * Get all agent actions.
   */
  getAgentActions(): AgentAction[] {
    return this.agentActionManager.getAllAgentActions();
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

  // ============================================================================
  // Agent Action Subscription Management (for server use)
  // ============================================================================

  /**
   * Set the agent action subscription for streaming.
   */
  setAgentActionSubscription(subscription: Subscription | null): void {
    this.agentActionSubscription = subscription;
  }

  /**
   * Get the agent action subscription.
   */
  getAgentActionSubscription(): Subscription | null {
    return this.agentActionSubscription;
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
   * Get the visible ReActSteps for a messageId, truncated to the cutoff step.
   *
   * For completed messages on the HEAD path, only steps up to (and including) the step
   * that contains the last action's toolCallId are returned. This prevents showing steps
   * from sibling branches that share the same messageId.
   *
   * For the current in-flight message, all steps are returned (no truncation).
   */
  private getStepsForMessage(
    msgId: string,
    lastActionPerMessage: Map<string, string>
  ): ReActStep[] {
    const allSteps = this.reActStepsByMessageId.get(msgId);
    if (!allSteps || allSteps.length === 0) return [];

    // Current in-flight message: return all steps (no truncation)
    if (msgId === this.currentMessageId) return allSteps;

    // Find the last action on the path for this messageId
    const lastActionId = lastActionPerMessage.get(msgId);
    if (!lastActionId) return allSteps; // no action for this message — return all

    const action = this.agentActionManager.getAgentAction(lastActionId);
    if (!action) return allSteps;

    // Truncation based on action type:
    // - user_request: only show the user step (nothing has happened yet)
    // - agent_response: show all steps (everything is done)
    // - tool_call: truncate to the step containing the tool call
    if (action.actionType === "user_request") {
      return allSteps.filter(s => s.role === "user");
    }

    if (action.actionType === "agent_response") {
      return allSteps;
    }

    // tool_call: find the step containing the toolCallId and truncate there
    if (!action.toolCallId) return allSteps;
    const toolCallId = action.toolCallId;
    let cutoffStepId = -1;
    for (const step of allSteps) {
      if (step.toolCalls?.some(tc => tc.toolCallId === toolCallId)) {
        cutoffStepId = step.stepId;
      }
    }

    if (cutoffStepId === -1) return allSteps; // toolCallId not found — return all
    return allSteps.filter(s => s.stepId <= cutoffStepId);
  }

  /**
   * Get ReActSteps visible from the current HEAD.
   *
   * 1. Walk the action tree from root to HEAD → get messageIds on that path.
   * 2. For each messageId, truncate steps to the cutoff point (last action on path).
   * 3. Include the current in-flight messageId with all its steps.
   */
  getVisibleReActSteps(): ReActStep[] {
    const lastActionPerMessage = this.agentActionManager.getLastActionPerMessageOnPath();
    const messageIds = this.agentActionManager.getMessageIdsOnPath();
    if (this.currentMessageId && !messageIds.includes(this.currentMessageId)) {
      messageIds.push(this.currentMessageId);
    }

    if (messageIds.length === 0) return [];

    const result: ReActStep[] = [];
    for (const msgId of messageIds) {
      result.push(...this.getStepsForMessage(msgId, lastActionPerMessage));
    }
    return result;
  }

  /**
   * Get the historical interactions as ModelMessage[] for the current HEAD path.
   *
   * 1. Get messageIds on the ancestor path (chronological order).
   * 2. Include the current in-flight messageId.
   * 3. For each messageId, get truncated steps, serialize into a summary text.
   * 4. Wrap each as a user-type ModelMessage.
   */
  getHistoricalInteractions(): ModelMessage[] {
    const lastActionPerMessage = this.agentActionManager.getLastActionPerMessageOnPath();
    const messageIds = this.agentActionManager.getMessageIdsOnPath();
    if (this.currentMessageId && !messageIds.includes(this.currentMessageId)) {
      messageIds.push(this.currentMessageId);
    }

    const result: ModelMessage[] = [];
    for (const msgId of messageIds) {
      const steps = this.getStepsForMessage(msgId, lastActionPerMessage);
      if (steps.length === 0) continue;

      const text = this.serializeInteraction(msgId, steps);
      result.push({ role: "user", content: text });
    }
    return result;
  }

  /**
   * Serialize a single message's ReActSteps into a summary string.
   */
  private serializeInteraction(messageId: string, steps: ReActStep[]): string {
    const isOngoing = messageId === this.currentMessageId;
    const lines: string[] = [];

    // Extract user task content first, then assistant steps
    const userStep = steps.find(s => s.role === "user");
    const assistantSteps = steps.filter(s => s.role !== "user");

    // Task header
    lines.push(`[Task: ${userStep?.content ?? ""}]`);

    // Assistant steps (if any)
    if (assistantSteps.length > 0) {
      lines.push("The steps you as an assistant already took:");
      for (const step of assistantSteps) {
        lines.push(`[Assistant Step ${step.stepId}]`);
        if (step.content) {
          lines.push(`- thought: ${step.content}`);
        }
        if (step.toolCalls && step.toolCalls.length > 0) {
          lines.push(`- actions:`);
          for (let i = 0; i < step.toolCalls.length; i++) {
            const tc = step.toolCalls[i];
            const tr = step.toolResults?.[i];
            if (tr && !tr.isError) {
              const outputStr = typeof tr.output === "string" ? tr.output : String(tr.output ?? "");
              lines.push(`    - ${tc.toolName}: ${outputStr}`);
            } else if (tr?.isError) {
              lines.push(`    - ${tc.toolName}: [ERROR]`);
            } else {
              lines.push(`    - ${tc.toolName}`);
            }
          }
        }
        lines.push(`[Step ${step.stepId} Finished]`);
      }
    }

    // Footer: only for completed tasks
    if (!isOngoing) {
      lines.push("[Task Completed]");
    }

    return lines.join("\n");
  }

  /**
   * Checkout to a specific agent action: move HEAD, restore workflow state.
   * Returns false if the action doesn't exist.
   */
  checkout(actionId: string): boolean {
    const result = this.agentActionManager.checkout(actionId);
    if (!result) return false;
    if (result.workflowContent) {
      this.workflowState.setWorkflowContent(result.workflowContent);
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
   * Add a ReActStep and notify the callback if set.
   */
  private addStep(step: ReActStep): void {
    let steps = this.reActStepsByMessageId.get(step.messageId);
    if (!steps) {
      steps = [];
      this.reActStepsByMessageId.set(step.messageId, steps);
    }
    steps.push(step);
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
    agentMode?: AgentMode;
    fineGrainedPrompt?: boolean;
    enableContextOptimization?: boolean;
    frontierDepth?: number;
    minimumResultCharLimit?: number;
    cacheEnabled?: boolean;
    executionBackend?: ExecutionBackend;
    latestOnly?: boolean;
    dynamicDepthEnabled?: boolean;
    parallelToolCalls?: boolean;
    optionalResultRetrieval?: boolean;
    noExecutionMetadata?: boolean;
    simplifiedTools?: boolean;
    noActionDetail?: boolean;
    noLogFallback?: boolean;
    carryMetadata?: boolean;
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
    if (updates.agentMode !== undefined && updates.agentMode !== this.settings.agentMode) {
      this.settings.agentMode = updates.agentMode;
      promptNeedsRebuild = true;
    }
    if (updates.fineGrainedPrompt !== undefined && updates.fineGrainedPrompt !== this.settings.fineGrainedPrompt) {
      this.settings.fineGrainedPrompt = updates.fineGrainedPrompt;
      promptNeedsRebuild = true;
    }
    if (updates.enableContextOptimization !== undefined) {
      this.settings.enableContextOptimization = updates.enableContextOptimization;
    }
    if (updates.frontierDepth !== undefined) {
      this.settings.frontierDepth = Math.max(1, updates.frontierDepth);
    }
    if (updates.minimumResultCharLimit !== undefined) {
      this.settings.minimumResultCharLimit = Math.max(0, updates.minimumResultCharLimit);
    }
    if (updates.cacheEnabled !== undefined) {
      this.settings.cacheEnabled = updates.cacheEnabled;
    }
    if (updates.executionBackend !== undefined) {
      this.settings.executionBackend = updates.executionBackend;
    }
    if (updates.latestOnly !== undefined) {
      this.settings.latestOnly = updates.latestOnly;
    }
    if (updates.dynamicDepthEnabled !== undefined) {
      this.settings.dynamicDepthEnabled = updates.dynamicDepthEnabled;
    }
    if (updates.parallelToolCalls !== undefined && updates.parallelToolCalls !== this.settings.parallelToolCalls) {
      this.settings.parallelToolCalls = updates.parallelToolCalls;
      promptNeedsRebuild = true;
    }
    if (updates.optionalResultRetrieval !== undefined && updates.optionalResultRetrieval !== this.settings.optionalResultRetrieval) {
      this.settings.optionalResultRetrieval = updates.optionalResultRetrieval;
      promptNeedsRebuild = true;
    }
    if (updates.noExecutionMetadata !== undefined) {
      this.settings.noExecutionMetadata = updates.noExecutionMetadata;
    }
    if (updates.simplifiedTools !== undefined) {
      this.settings.simplifiedTools = updates.simplifiedTools;
    }
    if (updates.noActionDetail !== undefined && updates.noActionDetail !== this.settings.noActionDetail) {
      this.settings.noActionDetail = updates.noActionDetail;
      promptNeedsRebuild = true;
    }
    if (updates.noLogFallback !== undefined) {
      this.settings.noLogFallback = updates.noLogFallback;
    }
    if (updates.carryMetadata !== undefined && updates.carryMetadata !== this.settings.carryMetadata) {
      this.settings.carryMetadata = updates.carryMetadata;
      promptNeedsRebuild = true;
    }
    if (updates.allowedOperatorTypes !== undefined) {
      this.settings.allowedOperatorTypes = updates.allowedOperatorTypes;
      promptNeedsRebuild = true;
    }

    // If mode or fineGrainedPrompt changed, rebuild system prompt
    if (promptNeedsRebuild) {
      this.rebuildSystemPrompt();
    }

    // Rebuild tools with updated settings
    this.tools = this.createTools();
    console.log(
      `[TexeraAgent ${this.agentId}] Settings updated: ` +
        `mode=${this.settings.agentMode}, ` +
        `fineGrainedPrompt=${this.settings.fineGrainedPrompt}, ` +
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
    // If HEAD points to a real action (not the initial dummy), the workflow is determined
    // by that action's snapshot. Only load from backend when HEAD is the initial state.
    if (this.agentActionManager.getHead() !== INITIAL_ACTION_ID) {
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
    const startTime = Date.now();
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

    // Initialize stats
    const stats: AgentMessageStats = {
      messageId,
      userMessage,
      startTime,
      totalInputTokens: 0,
      totalOutputTokens: 0,
      totalTokens: 0,
      stepCount: 0,
      status: "running",
    };

    // Create new abort controller for this message
    this.abortController = new AbortController();

    // Set state to generating
    this.state = AgentStateEnum.GENERATING;

    this.currentMessageId = messageId;

    try {
      // Create user message step (stepId 0) - show original message for display
      const estimatedInputTokens = Math.ceil(actualUserMessage.length / 4);
      const userStep: ReActStep = {
        messageId,
        stepId: 0,
        timestamp: Date.now(),
        role: "user",
        content: userMessage,
        actualContent: actualUserMessage !== userMessage ? actualUserMessage : undefined,
        isBegin: true,
        isEnd: true,
        usage: {
          inputTokens: estimatedInputTokens,
          outputTokens: 0,
          totalTokens: estimatedInputTokens,
        },
      };
      this.addStep(userStep);

      let isFirstStep = true;
      let lastPreparedMessages: ModelMessage[] | undefined;
      // Capture workflow state before each step for action before/after snapshots
      let beforeStepContent = this.workflowState.getWorkflowContent();

      // Create "user_request" action so every user message appears in the action tree
      const userAction = this.agentActionManager.createAgentAction(
        this.agentId,
        this.agentName,
        `User: ${userMessage.substring(0, 50)}${userMessage.length > 50 ? "..." : ""}`,
        {}, // empty operations — no workflow changes
        this.delegateConfig
          ? { wid: this.delegateConfig.workflowId, name: this.delegateConfig.workflowName }
          : {},
        beforeStepContent,
        beforeStepContent, // after = same (no change)
        undefined,
        undefined,
        "user_request",
        messageSource
      );
      this.agentActionManager.setActionMessageId(userAction.id, messageId);
      const startActionId = userAction.id;

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
        prepareStep: ({ stepNumber, messages: currentMessages }) => {
              // Build historical interactions from HEAD path + current message
              const historicalInteractions = this.getHistoricalInteractions();

              // Assemble context: historical interactions + DAG summary with execution results.
              // useRedact controls whether operator properties are shown in the DAG
              // (properties are always shown for operators with execution errors).
              const useRedact = this.settings.noActionDetail;
              let processed = assembleContext(historicalInteractions, this.workflowState, this.getFormattedResultsForDAG(), useRedact);
              // context optimization: trims execution result sections
              if (this.settings.enableContextOptimization) {
                const effectiveDepth = this.settings.dynamicDepthEnabled
                  ? this.workflowState.computeAveragePathLength()
                  : this.settings.frontierDepth;
                processed = trimNonFrontierResults(
                  processed,
                  this.workflowState,
                  effectiveDepth,
                  this.settings.agentMode,
                  this.settings.minimumResultCharLimit,
                  this.settings.maxOperatorResultCharLimit,
                  this.settings.noLogFallback
                );
              }
              lastPreparedMessages = processed;
              return { messages: processed };
            },
        abortSignal: this.abortController?.signal,
        // Note: reasoning_effort is NOT passed here — it's configured per-model in
        // litellm-config.yaml via extra_body to bypass LiteLLM's param validation.
        providerOptions: this.settings.parallelToolCalls
          ? {}
          : {
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

          // Create agent actions for workflow-modifying tool calls and executions.
          // We have full context here: toolCallId, tool result (success/error), and
          // before/after workflow snapshots.
          const afterStepContent = this.workflowState.getWorkflowContent();
          if (toolCalls && toolResults) {
            const ACTION_TOOL_NAMES = new Set([
              TOOL_NAME_ADD_OPERATOR, TOOL_NAME_MODIFY_OPERATOR,
              TOOL_NAME_CREATE_OR_MODIFY_OPERATOR, TOOL_NAME_DELETE_OPERATOR,
              TOOL_NAME_EXECUTE_OPERATOR,
            ]);
            for (let i = 0; i < toolCalls.length; i++) {
              const tc = toolCalls[i];
              const tr = toolResults[i];
              if (!ACTION_TOOL_NAMES.has(tc.toolName)) continue;
              // Skip failed tool calls — but always create actions for executeOperator
              // so that errors are versioned and visible in the DAG summary
              const resultText = typeof tr?.output === "string" ? tr.output : String(tr?.output ?? "");
              if (resultText.startsWith("[ERROR]") && tc.toolName !== TOOL_NAME_EXECUTE_OPERATOR) continue;

              const operatorId = (tc.input as any)?.operatorId || "unknown";
              const summary = (tc.input as any)?.summary || `${tc.toolName} ${operatorId}`;
              const action = this.agentActionManager.createAgentAction(
                this.agentId,
                this.agentName,
                summary,
                this.parseOperations(tc.toolName, operatorId),
                this.delegateConfig
                  ? { wid: this.delegateConfig.workflowId, name: this.delegateConfig.workflowName }
                  : {},
                beforeStepContent,
                afterStepContent,
                undefined,
                tc.toolCallId
              );
              // Link this action to the current messageId
              this.agentActionManager.setActionMessageId(action.id, messageId);
              // Progressively update the action range for this message
              this.messageActionRange.set(messageId, { startActionId, endActionId: action.id });
            }
          }
          // Post-step auto-execution: execute operators that were successfully added/modified
          const execConfig = this.buildExecutionConfig();
          if (execConfig && toolCalls && toolResults) {
            const EXECUTE_AFTER_TOOLS = new Set([
              TOOL_NAME_ADD_OPERATOR, TOOL_NAME_MODIFY_OPERATOR,
              TOOL_NAME_CREATE_OR_MODIFY_OPERATOR,
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
                      const actionId = this.agentActionManager.getHead();
                      this.operatorResultStore.set(opId, actionId, operatorInfo);
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

          // Create agent step
          const agentStep: ReActStep = {
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
          };
          lastPreparedMessages = undefined;
          this.addStep(agentStep);

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

      // Create "agent_response" action so every agent reply appears in the action tree
      const finalContent = this.workflowState.getWorkflowContent();
      const responseText = result.text || "";
      const agentResponseAction = this.agentActionManager.createAgentAction(
        this.agentId,
        this.agentName,
        `Agent: ${responseText.substring(0, 50)}${responseText.length > 50 ? "..." : ""}`,
        {}, // empty operations
        this.delegateConfig
          ? { wid: this.delegateConfig.workflowId, name: this.delegateConfig.workflowName }
          : {},
        finalContent,
        finalContent, // after = same
        undefined,
        undefined,
        "agent_response"
      );
      this.agentActionManager.setActionMessageId(agentResponseAction.id, messageId);
      this.messageActionRange.set(messageId, { startActionId, endActionId: agentResponseAction.id });

      // Update final stats - use totalUsage from result (aggregate across all steps)
      // Note: result.usage is only the final step, result.totalUsage is the aggregate
      stats.endTime = Date.now();
      stats.stepCount = stepIndex;
      stats.status = "completed";

      // Prefer totalUsage (aggregate) over accumulated values
      const finalUsage = (result as any).totalUsage || result.usage;
      if (finalUsage) {
        stats.totalInputTokens = finalUsage.inputTokens || finalUsage.promptTokens || 0;
        stats.totalOutputTokens = finalUsage.outputTokens || finalUsage.completionTokens || 0;
        stats.totalTokens = finalUsage.totalTokens || 0;
      }

      return {
        response: result.text,
        messages: result.response.messages,
        usage: {
          inputTokens: stats.totalInputTokens,
          outputTokens: stats.totalOutputTokens,
          totalTokens: stats.totalTokens,
        },
        stats,
        stopped: false,
      };
    } catch (error: any) {
      // Check if this was an abort (user requested stop)
      const isAborted = error.name === "AbortError" || this.abortController?.signal.aborted;

      if (isAborted) {
        // Handle stop gracefully
        stepIndex++;
        const stoppedStep: ReActStep = {
          messageId,
          stepId: stepIndex,
          timestamp: Date.now(),
          role: "agent",
          content: "Generation stopped by user.",
          isBegin: false,
          isEnd: true,
        };
        this.addStep(stoppedStep);

        stats.endTime = Date.now();
        stats.stepCount = stepIndex;
        stats.status = "stopped";

        return {
          response: "",
          messages: [],
          usage: {
            inputTokens: stats.totalInputTokens,
            outputTokens: stats.totalOutputTokens,
            totalTokens: stats.totalTokens,
          },
          stats,
          stopped: true,
        };
      }

      // Handle actual error - add error step
      stepIndex++;
      const errorStep: ReActStep = {
        messageId,
        stepId: stepIndex,
        timestamp: Date.now(),
        role: "agent",
        content: `Error: ${error.message || String(error)}`,
        isBegin: false,
        isEnd: true,
      };
      this.addStep(errorStep);

      // Update stats
      stats.endTime = Date.now();
      stats.stepCount = stepIndex;
      stats.status = "error";
      stats.errorMessage = error.message || String(error);

      return {
        response: "",
        messages: [],
        usage: {
          inputTokens: stats.totalInputTokens,
          outputTokens: stats.totalOutputTokens,
          totalTokens: stats.totalTokens,
        },
        stats,
        stopped: false,
        error: stats.errorMessage,
      };
    } finally {
      this.abortController = null;
      this.currentMessageId = undefined;
      this.state = AgentStateEnum.AVAILABLE;
    }
  }

  /**
   * Derive AgentActionOperations from a tool name and operator ID.
   */
  private parseOperations(toolName: string, operatorId: string): import("../types/agent").AgentActionOperations {
    switch (toolName) {
      case TOOL_NAME_ADD_OPERATOR:
      case TOOL_NAME_CREATE_OR_MODIFY_OPERATOR:
        return { add: { operatorIds: [operatorId], linkIds: [] } };
      case TOOL_NAME_MODIFY_OPERATOR:
        return { modify: { operatorIds: [operatorId] } };
      case TOOL_NAME_DELETE_OPERATOR:
        return { delete: { operatorIds: [operatorId], linkIds: [] } };
      case TOOL_NAME_EXECUTE_OPERATOR:
        return { execute: { operatorIds: [operatorId] } };
      default:
        return { modify: { operatorIds: [operatorId] } };
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
      serializationMode: this.settings.operatorResultSerializationMode,
      maxCharLimit: this.settings.maxOperatorResultCharLimit,
      carryMetadata: this.settings.carryMetadata,
      noExecutionMetadata: this.settings.noExecutionMetadata,
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
    this.messageActionRange.clear();
    this.currentMessageId = undefined;
    this.agentActionManager.clearAllAgentActions();
  }

  /**
   * Reset the agent (clear history, ReActSteps, actions, and workflow).
   */
  reset(): void {
    this.reActStepsByMessageId.clear();
    this.messageActionRange.clear();
    this.currentMessageId = undefined;
    this.agentActionManager.clearAllAgentActions();
    this.workflowState.reset();
    this.operatorResultStore.clear();
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
    // Cleanup agent action subscription
    if (this.agentActionSubscription) {
      this.agentActionSubscription.unsubscribe();
      this.agentActionSubscription = null;
    }

    // Cleanup workflow change subscription
    if (this.workflowChangeSubscription) {
      this.workflowChangeSubscription.unsubscribe();
      this.workflowChangeSubscription = null;
    }

    // Cleanup workflow state (unsubscribes all RxJS subscriptions, completes subjects)
    this.workflowState.destroy();

    // Cleanup agent action manager
    this.agentActionManager.destroy();

    // Clear websocket connections
    this.websockets.clear();

    // Clear conversation history
    this.reActStepsByMessageId.clear();
    this.messageActionRange.clear();
    this.currentMessageId = undefined;
  }

  // ============================================================================
  // Trace Replay Methods
  // ============================================================================

  /**
   * Set the agent state (public method for replay).
   */
  setState(state: AgentStateEnum): void {
    this.state = state;
  }

  /**
   * Add a ReActStep publicly (for replay).
   */
  addReActStepPublic(step: ReActStep): void {
    this.addStep(step);
  }

  /**
   * Execute a single tool by name with the given input.
   * Returns the tool result as a string.
   * @throws Error if tool not found or execution fails
   */
  async executeTool(toolName: string, input: any): Promise<any> {
    const toolDef = this.tools[toolName];
    if (!toolDef) {
      throw new Error(`Tool '${toolName}' not found`);
    }
    if (!toolDef.execute) {
      throw new Error(`Tool '${toolName}' does not have an execute function`);
    }
    return await toolDef.execute(input);
  }

  /**
   * Replay a trace in two phases:
   *
   * Phase 1: Parse all messages and emit ReActSteps
   *   - Tool results are looked up from trace (not executed yet)
   *   - Each assistant message becomes ONE ReActStep
   *   - stepId resets for each user message
   *
   * Phase 2: Execute tool calls to build workflow
   *   - Skip execution tools (executeOperator, getOperatorResult, etc.)
   *   - Abort on any error (no rollback needed)
   *
   * @param trace - The trace content containing messages
   * @param onStep - Callback for each ReActStep generated
   * @param onError - Callback for errors
   * @returns Promise that resolves when replay is complete
   */
  async replayTrace(
    trace: TraceContent,
    onStep: (step: ReActStep) => void,
    onError: (error: string) => void
  ): Promise<void> {
    const messages = trace.messages;

    // ========================================================================
    // Phase 1: Build tool results map and emit ReActSteps
    // ========================================================================

    // Build a map of toolCallId -> tool result from all tool messages
    const toolResultsMap = new Map<string, { output: any; isError: boolean }>();
    for (const message of messages) {
      if (message.role === "tool") {
        const content = message.content;
        if (Array.isArray(content)) {
          for (const part of content) {
            if (part.type === "tool-result" && part.toolCallId) {
              // Handle both formats:
              // 1. Direct output: { output: "string" } or { output: {...} }
              // 2. Wrapped output: { output: { type: "text", value: "string" } }
              let rawOutput = part.output || part.result || part.content;
              if (rawOutput && typeof rawOutput === "object" && rawOutput.type === "text" && rawOutput.value !== undefined) {
                rawOutput = rawOutput.value;
              }
              toolResultsMap.set(part.toolCallId, {
                output: rawOutput,
                isError: part.isError || false,
              });
            }
          }
        } else if (message.tool_call_id) {
          // Alternative format: tool message with tool_call_id at message level
          toolResultsMap.set(message.tool_call_id, {
            output: content,
            isError: false,
          });
        }
      }
    }

    // Collect all tool calls for Phase 2 execution
    const allToolCalls: Array<{ toolName: string; toolCallId: string; input: any }> = [];

    let stepId = 0;
    let currentMessageId = "";

    this.state = AgentStateEnum.GENERATING;

    // Emit all ReActSteps
    for (const message of messages) {
      if (message.role === "user") {
        // Generate new message ID and reset stepId for this user message
        currentMessageId = `replay-${Date.now()}-${Math.random().toString(36).substring(7)}`;
        stepId = 0; // Reset stepId for each user message

        // Extract user content
        const userContent =
          typeof message.content === "string" ? message.content : JSON.stringify(message.content);

        // Create user step (stepId 0)
        const userStep: ReActStep = {
          messageId: currentMessageId,
          stepId: stepId++,
          timestamp: Date.now(),
          role: "user",
          content: userContent,
          isBegin: true,
          isEnd: true,
        };

        this.addStep(userStep);
        onStep(userStep);
      } else if (message.role === "assistant") {
        // Parse assistant message content
        const { textContent, toolCalls } = this.parseAssistantMessage(message);

        // Look up tool results from the map and collect for execution
        const toolCallsForStep: Array<{ toolName: string; toolCallId: string; input: any }> = [];
        const toolResultsForStep: Array<{ toolCallId: string; output: any; isError?: boolean }> = [];

        for (const toolCall of toolCalls) {
          toolCallsForStep.push({
            toolName: toolCall.toolName,
            toolCallId: toolCall.toolCallId,
            input: toolCall.input,
          });

          // Look up the result from the trace
          const result = toolResultsMap.get(toolCall.toolCallId);
          toolResultsForStep.push({
            toolCallId: toolCall.toolCallId,
            output: result?.output ?? "[Result not found in trace]",
            isError: result?.isError ?? false,
          });

          // Collect for Phase 2 execution
          allToolCalls.push(toolCall);
        }

        // Create ONE ReActStep for this assistant message
        // stepId 0 = user, stepId 1 = first assistant step, etc.
        const isFirstAssistantStep = stepId === 1;

        const assistantStep: ReActStep = {
          messageId: currentMessageId,
          stepId: stepId++,
          timestamp: Date.now(),
          role: "agent",
          content: textContent,
          isBegin: isFirstAssistantStep,
          isEnd: true,
          toolCalls: toolCallsForStep.length > 0 ? toolCallsForStep : undefined,
          toolResults: toolResultsForStep.length > 0 ? toolResultsForStep : undefined,
        };

        this.addStep(assistantStep);
        onStep(assistantStep);
      } else if (message.role === "tool") {
        // Tool messages are already processed into the map — no storage needed
      }
    }

    // ========================================================================
    // Phase 2: Execute tool calls to build workflow
    // ========================================================================

    for (const toolCall of allToolCalls) {
      // Skip execution tools
      if (REPLAY_SKIP_TOOLS.has(toolCall.toolName)) {
        console.log(`[Replay] Skipping execution tool: ${toolCall.toolName}`);
        continue;
      }

      try {
        console.log(`[Replay] Executing tool: ${toolCall.toolName}`);
        await this.executeTool(toolCall.toolName, toolCall.input);
      } catch (error: any) {
        const errorMsg = `Replay aborted: ${toolCall.toolName} failed - ${error.message || String(error)}`;
        console.error(`[Replay] ${errorMsg}`);
        onError(errorMsg);
        this.state = AgentStateEnum.AVAILABLE;
        return; // Abort without rollback
      }
    }

    this.state = AgentStateEnum.AVAILABLE;
    console.log(`[Replay] Completed successfully. Executed ${allToolCalls.length} tool calls.`);
  }

  /**
   * Parse an assistant message to extract text content and tool calls.
   */
  private parseAssistantMessage(message: any): {
    textContent: string;
    toolCalls: Array<{ toolName: string; toolCallId: string; input: any }>;
  } {
    let textContent = "";
    const toolCalls: Array<{ toolName: string; toolCallId: string; input: any }> = [];

    const content = message.content;

    if (typeof content === "string") {
      textContent = content;
    } else if (Array.isArray(content)) {
      for (const part of content) {
        if (part.type === "text") {
          textContent += part.text || "";
        } else if (part.type === "tool-call") {
          toolCalls.push({
            toolName: part.toolName,
            toolCallId: part.toolCallId,
            input: part.input || part.args || {},
          });
        }
      }
    }

    return { textContent, toolCalls };
  }
}
