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

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable, of, throwError, defer } from "rxjs";
import { finalize } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { toolWithTimeout } from "./tool/tools-utility";
import * as workflowMetadataTools from "./tool/workflow-metadata-tools";
import * as currentWorkflowEditingObservingTools from "./tool/current-workflow-editing-observing-tools";
import * as currentWorkflowExecutionTools from "./tool/current-workflow-execution-tools";
import * as agentActionTools from "./tool/agent-action-tools";
import * as baselineTools from "./tool/baseline-tools";
import * as operatorTools from "./tool/operator-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import {
  AssistantModelMessage,
  generateText,
  type ModelMessage,
  stepCountIs,
  UIMessage,
  UserModelMessage,
  LanguageModelUsage,
} from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { getCopilotSystemPrompt, PLANNING_MODE_PROMPT, BASELINE_SYSTEM_PROMPT } from "./copilot-prompts";
import { getAllowedOperatorSchemasAsJson } from "./tool/workflow-metadata-tools";
import { AgentActionService } from "../agent-action/agent-action.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../workflow-console/workflow-console.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { TOOL_NAME_GET_CURRENT_WORKFLOW } from "./tool/current-workflow-editing-observing-tools";
import { parseOperatorAccessFromStep, ToolOperatorAccess } from "./tool/react-step-operator-parser";

/**
 * Copilot state enum.
 */
export enum CopilotState {
  UNAVAILABLE = "Unavailable",
  AVAILABLE = "Available",
  GENERATING = "Generating",
  STOPPING = "Stopping",
}

/**
 * ReActStep - Represents a single reasoning and acting step in the agent's response.
 * Each step contains the agent's reasoning text, tool calls, results, and metadata.
 */
export interface ReActStep {
  messageId: string;
  stepId: number;
  timestamp: Date;
  role: "user" | "agent";
  content: string;
  isBegin: boolean;
  isEnd: boolean;
  toolCalls?: any[];
  toolResults?: any[];
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    cachedInputTokens?: number;
  };
  // Map from tool call index to operator access information
  operatorAccess?: Map<number, ToolOperatorAccess>;
}

/**
 * Statistics for a single message request.
 */
export interface CopilotMessageStats {
  messageId: string;
  userMessage: string;
  startTime: Date;
  endTime?: Date;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalTokens: number;
  cachedInputTokens: number;
  stepCount: number;
  status: "running" | "completed" | "error" | "stopped";
  errorMessage?: string;
}

/**
 * Texera Copilot - An AI assistant for workflow manipulation.
 * Uses Vercel AI SDK for chat completion.
 * Note: Not a singleton - each agent has its own instance.
 */
@Injectable()
export class TexeraCopilot {
  // Maximum number of retries for failed message attempts
  private static readonly MAX_RETRY_COUNT = 3;

  private model: any;
  private modelType: string;
  private agentId: string = "";
  private agentName: string = "";
  private messages: ModelMessage[] = [];
  // Array of ReActStep, appended based on timestamp
  private reActSteps: ReActStep[] = [];
  private reActStepsSubject = new BehaviorSubject<ReActStep[]>([]);
  public reActSteps$ = this.reActStepsSubject.asObservable();
  private state: CopilotState = CopilotState.UNAVAILABLE;
  private stateSubject = new BehaviorSubject<CopilotState>(CopilotState.UNAVAILABLE);
  public state$ = this.stateSubject.asObservable();
  private shouldStopAfterAgentAction: boolean = false;
  private planningMode: boolean = false;
  private baselineMode: boolean = false;
  private relevantOperators: string[] = [];
  private relevantOperatorsSubject = new BehaviorSubject<string[]>([]);
  public relevantOperators$ = this.relevantOperatorsSubject.asObservable();
  private messageStatsMap: Map<string, CopilotMessageStats> = new Map();
  private messageStatsSubject = new BehaviorSubject<Map<string, CopilotMessageStats>>(new Map());
  public messageStats$ = this.messageStatsSubject.asObservable();
  private messageIdCounter: number = 0;
  // Track which message is being hovered and its operator IDs
  private hoveredMessageOperatorsSubject = new BehaviorSubject<{
    viewedOperatorIds: string[];
    modifiedOperatorIds: string[];
  }>({ viewedOperatorIds: [], modifiedOperatorIds: [] });
  public hoveredMessageOperators$ = this.hoveredMessageOperatorsSubject.asObservable();
  // Agent action approval blocking state
  private agentActionApprovalSubject = new BehaviorSubject<{
    isWaitingForApproval: boolean;
    agentActionId?: string;
  }>({ isWaitingForApproval: false });
  public agentActionApproval$ = this.agentActionApprovalSubject.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private workflowCompilingService: WorkflowCompilingService,
    private validationWorkflowService: ValidationWorkflowService,
    private agentActionService: AgentActionService,
    private notificationService: NotificationService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private workflowStatusService: WorkflowStatusService,
    private workflowPersistService: WorkflowPersistService,
    private workflowVersionService: WorkflowVersionService
  ) {
    this.modelType = "";
  }

  public setAgentInfo(agentId: string, agentName: string): void {
    this.agentId = agentId;
    this.agentName = agentName;
  }

  public setModelType(modelType: string): void {
    this.modelType = modelType;
  }

  public setPlanningMode(planningMode: boolean): void {
    this.planningMode = planningMode;
  }

  public getPlanningMode(): boolean {
    return this.planningMode;
  }

  public setBaselineMode(baselineMode: boolean): void {
    this.baselineMode = baselineMode;
  }

  public getBaselineMode(): boolean {
    return this.baselineMode;
  }

  /**
   * Get the copilot system prompt with operator schemas embedded.
   * For non-baseline mode, fetches operator schemas and embeds them in the prompt.
   */
  private getCopilotPromptWithSchemas(): string {
    const operatorSchemasJson = getAllowedOperatorSchemasAsJson(this.operatorMetadataService);
    return getCopilotSystemPrompt(operatorSchemasJson);
  }

  /**
   * Update the state and emit to the observable.
   */
  private setState(newState: CopilotState): void {
    this.state = newState;
    this.stateSubject.next(newState);
  }

  /**
   * Initialize the copilot with the AI model.
   * Returns an Observable that completes when initialization is done.
   */
  public initialize(): Observable<void> {
    return defer(() => {
      try {
        this.model = createOpenAI({
          baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
          apiKey: "dummy",
        }).chat(this.modelType);

        this.setState(CopilotState.AVAILABLE);
        return of(undefined);
      } catch (error: unknown) {
        this.setState(CopilotState.UNAVAILABLE);
        return throwError(() => error);
      }
    });
  }

  public sendMessage(message: string): Observable<void> {
    return defer(async () => {
      // Validation
      if (!this.model) {
        throw new Error("Copilot not initialized");
      }
      if (this.state !== CopilotState.AVAILABLE) {
        throw new Error(`Cannot send message: agent is ${this.state}`);
      }

      // Clear agent action approval state when any message is sent
      this.agentActionApprovalSubject.next({ isWaitingForApproval: false });

      // Set state to generating once at the start
      this.setState(CopilotState.GENERATING);
      this.shouldStopAfterAgentAction = false;

      // Determine the system prompt based on mode
      let systemPrompt: string;
      if (this.baselineMode) {
        systemPrompt = BASELINE_SYSTEM_PROMPT;
      } else if (this.planningMode) {
        systemPrompt = this.getCopilotPromptWithSchemas() + "\n\n" + PLANNING_MODE_PROMPT;
      } else {
        systemPrompt = this.getCopilotPromptWithSchemas();
      }

      let lastError: unknown = null;

      // Retry loop - each attempt is independent with a fresh start
      for (let attempt = 1; attempt <= TexeraCopilot.MAX_RETRY_COUNT + 1; attempt++) {
        // Generate unique message ID for this attempt
        const messageId = `msg-${this.agentId}-${++this.messageIdCounter}-${Date.now()}`;

        // Initialize message stats for this attempt
        const messageStats: CopilotMessageStats = {
          messageId,
          userMessage: message,
          startTime: new Date(),
          totalInputTokens: 0,
          totalOutputTokens: 0,
          totalTokens: 0,
          cachedInputTokens: 0,
          stepCount: 0,
          status: "running",
        };
        this.messageStatsMap.set(messageId, messageStats);
        this.messageStatsSubject.next(new Map(this.messageStatsMap));

        // Add user message
        const userMessage: UserModelMessage = { role: "user", content: message };
        this.messages.push(userMessage);
        const userUIMessage: ReActStep = {
          messageId: messageId,
          stepId: 0, // User message is always step 0
          timestamp: new Date(),
          role: "user",
          content: message,
          isBegin: true,
          isEnd: true,
        };
        this.reActSteps.push(userUIMessage);
        this.reActStepsSubject.next([...this.reActSteps]);

        const tools = this.baselineMode ? this.createBaselineTools() : this.createWorkflowTools();
        let isFirstStep = true;
        let stepIndex = 0;
        let wasStopped = false;

        try {
          // Generate text using AI
          const result = await generateText({
            model: this.model,
            messages: this.messages,
            tools,
            system: systemPrompt,
            experimental_repairToolCall: async ({ toolCall, error }) => {
              // Log the malformed tool call for debugging
              console.warn(
                `[Copilot] Malformed tool call for "${toolCall.toolName}":`,
                error.message,
                "\nRaw input:",
                toolCall.input
              );

              // Try to repair common JSON escaping issues
              try {
                const rawInput = toolCall.input;
                // Attempt 1: Try parsing as-is (might just be a minor issue)
                try {
                  const parsed = JSON.parse(rawInput);
                  // Return repaired tool call with same structure
                  return { ...toolCall, input: JSON.stringify(parsed) };
                } catch {
                  // Continue to repair attempts
                }

                // Attempt 2: Fix double-escaped strings (common LLM issue)
                // Replace escaped newlines and quotes that are incorrectly double-escaped
                let repaired = rawInput
                  .replace(/\\\\n/g, "\\n") // \\n -> \n
                  .replace(/\\\\"/g, '\\"') // \\" -> \"
                  .replace(/\\\\t/g, "\\t"); // \\t -> \t

                const parsed = JSON.parse(repaired);
                console.info(`[Copilot] Successfully repaired tool call for "${toolCall.toolName}"`);
                return { ...toolCall, input: JSON.stringify(parsed) };
              } catch (repairError) {
                // If repair fails, skip this tool call by returning null
                console.error(
                  `[Copilot] Failed to repair tool call for "${toolCall.toolName}", skipping:`,
                  repairError
                );
                return null;
              }
            },
            stopWhen: ({ steps }) => {
              if (this.state === CopilotState.STOPPING) {
                wasStopped = true;
                this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
                return true;
              }
              if (this.shouldStopAfterAgentAction) {
                return true;
              }
              return stepCountIs(500)({ steps });
            },
            onStepFinish: ({ text, toolCalls, toolResults, usage }) => {
              if (this.state === CopilotState.STOPPING) {
                return;
              }

              // Update step count
              const stats = this.messageStatsMap.get(messageId);
              if (stats) {
                stats.stepCount++;
                this.messageStatsMap.set(messageId, stats);
                this.messageStatsSubject.next(new Map(this.messageStatsMap));
              }

              // Check if planning mode is on and there's any workflow action tool call
              if (this.planningMode && toolCalls && toolResults) {
                const workflowActionToolNames = [
                  agentActionTools.TOOL_NAME_ADD_TO_WORKFLOW,
                  agentActionTools.TOOL_NAME_MODIFY_IN_WORKFLOW,
                  agentActionTools.TOOL_NAME_DELETE_FROM_WORKFLOW,
                ];
                const agentActionCallIndex = toolCalls.findIndex(call =>
                  workflowActionToolNames.includes(call.toolName)
                );

                if (agentActionCallIndex !== -1) {
                  // Extract agent action ID from the result
                  const agentActionResult = toolResults[agentActionCallIndex];
                  const agentActionId = agentActionResult?.result?.agentActionId;

                  // Stop generation after this step to wait for user approval
                  this.shouldStopAfterAgentAction = true;

                  // Start pending preview in the agent action service
                  if (agentActionId) {
                    this.agentActionService.startPendingPreview(agentActionId);
                  }
                }
              }

              // Parse operator access (READ/WRITE) from tool calls and results
              let operatorAccess: Map<number, ToolOperatorAccess> | undefined;
              if (toolCalls && toolResults) {
                operatorAccess = parseOperatorAccessFromStep(toolCalls, toolResults);
              }

              stepIndex++; // Increment first since user message is step 0
              const stepResponse: ReActStep = {
                messageId: messageId,
                stepId: stepIndex,
                timestamp: new Date(),
                role: "agent",
                content: text || "",
                isBegin: isFirstStep,
                isEnd: false,
                toolCalls: toolCalls,
                toolResults: toolResults,
                usage: usage as any,
                operatorAccess: operatorAccess,
              };

              // Add to reActSteps array
              this.reActSteps.push(stepResponse);
              this.reActStepsSubject.next([...this.reActSteps]);

              isFirstStep = false;
            },
          });

          // Success! Process the result
          this.messages.push(...result.response.messages);
          this.reActStepsSubject.next([...this.reActSteps]);

          // Update final stats for completion with final usage
          const stats = this.messageStatsMap.get(messageId);
          if (stats) {
            stats.endTime = new Date();
            stats.status = wasStopped ? "stopped" : "completed";
            // Use the final usage from generateText result
            if (result.usage) {
              stats.totalInputTokens = result.usage.inputTokens || 0;
              stats.totalOutputTokens = result.usage.outputTokens || 0;
              stats.totalTokens = result.usage.totalTokens || 0;
              stats.cachedInputTokens = result.usage.cachedInputTokens || 0;
            }
            this.messageStatsMap.set(messageId, stats);
            this.messageStatsSubject.next(new Map(this.messageStatsMap));
          }

          // Success - return from the async function
          return;
        } catch (err) {
          lastError = err;
          const errorText = `Error: ${err instanceof Error ? err.message : String(err)}`;
          // this.notificationService.info(errorText);

          // Clean up the failed attempt
          this.messageStatsMap.delete(messageId);
          this.messageStatsSubject.next(new Map(this.messageStatsMap));

          // Clear message history for fresh start
          this.clearMessages();

          // If this was not the last attempt, retry
          if (attempt < TexeraCopilot.MAX_RETRY_COUNT + 1) {
            const retryMessage = `Retrying message (attempt ${attempt + 1}/${TexeraCopilot.MAX_RETRY_COUNT + 1}) after error: ${errorText}`;
            console.warn(retryMessage);
            // this.notificationService.info(retryMessage);
          }
        }
      }

      // If we get here, all retries failed
      // Generate a final messageId for the error
      const errorMessageId = `msg-${this.agentId}-${++this.messageIdCounter}-${Date.now()}`;

      // Add error message to UI
      const errorText = `Error: ${lastError instanceof Error ? lastError.message : String(lastError)}`;
      const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
      this.messages.push(assistantError);

      const errorResponse: ReActStep = {
        messageId: errorMessageId,
        stepId: 1,
        timestamp: new Date(),
        role: "agent",
        content: errorText,
        isBegin: false,
        isEnd: true,
      };
      this.reActSteps.push(errorResponse);
      this.reActStepsSubject.next([...this.reActSteps]);

      // Add stats for the final failed attempt
      const failedStats: CopilotMessageStats = {
        messageId: errorMessageId,
        userMessage: message,
        startTime: new Date(),
        endTime: new Date(),
        totalInputTokens: 0,
        totalOutputTokens: 0,
        totalTokens: 0,
        cachedInputTokens: 0,
        stepCount: 0,
        status: "error",
        errorMessage: `Failed after ${TexeraCopilot.MAX_RETRY_COUNT + 1} attempts: ${errorText}`,
      };
      this.messageStatsMap.set(errorMessageId, failedStats);
      this.messageStatsSubject.next(new Map(this.messageStatsMap));

      // Throw the error
      throw lastError;
    }).pipe(
      finalize(() => {
        // Always set state to AVAILABLE when done (success or failure)
        this.setState(CopilotState.AVAILABLE);
      })
    );
  }

  /**
   * Create workflow manipulation tools with timeout protection.
   */
  private createWorkflowTools(): Record<string, any> {
    // Current workflow editing and observing tools - merged into single getCurrentWorkflow tool
    const getCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createGetCurrentWorkflowTool(
        this.workflowActionService,
        this.workflowCompilingService
      )
    );

    // Workflow execution tools
    const executeCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowExecutionTools.createExecuteCurrentWorkflowTool(
        this.executeWorkflowService,
        this.validationWorkflowService,
        this.workflowActionService,
        this.workflowConsoleService,
        this.workflowStatusService,
        this.workflowResultService
      )
    );
    const getExistingWorkflowExecutionResultTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetExistingWorkflowExecutionResultTool(
        this.workflowResultService,
        this.workflowConsoleService,
        this.workflowActionService
      )
    );
    const getCurrentComputingUnitStatusTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetCurrentComputingUnitStatusTool(this.computingUnitStatusService)
    );
    // Operator-specific tools
    const addPythonUDFV2Tool = toolWithTimeout(
      operatorTools.createAddPythonUDFV2Tool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addAggregateTool = toolWithTimeout(
      operatorTools.createAddAggregateTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addProjectionTool = toolWithTimeout(
      operatorTools.createAddProjectionTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addHashJoinTool = toolWithTimeout(
      operatorTools.createAddHashJoinTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addSortTool = toolWithTimeout(
      operatorTools.createAddSortTool(this.workflowActionService, this.agentActionService, this.agentId, this.agentName)
    );
    const addUnionTool = toolWithTimeout(
      operatorTools.createAddUnionTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addIntersectTool = toolWithTimeout(
      operatorTools.createAddIntersectTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addCartesianProductTool = toolWithTimeout(
      operatorTools.createAddCartesianProductTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addCSVFileScanTool = toolWithTimeout(
      operatorTools.createAddCSVFileScanTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const addLinkTool = toolWithTimeout(
      operatorTools.createAddLinkTool(this.workflowActionService, this.agentActionService, this.agentId, this.agentName)
    );
    const modifyOperatorTool = toolWithTimeout(
      operatorTools.createModifyOperatorTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );
    const deleteFromWorkflowTool = toolWithTimeout(
      operatorTools.createDeleteFromWorkflowTool(
        this.workflowActionService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );

    // Base tools available in both modes
    const baseTools: Record<string, any> = {
      // Merged workflow observing tool (replaces listCurrentLinks and listCurrentRelevantOperatorIds)
      [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_WORKFLOW]: getCurrentWorkflowTool,

      // Workflow execution tools
      [currentWorkflowExecutionTools.TOOL_NAME_EXECUTE_CURRENT_WORKFLOW]: executeCurrentWorkflowTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_EXISTING_WORKFLOW_EXECUTION_RESULT]:
        getExistingWorkflowExecutionResultTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS]: getCurrentComputingUnitStatusTool,
      // Operator-specific tools
      [operatorTools.TOOL_NAME_ADD_PYTHON_UDF_V2]: addPythonUDFV2Tool,
      [operatorTools.TOOL_NAME_ADD_AGGREGATE]: addAggregateTool,
      [operatorTools.TOOL_NAME_ADD_PROJECTION]: addProjectionTool,
      [operatorTools.TOOL_NAME_ADD_HASH_JOIN]: addHashJoinTool,
      [operatorTools.TOOL_NAME_ADD_SORT]: addSortTool,
      [operatorTools.TOOL_NAME_ADD_UNION]: addUnionTool,
      [operatorTools.TOOL_NAME_ADD_INTERSECT]: addIntersectTool,
      [operatorTools.TOOL_NAME_ADD_CARTESIAN_PRODUCT]: addCartesianProductTool,
      [operatorTools.TOOL_NAME_ADD_CSV_FILE_SCAN]: addCSVFileScanTool,
      [operatorTools.TOOL_NAME_ADD_LINK]: addLinkTool,
      [operatorTools.TOOL_NAME_MODIFY_OPERATOR]: modifyOperatorTool,
      [operatorTools.TOOL_NAME_DELETE_FROM_WORKFLOW]: deleteFromWorkflowTool,
    };

    return baseTools;
  }

  /**
   * Create tools for baseline mode.
   * Baseline mode only has: createPythonUDF and executeToOperator.
   */
  private createBaselineTools(): Record<string, any> {
    // Python UDF creation tool - only creates the operator (no execution)
    const createPythonUDFTool = toolWithTimeout(
      baselineTools.createPythonUDFTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService,
        this.agentActionService,
        this.agentId,
        this.agentName
      )
    );

    // Execute to operator tool - executes workflow up to a specific operator
    const executeToOperatorTool = toolWithTimeout(
      baselineTools.createExecuteToOperatorTool(
        this.executeWorkflowService,
        this.validationWorkflowService,
        this.workflowActionService,
        this.workflowConsoleService,
        this.workflowStatusService,
        this.workflowResultService
      )
    );

    return {
      // Baseline mode primary tools
      [baselineTools.TOOL_NAME_CREATE_PYTHON_UDF]: createPythonUDFTool,
      [baselineTools.TOOL_NAME_EXECUTE_TO_OPERATOR]: executeToOperatorTool,
    };
  }

  public getReActSteps(): ReActStep[] {
    return [...this.reActSteps];
  }

  /**
   * Get a specific ReAct step by messageId and stepId
   */
  public getReActStepById(messageId: string, stepId: number): ReActStep | undefined {
    return this.reActSteps.find(step => step.messageId === messageId && step.stepId === stepId);
  }

  /**
   * Get all ReAct steps for a specific message
   */
  public getReActStepsByMessageId(messageId: string): ReActStep[] {
    return this.reActSteps.filter(step => step.messageId === messageId);
  }

  public stopGeneration(): void {
    if (this.state !== CopilotState.GENERATING) {
      return;
    }
    this.setState(CopilotState.STOPPING);
  }

  public clearMessages(): void {
    this.messages = [];
    this.reActSteps = [];
    this.reActStepsSubject.next([]);
    this.relevantOperators = [];
    this.relevantOperatorsSubject.next([]);
    this.messageStatsMap.clear();
    this.messageStatsSubject.next(new Map());
  }

  public getMessageStats(): CopilotMessageStats[] {
    return Array.from(this.messageStatsMap.values());
  }

  public getState(): CopilotState {
    return this.state;
  }

  public getRelevantOperators(): string[] {
    return [...this.relevantOperators];
  }

  /**
   * Set the hovered message and emit its operator IDs.
   * @param step The ReActStep being hovered, or null to clear
   */
  public setHoveredMessage(step: ReActStep | null): void {
    if (!step) {
      this.hoveredMessageOperatorsSubject.next({ viewedOperatorIds: [], modifiedOperatorIds: [] });
      return;
    }

    // Collect all operator IDs from this step's tool calls
    const viewedOperatorIds = new Set<string>();
    const modifiedOperatorIds = new Set<string>();

    if (step.operatorAccess) {
      step.operatorAccess.forEach((access, _) => {
        access.viewedOperatorIds.forEach(id => viewedOperatorIds.add(id));
        access.modifiedOperatorIds.forEach(id => modifiedOperatorIds.add(id));
      });
    }

    this.hoveredMessageOperatorsSubject.next({
      viewedOperatorIds: Array.from(viewedOperatorIds),
      modifiedOperatorIds: Array.from(modifiedOperatorIds),
    });
  }

  public disconnect(): Observable<void> {
    return defer(() => {
      if (this.state === CopilotState.GENERATING) {
        this.stopGeneration();
      }

      this.clearMessages();
      this.setState(CopilotState.UNAVAILABLE);
      this.notificationService.info(`Agent ${this.agentName} is removed successfully`);

      return of(undefined);
    });
  }

  public isConnected(): boolean {
    return this.state !== CopilotState.UNAVAILABLE;
  }

  public getSystemPrompt(): string {
    if (this.baselineMode) {
      return BASELINE_SYSTEM_PROMPT;
    }
    const copilotPrompt = this.getCopilotPromptWithSchemas();
    return this.planningMode ? copilotPrompt + "\n\n" + PLANNING_MODE_PROMPT : copilotPrompt;
  }

  public getToolsInfo(): Array<{ name: string; description: string; inputSchema: any }> {
    const tools = this.baselineMode ? this.createBaselineTools() : this.createWorkflowTools();
    return Object.entries(tools).map(([name, tool]) => ({
      name: name,
      description: tool.description || "No description available",
      inputSchema: tool.parameters || {},
    }));
  }
}
