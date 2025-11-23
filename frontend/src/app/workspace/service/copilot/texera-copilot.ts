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
import * as currentWorkflowValidationTools from "./tool/current-workflow-validation-tools";
import * as currentWorkflowExecutionTools from "./tool/current-workflow-execution-tools";
import * as actionPlanTools from "./tool/action-plan-tools";
import * as dataInconsistencyTools from "./tool/data-inconsistency-tools";
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
import { COPILOT_SYSTEM_PROMPT, PLANNING_MODE_PROMPT } from "./copilot-prompts";
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
import { ActionPlanService } from "../action-plan/action-plan.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../workflow-console/workflow-console.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS } from "./tool/current-workflow-editing-observing-tools";
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
  private shouldStopAfterActionPlan: boolean = false;
  private planningMode: boolean = false;
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
  // Action plan approval blocking state
  private actionPlanApprovalSubject = new BehaviorSubject<{
    isWaitingForApproval: boolean;
    actionPlanId?: string;
  }>({ isWaitingForApproval: false });
  public actionPlanApproval$ = this.actionPlanApprovalSubject.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private workflowCompilingService: WorkflowCompilingService,
    private validationWorkflowService: ValidationWorkflowService,
    private dataInconsistencyService: DataInconsistencyService,
    private actionPlanService: ActionPlanService,
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

      // Set state to generating once at the start
      this.setState(CopilotState.GENERATING);
      this.shouldStopAfterActionPlan = false;

      const systemPrompt = this.planningMode
        ? COPILOT_SYSTEM_PROMPT + "\n\n" + PLANNING_MODE_PROMPT
        : COPILOT_SYSTEM_PROMPT;

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

        const tools = this.createWorkflowTools();
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
            stopWhen: ({ steps }) => {
              if (this.state === CopilotState.STOPPING) {
                wasStopped = true;
                this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
                return true;
              }
              if (this.shouldStopAfterActionPlan) {
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

              // Check if planning mode is on and there's an actionPlan tool call
              if (this.planningMode && toolCalls && toolResults) {
                const actionPlanCallIndex = toolCalls.findIndex(
                  call => call.toolName === actionPlanTools.TOOL_NAME_ACTION_PLAN
                );

                if (actionPlanCallIndex !== -1) {
                  // Extract action plan ID from the result
                  const actionPlanResult = toolResults[actionPlanCallIndex];
                  const actionPlanId = actionPlanResult?.result?.actionPlanId;

                  // Stop generation after this step to wait for user approval
                  this.shouldStopAfterActionPlan = true;

                  // Emit blocking state
                  this.actionPlanApprovalSubject.next({
                    isWaitingForApproval: true,
                    actionPlanId: actionPlanId,
                  });
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
    // Workflow metadata tools
    const listAllOperatorTypesTool = toolWithTimeout(
      workflowMetadataTools.createListAllOperatorTypesTool(this.workflowUtilService)
    );
    const getOperatorPropertiesSchemaTool = toolWithTimeout(
      workflowMetadataTools.createGetOperatorPropertiesSchemaTool(this.operatorMetadataService)
    );
    const getOperatorPortsInfoTool = toolWithTimeout(
      workflowMetadataTools.createGetOperatorPortsInfoTool(this.operatorMetadataService)
    );
    const getOperatorMetadataTool = toolWithTimeout(
      workflowMetadataTools.createGetOperatorMetadataTool(this.operatorMetadataService)
    );

    // Current workflow editing and observing tools
    const listCurrentLinksTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createListCurrentLinksTool(this.workflowActionService)
    );
    const listOperatorsInCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createListOperatorsInCurrentWorkflowTool(this.workflowActionService)
    );
    const getCurrentOperatorTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createGetCurrentOperatorTool(
        this.workflowActionService,
        this.workflowCompilingService
      )
    );
    const listCurrentRelevantOperatorIdsTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createListCurrentRelevantOperatorIdsTool(
        this.workflowActionService,
        this.workflowCompilingService
      )
    );
    const getCurrentWorkflowCompilationStateTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createGetCurrentWorkflowCompilationStateTool(this.workflowCompilingService)
    );
    const addOperatorToCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createAddOperatorToCurrentWorkflowTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService
      )
    );
    const addLinkToCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createAddLinkToCurrentWorkflowTool(this.workflowActionService)
    );
    const deleteOperatorInCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createDeleteOperatorInCurrentWorkflowTool(this.workflowActionService)
    );
    const deleteLinkInCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createDeleteLinkInCurrentWorkflowTool(this.workflowActionService)
    );
    const setOperatorPropertyInCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createSetOperatorPropertyInCurrentWorkflowTool(
        this.workflowActionService,
        this.validationWorkflowService
      )
    );
    const setPortPropertyInCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowEditingObservingTools.createSetPortPropertyInCurrentWorkflowTool(
        this.workflowActionService,
        this.validationWorkflowService
      )
    );

    // Workflow validation tools
    const getCurrentWorkflowValidationInfoTool = toolWithTimeout(
      currentWorkflowValidationTools.createGetCurrentWorkflowValidationInfoTool(
        this.validationWorkflowService,
        this.workflowActionService
      )
    );
    const validateCurrentOperatorTool = toolWithTimeout(
      currentWorkflowValidationTools.createValidateCurrentOperatorTool(this.validationWorkflowService)
    );

    // Workflow execution tools
    const executeCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowExecutionTools.createExecuteCurrentWorkflowTool(this.executeWorkflowService)
    );
    const getCurrentExecutionStateTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetCurrentExecutionStateTool(
        this.executeWorkflowService,
        this.workflowActionService,
        this.workflowConsoleService,
        this.workflowStatusService
      )
    );
    const killCurrentWorkflowTool = toolWithTimeout(
      currentWorkflowExecutionTools.createKillCurrentWorkflowTool(this.executeWorkflowService)
    );
    const hasCurrentOperatorResultTool = toolWithTimeout(
      currentWorkflowExecutionTools.createHasCurrentOperatorResultTool(
        this.workflowResultService,
        this.workflowActionService
      )
    );
    const getCurrentOperatorResultTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetCurrentOperatorResultTool(this.workflowResultService)
    );
    const getCurrentOperatorResultInfoTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetCurrentOperatorResultInfoTool(
        this.workflowResultService,
        this.workflowActionService
      )
    );
    const getCurrentComputingUnitStatusTool = toolWithTimeout(
      currentWorkflowExecutionTools.createGetCurrentComputingUnitStatusTool(this.computingUnitStatusService)
    );

    // Data inconsistency tools
    const addInconsistencyTool = toolWithTimeout(
      dataInconsistencyTools.createAddInconsistencyTool(this.dataInconsistencyService)
    );
    const listInconsistenciesTool = toolWithTimeout(
      dataInconsistencyTools.createListInconsistenciesTool(this.dataInconsistencyService)
    );
    const updateInconsistencyTool = toolWithTimeout(
      dataInconsistencyTools.createUpdateInconsistencyTool(this.dataInconsistencyService)
    );
    const deleteInconsistencyTool = toolWithTimeout(
      dataInconsistencyTools.createDeleteInconsistencyTool(this.dataInconsistencyService)
    );
    const clearInconsistenciesTool = toolWithTimeout(
      dataInconsistencyTools.createClearInconsistenciesTool(this.dataInconsistencyService)
    );

    // Action plan tools (for planning mode)
    // Inject WorkflowActionService into ActionPlanService for revert functionality
    this.actionPlanService.setWorkflowActionService(this.workflowActionService);

    const actionPlanTool = toolWithTimeout(
      actionPlanTools.createActionPlanTool(
        this.workflowActionService,
        this.actionPlanService,
        this.validationWorkflowService,
        this.workflowVersionService,
        this.agentId,
        this.agentName
      )
    );
    const getActionPlanTool = toolWithTimeout(actionPlanTools.createGetActionPlanTool(this.actionPlanService));
    const listActionPlansTool = toolWithTimeout(actionPlanTools.createListActionPlansTool(this.actionPlanService));
    const deleteActionPlanTool = toolWithTimeout(actionPlanTools.createDeleteActionPlanTool(this.actionPlanService));
    const updateActionPlanTool = toolWithTimeout(actionPlanTools.createUpdateActionPlanTool(this.actionPlanService));

    // Base tools available in both modes
    const baseTools: Record<string, any> = {
      // meta level knowledge
      [workflowMetadataTools.TOOL_NAME_LIST_ALL_OPERATOR_TYPES]: listAllOperatorTypesTool,
      [workflowMetadataTools.TOOL_NAME_GET_OPERATOR_PROPERTIES_SCHEMA]: getOperatorPropertiesSchemaTool,
      [workflowMetadataTools.TOOL_NAME_GET_OPERATOR_PORTS_INFO]: getOperatorPortsInfoTool,
      [workflowMetadataTools.TOOL_NAME_GET_OPERATOR_METADATA]: getOperatorMetadataTool,
      // current workflow editing - TEMPORARILY COMMENTED OUT
      // [currentWorkflowEditingObservingTools.TOOL_NAME_ADD_OPERATOR_TO_CURRENT_WORKFLOW]:
      //   addOperatorToCurrentWorkflowTool,
      // [currentWorkflowEditingObservingTools.TOOL_NAME_ADD_LINK_TO_CURRENT_WORKFLOW]: addLinkToCurrentWorkflowTool,
      // [currentWorkflowEditingObservingTools.TOOL_NAME_DELETE_OPERATOR_IN_CURRENT_WORKFLOW]:
      //   deleteOperatorInCurrentWorkflowTool,
      // [currentWorkflowEditingObservingTools.TOOL_NAME_DELETE_LINK_IN_CURRENT_WORKFLOW]: deleteLinkInCurrentWorkflowTool,
      // [currentWorkflowEditingObservingTools.TOOL_NAME_SET_OPERATOR_PROPERTY_IN_CURRENT_WORKFLOW]:
      //   setOperatorPropertyInCurrentWorkflowTool,
      // [currentWorkflowEditingObservingTools.TOOL_NAME_SET_PORT_PROPERTY_IN_CURRENT_WORKFLOW]:
      //   setPortPropertyInCurrentWorkflowTool,
      // current workflow validation
      [currentWorkflowValidationTools.TOOL_NAME_GET_CURRENT_WORKFLOW_VALIDATION_INFO]:
        getCurrentWorkflowValidationInfoTool,
      [currentWorkflowValidationTools.TOOL_NAME_VALIDATE_CURRENT_OPERATOR]: validateCurrentOperatorTool,
      // current workflow inspecting
      [currentWorkflowEditingObservingTools.TOOL_NAME_LIST_CURRENT_RELEVANT_OPERATOR_IDS]:
        listCurrentRelevantOperatorIdsTool,
      [currentWorkflowEditingObservingTools.TOOL_NAME_LIST_CURRENT_LINKS]: listCurrentLinksTool,
      [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_OPERATOR]: getCurrentOperatorTool,
      [currentWorkflowEditingObservingTools.TOOL_NAME_GET_CURRENT_WORKFLOW_COMPILATION_STATE]:
        getCurrentWorkflowCompilationStateTool,
      // current workflow execution
      [currentWorkflowExecutionTools.TOOL_NAME_EXECUTE_CURRENT_WORKFLOW]: executeCurrentWorkflowTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_EXECUTION_STATE]: getCurrentExecutionStateTool,
      [currentWorkflowExecutionTools.TOOL_NAME_KILL_CURRENT_WORKFLOW]: killCurrentWorkflowTool,
      [currentWorkflowExecutionTools.TOOL_NAME_HAS_CURRENT_OPERATOR_RESULT]: hasCurrentOperatorResultTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_OPERATOR_RESULT]: getCurrentOperatorResultTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_OPERATOR_RESULT_INFO]: getCurrentOperatorResultInfoTool,
      [currentWorkflowExecutionTools.TOOL_NAME_GET_CURRENT_COMPUTING_UNIT_STATUS]: getCurrentComputingUnitStatusTool,
      // Data inconsistency tools
      [dataInconsistencyTools.TOOL_NAME_ADD_INCONSISTENCY]: addInconsistencyTool,
      [dataInconsistencyTools.TOOL_NAME_LIST_INCONSISTENCIES]: listInconsistenciesTool,
      [dataInconsistencyTools.TOOL_NAME_UPDATE_INCONSISTENCY]: updateInconsistencyTool,
      [dataInconsistencyTools.TOOL_NAME_DELETE_INCONSISTENCY]: deleteInconsistencyTool,
      [dataInconsistencyTools.TOOL_NAME_CLEAR_INCONSISTENCIES]: clearInconsistenciesTool,
      // Action plan tools - always available
      [actionPlanTools.TOOL_NAME_ACTION_PLAN]: actionPlanTool,
      // [actionPlanTools.TOOL_NAME_GET_ACTION_PLAN]: getActionPlanTool,
      // [actionPlanTools.TOOL_NAME_LIST_ACTION_PLANS]: listActionPlansTool,
      // [actionPlanTools.TOOL_NAME_DELETE_ACTION_PLAN]: deleteActionPlanTool,
      // [actionPlanTools.TOOL_NAME_UPDATE_ACTION_PLAN]: updateActionPlanTool,
    };

    return baseTools;
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
    return this.planningMode ? COPILOT_SYSTEM_PROMPT + "\n\n" + PLANNING_MODE_PROMPT : COPILOT_SYSTEM_PROMPT;
  }

  public getToolsInfo(): Array<{ name: string; description: string; inputSchema: any }> {
    const tools = this.createWorkflowTools();
    return Object.entries(tools).map(([name, tool]) => ({
      name: name,
      description: tool.description || "No description available",
      inputSchema: tool.parameters || {},
    }));
  }

  /**
   * Approve the pending action plan and continue with execution
   */
  public approveActionPlan(feedback?: string): void {
    // STEP 1: Clean up version display UI if in planning mode (but keep the workflow state)
    if (this.planningMode && this.workflowVersionService) {
      console.log("[Planning Mode] Cleaning up version display UI (approved)");
      // Clear all version display highlights and state
      // Do NOT reload the workflow - we want to keep the current state with action plan applied
      const allOperators = this.workflowActionService.getTexeraGraph().getAllOperators();
      const allOperatorIds = allOperators.map(op => op.operatorID);

      // Clear highlights on all operators
      this.workflowVersionService.unhighlightOpVersionDiff({
        modified: allOperatorIds,
        added: [],
        deleted: [],
      });
      this.workflowVersionService.setDisplayParticularVersion(false);
      // Clear the temp workflow without reloading
      this.workflowActionService.resetTempWorkflow();
      // Re-enable workflow modification
      this.workflowActionService.enableWorkflowModification();
    }

    // STEP 2: Construct the approval message
    const message = feedback
      ? `I approve this action plan. Additional feedback: ${feedback}`
      : "I approve this action plan. Please proceed with execution.";

    // STEP 3: Clear the approval state - UI will update immediately
    this.actionPlanApprovalSubject.next({ isWaitingForApproval: false });

    // STEP 4: Continue the conversation with the approval message
    // sendMessage() will handle adding the message to history
    this.sendMessage(message).subscribe();
  }

  /**
   * Reject the pending action plan and revert changes
   */
  public rejectActionPlan(feedback?: string, actionPlanId?: string): void {
    // STEP 1: Revert the action plan changes FIRST (while workflow is still in modified state)
    if (actionPlanId) {
      console.log(`Reverting action plan ${actionPlanId}...`);
      const success = this.actionPlanService.revertActionPlan(actionPlanId);
      if (success) {
        console.log(`Successfully reverted action plan ${actionPlanId}`);
      } else {
        console.error(`Failed to revert action plan ${actionPlanId}`);
      }
    } else {
      console.warn("No action plan ID provided for rejection");
    }

    // STEP 3: Construct the rejection message
    const message = feedback
      ? `I reject this action plan. Reason: ${feedback}`
      : "I reject this action plan. Please revise your approach.";

    // STEP 4: Clear the approval state - UI will update immediately
    this.actionPlanApprovalSubject.next({ isWaitingForApproval: false });

    // STEP 5: Continue the conversation with the rejection message
    // sendMessage() will handle adding the message to history
    this.sendMessage(message).subscribe();
  }
}
