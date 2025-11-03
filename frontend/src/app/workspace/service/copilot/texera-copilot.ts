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
import { BehaviorSubject, Observable, from } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  createAddOperatorTool,
  createAddLinkTool,
  createActionPlanTool,
  createUpdateActionPlanProgressTool,
  createGetActionPlanTool,
  createListActionPlansTool,
  createDeleteActionPlanTool,
  createUpdateActionPlanTool,
  createGetOperatorTool,
  createDeleteOperatorTool,
  createDeleteLinkTool,
  createSetOperatorPropertyTool,
  createSetPortPropertyTool,
  createGetOperatorSchemaTool,
  createGetOperatorPropertiesSchemaTool,
  createGetOperatorPortsInfoTool,
  createGetOperatorMetadataTool,
  createGetOperatorInputSchemaTool,
  createGetWorkflowCompilationStateTool,
  createExecuteWorkflowTool,
  createGetExecutionStateTool,
  createKillWorkflowTool,
  createHasOperatorResultTool,
  createGetOperatorResultTool,
  createGetOperatorResultInfoTool,
  createGetValidationInfoOfCurrentWorkflowTool,
  createValidateOperatorTool,
  toolWithTimeout,
  createListAllOperatorTypesTool,
  createListLinksTool,
  createListOperatorIdsTool,
} from "./workflow-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import { AssistantModelMessage, generateText, type ModelMessage, stepCountIs, UIMessage, UserModelMessage } from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { COPILOT_SYSTEM_PROMPT, PLANNING_MODE_PROMPT } from "./copilot-prompts";
import { ActionPlanService } from "../action-plan/action-plan.service";
import { NotificationService } from "../../../common/service/notification/notification.service";

// API endpoints as constants
export const DEFAULT_AGENT_MODEL_ID = "claude-3.7";

/**
 * Copilot state enum
 */
export enum CopilotState {
  UNAVAILABLE = "Unavailable",
  AVAILABLE = "Available",
  GENERATING = "Generating",
  STOPPING = "Stopping",
}

/**
 * Agent response for UI display
 * Represents a step or final response from the agent
 */
export interface AgentUIMessage {
  role: "user" | "agent";
  content: string;
  isBegin: boolean;
  isEnd: boolean;
  // Raw data for subscribers to process
  toolCalls?: any[];
  toolResults?: any[];
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    cachedInputTokens?: number;
  };
}

/**
 * Texera Copilot - An AI assistant for workflow manipulation
 * Uses Vercel AI SDK for chat completion and MCP SDK for tool discovery
 *
 * Note: Not a singleton - each agent has its own instance
 */
@Injectable()
export class TexeraCopilot {
  private model: any;
  private modelType: string;

  // Agent identification
  private agentId: string = "";
  private agentName: string = "";

  // PRIVATE message history for AI conversation (not exposed to UI)
  private messages: ModelMessage[] = [];

  // PUBLIC agent responses for UI display
  private agentResponses: AgentUIMessage[] = [];
  private agentResponsesSubject = new BehaviorSubject<AgentUIMessage[]>([]);
  public agentResponses$ = this.agentResponsesSubject.asObservable();

  // Copilot state management
  private state: CopilotState = CopilotState.UNAVAILABLE;

  // Flag to stop generation after action plan is created
  private shouldStopAfterActionPlan: boolean = false;

  // Planning mode - controls which tools are available
  private planningMode: boolean = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private dynamicSchemaService: DynamicSchemaService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private workflowCompilingService: WorkflowCompilingService,
    private validationWorkflowService: ValidationWorkflowService,
    private actionPlanService: ActionPlanService,
    private notificationService: NotificationService
  ) {
    // Default model type
    this.modelType = DEFAULT_AGENT_MODEL_ID;
  }

  /**
   * Set the agent identification
   */
  public setAgentInfo(agentId: string, agentName: string): void {
    this.agentId = agentId;
    this.agentName = agentName;
  }

  /**
   * Set the model type for this agent
   */
  public setModelType(modelType: string): void {
    this.modelType = modelType;
  }

  /**
   * Set the planning mode for this agent
   */
  public setPlanningMode(planningMode: boolean): void {
    this.planningMode = planningMode;
    console.log(`[${this.agentId}] Planning mode set to: ${planningMode}`);
  }

  /**
   * Get the current planning mode
   */
  public getPlanningMode(): boolean {
    return this.planningMode;
  }

  /**
   * Initialize the copilot with MCP and AI model
   */
  public async initialize(): Promise<void> {
    try {
      // Initialize OpenAI model with the configured model type
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy",
      }).chat(this.modelType);

      // Set state to Available
      this.state = CopilotState.AVAILABLE;

      console.log("Texera Copilot initialized successfully");
    } catch (error: unknown) {
      console.error("Failed to initialize copilot:", error);
      this.state = CopilotState.UNAVAILABLE;
      throw error;
    }
  }

  public sendMessage(message: string): Observable<void> {
    return from(
      (async () => {
        if (!this.model) {
          throw new Error("Copilot not initialized");
        }

        // Set state to Generating
        this.state = CopilotState.GENERATING;

        // Reset action plan stop flag for this generation
        this.shouldStopAfterActionPlan = false;

        // 1) push the user message to PRIVATE history
        const userMessage: UserModelMessage = { role: "user", content: message };
        this.messages.push(userMessage);

        // Add user message to UI responses
        const userUIMessage: AgentUIMessage = {
          role: "user",
          content: message,
          isBegin: true,
          isEnd: true,
        };
        this.agentResponses.push(userUIMessage);
        this.agentResponsesSubject.next([...this.agentResponses]);

        try {
          const tools = this.createWorkflowTools();

          let isFirstStep = true;

          // Conditionally append planning mode instructions to system prompt
          const systemPrompt = this.planningMode
            ? COPILOT_SYSTEM_PROMPT + "\n\n" + PLANNING_MODE_PROMPT
            : COPILOT_SYSTEM_PROMPT;

          const { text, steps, response } = await generateText({
            model: this.model,
            messages: this.messages, // full history
            tools,
            system: systemPrompt,
            // Stop when: user requested stop OR action plan created OR reached 50 steps
            stopWhen: ({ steps }) => {
              // Check if user requested stop
              if (this.state === CopilotState.STOPPING) {
                this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
                return true;
              }
              // Check if action plan was just created
              if (this.shouldStopAfterActionPlan) {
                return true;
              }
              // Otherwise use the default step count limit
              return stepCountIs(50)({ steps });
            },
            // optional: observe every completed step (tool calls + results available)
            onStepFinish: ({ text, toolCalls, toolResults, finishReason, usage }) => {
              // If stopped by user, skip processing this step
              if (this.state === CopilotState.STOPPING) {
                return;
              }

              // Check if actionPlan tool was called in this step
              if (toolCalls && toolCalls.some((call: any) => call.toolName === "actionPlan")) {
                this.shouldStopAfterActionPlan = true;
              }

              // Emit AgentResponse for this step (not done yet)
              const stepResponse: AgentUIMessage = {
                role: "agent",
                content: text || "",
                isBegin: isFirstStep,
                isEnd: false,
                toolCalls: toolCalls,
                toolResults: toolResults,
                usage: usage as any,
              };
              this.agentResponses.push(stepResponse);
              this.agentResponsesSubject.next([...this.agentResponses]);

              // Mark that we've processed the first step
              isFirstStep = false;
            },
          });
          this.messages.push(...response.messages);
          this.agentResponsesSubject.next([...this.agentResponses]);

          this.state = CopilotState.AVAILABLE;
        } catch (err: any) {
          this.state = CopilotState.AVAILABLE;
          const errorText = `Error: ${err?.message ?? String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);

          const errorResponse: AgentUIMessage = {
            role: "agent",
            content: errorText,
            isBegin: false,
            isEnd: true,
          };
          this.agentResponses.push(errorResponse);
          this.agentResponsesSubject.next([...this.agentResponses]);

          throw err;
        }
      })()
    );
  }

  /**
   * Create workflow manipulation tools with timeout protection
   */
  private createWorkflowTools(): Record<string, any> {
    const addOperatorTool = toolWithTimeout(
      createAddOperatorTool(this.workflowActionService, this.workflowUtilService, this.operatorMetadataService)
    );
    const addLinkTool = toolWithTimeout(createAddLinkTool(this.workflowActionService));
    const actionPlanTool = toolWithTimeout(
      createActionPlanTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService,
        this.actionPlanService,
        this.agentId,
        this.agentName
      )
    );
    const updateActionPlanProgressTool = toolWithTimeout(createUpdateActionPlanProgressTool(this.actionPlanService));
    const getActionPlanTool = toolWithTimeout(createGetActionPlanTool(this.actionPlanService));
    const listActionPlansTool = toolWithTimeout(createListActionPlansTool(this.actionPlanService));
    const deleteActionPlanTool = toolWithTimeout(createDeleteActionPlanTool(this.actionPlanService));
    const updateActionPlanTool = toolWithTimeout(createUpdateActionPlanTool(this.actionPlanService));
    const listOperatorIdsTool = toolWithTimeout(createListOperatorIdsTool(this.workflowActionService));
    const listLinksTool = toolWithTimeout(createListLinksTool(this.workflowActionService));
    const listAllOperatorTypesTool = toolWithTimeout(createListAllOperatorTypesTool(this.workflowUtilService));
    const getOperatorTool = toolWithTimeout(createGetOperatorTool(this.workflowActionService));
    const deleteOperatorTool = toolWithTimeout(createDeleteOperatorTool(this.workflowActionService));
    const deleteLinkTool = toolWithTimeout(createDeleteLinkTool(this.workflowActionService));
    const setOperatorPropertyTool = toolWithTimeout(
      createSetOperatorPropertyTool(this.workflowActionService, this.validationWorkflowService)
    );
    const setPortPropertyTool = toolWithTimeout(
      createSetPortPropertyTool(this.workflowActionService, this.validationWorkflowService)
    );
    const getOperatorPropertiesSchemaTool = toolWithTimeout(
      createGetOperatorPropertiesSchemaTool(this.workflowActionService, this.operatorMetadataService)
    );
    const getOperatorPortsInfoTool = toolWithTimeout(
      createGetOperatorPortsInfoTool(this.workflowActionService, this.operatorMetadataService)
    );
    const getOperatorMetadataTool = toolWithTimeout(
      createGetOperatorMetadataTool(this.workflowActionService, this.operatorMetadataService)
    );
    const getOperatorInputSchemaTool = toolWithTimeout(createGetOperatorInputSchemaTool(this.workflowCompilingService));
    const getWorkflowCompilationStateTool = toolWithTimeout(
      createGetWorkflowCompilationStateTool(this.workflowCompilingService)
    );
    const executeWorkflowTool = toolWithTimeout(createExecuteWorkflowTool(this.executeWorkflowService));
    const getExecutionStateTool = toolWithTimeout(createGetExecutionStateTool(this.executeWorkflowService));
    const killWorkflowTool = toolWithTimeout(createKillWorkflowTool(this.executeWorkflowService));
    const hasOperatorResultTool = toolWithTimeout(
      createHasOperatorResultTool(this.workflowResultService, this.workflowActionService)
    );
    const getOperatorResultTool = toolWithTimeout(createGetOperatorResultTool(this.workflowResultService));
    const getOperatorResultInfoTool = toolWithTimeout(
      createGetOperatorResultInfoTool(this.workflowResultService, this.workflowActionService)
    );
    const getValidationInfoOfCurrentWorkflowTool = toolWithTimeout(
      createGetValidationInfoOfCurrentWorkflowTool(this.validationWorkflowService, this.workflowActionService)
    );
    const validateOperatorTool = toolWithTimeout(createValidateOperatorTool(this.validationWorkflowService));

    // Base tools available in both modes
    const baseTools: Record<string, any> = {
      // workflow editing
      addOperator: addOperatorTool,
      addLink: addLinkTool,
      deleteOperator: deleteOperatorTool,
      deleteLink: deleteLinkTool,
      setOperatorProperty: setOperatorPropertyTool,
      setPortProperty: setPortPropertyTool,
      // workflow validation
      getValidationInfoOfCurrentWorkflow: getValidationInfoOfCurrentWorkflowTool,
      validateOperator: validateOperatorTool,
      // workflow inspecting
      listOperatorIds: listOperatorIdsTool,
      listLinks: listLinksTool,
      listAllOperatorTypes: listAllOperatorTypesTool,
      getOperator: getOperatorTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
      getOperatorInputSchema: getOperatorInputSchemaTool,
      getWorkflowCompilationState: getWorkflowCompilationStateTool,
      // workflow execution
      executeWorkflow: executeWorkflowTool,
      getExecutionStateTool: getExecutionStateTool,
      killWorkflow: killWorkflowTool,
      hasOperatorResult: hasOperatorResultTool,
      getOperatorResult: getOperatorResultTool,
      getOperatorResultInfo: getOperatorResultInfoTool,
    };

    // Conditionally add action plan tools based on planning mode
    if (this.planningMode) {
      // In planning mode: include action plan tools
      console.log(`[${this.agentId}] Creating tools WITH action plan tools (planning mode ON)`);
      return {
        ...baseTools,
        actionPlan: actionPlanTool,
        updateActionPlanProgress: updateActionPlanProgressTool,
        getActionPlan: getActionPlanTool,
        listActionPlans: listActionPlansTool,
        deleteActionPlan: deleteActionPlanTool,
        updateActionPlan: updateActionPlanTool,
      };
    } else {
      // Not in planning mode: exclude action plan tools
      console.log(`[${this.agentId}] Creating tools WITHOUT action plan tools (planning mode OFF)`);
      return baseTools;
    }
  }

  /**
   * Get agent responses for UI display
   */
  public getAgentResponses(): AgentUIMessage[] {
    return [...this.agentResponses];
  }

  /**
   * Stop the current generation (async - waits for generation to actually stop)
   */
  public stopGeneration(): void {
    if (this.state !== CopilotState.GENERATING) {
      return;
    }
    this.state = CopilotState.STOPPING;
  }

  /**
   * Clear message history and agent responses
   */
  public clearMessages(): void {
    this.messages = [];
    this.agentResponses = [];
    this.agentResponsesSubject.next([...this.agentResponses]);
  }

  /**
   * Get current copilot state
   */
  public getState(): CopilotState {
    return this.state;
  }

  /**
   * Disconnect and cleanup copilot resources
   */
  public async disconnect(): Promise<void> {
    // Stop any ongoing generation
    if (this.state === CopilotState.GENERATING) {
      this.stopGeneration();
    }

    // Clear message history
    this.clearMessages();
    // Set state to Unavailable
    this.state = CopilotState.UNAVAILABLE;
    this.notificationService.info(`Agent ${this.agentName} is removed successfully`);
  }

  /**
   * Check if copilot is connected
   */
  public isConnected(): boolean {
    return this.state !== CopilotState.UNAVAILABLE;
  }

  /**
   * Get system prompt based on current planning mode
   */
  public getSystemPrompt(): string {
    return this.planningMode ? COPILOT_SYSTEM_PROMPT + "\n\n" + PLANNING_MODE_PROMPT : COPILOT_SYSTEM_PROMPT;
  }

  /**
   * Get available tools information (name, description, input schema)
   */
  public getToolsInfo(): Array<{ name: string; description: string; inputSchema: any }> {
    const tools = this.createWorkflowTools();
    return Object.entries(tools).map(([name, tool]) => ({
      name: name,
      description: tool.description || "No description available",
      inputSchema: tool.parameters || {},
    }));
  }
}
