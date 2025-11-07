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
  createGetOperatorOutputSchemaTool,
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
  createGetComputingUnitStatusTool,
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
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../workflow-console/workflow-console.service";

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
 * Agent response for UI display.
 */
export interface AgentUIMessage {
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
}

/**
 * Texera Copilot - An AI assistant for workflow manipulation.
 * Uses Vercel AI SDK for chat completion.
 * Note: Not a singleton - each agent has its own instance.
 */
@Injectable()
export class TexeraCopilot {
  private model: any;
  private modelType: string;
  private agentId: string = "";
  private agentName: string = "";
  private messages: ModelMessage[] = [];
  private agentResponses: AgentUIMessage[] = [];
  private agentResponsesSubject = new BehaviorSubject<AgentUIMessage[]>([]);
  public agentResponses$ = this.agentResponsesSubject.asObservable();
  private state: CopilotState = CopilotState.UNAVAILABLE;
  private stateSubject = new BehaviorSubject<CopilotState>(CopilotState.UNAVAILABLE);
  public state$ = this.stateSubject.asObservable();
  private shouldStopAfterActionPlan: boolean = false;
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
    private notificationService: NotificationService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService
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
   * Type guard to check if a message is a valid ModelMessage.
   * Uses TypeScript's type predicate for compile-time type safety.
   *
   * Validates messages according to Vercel AI SDK ModelMessage types:
   * - UserModelMessage: { role: "user", content: string | ContentPart[] }
   * - AssistantModelMessage: { role: "assistant", content: string | ContentPart[] }
   * - ToolModelMessage: { role: "tool", content: ToolResultPart[] }
   * - SystemModelMessage: { role: "system", content: string }
   */
  private isValidModelMessage(message: unknown): message is ModelMessage {
    if (!message || typeof message !== "object") {
      return false;
    }

    const msg = message as Record<string, unknown>;

    // Check if role property exists and is a string
    if (typeof msg.role !== "string") {
      return false;
    }

    // Validate based on role using type narrowing
    switch (msg.role) {
      case "user":
      case "system":
        // UserModelMessage/SystemModelMessage: { role: "user"/"system", content: string | array }
        return typeof msg.content === "string" || Array.isArray(msg.content);

      case "assistant":
        // AssistantModelMessage: { role: "assistant", content: string | array }
        // Array content must contain valid content parts (text, tool-call, tool-result, etc.)
        if (typeof msg.content === "string") {
          return true;
        }
        if (Array.isArray(msg.content)) {
          // Verify all parts have the required 'type' field
          return msg.content.every((part: any) => part && typeof part === "object" && typeof part.type === "string");
        }
        return false;

      case "tool":
        // ToolModelMessage: { role: "tool", content: ToolResultPart[] }
        // Content must be array of tool result parts
        if (!Array.isArray(msg.content)) {
          return false;
        }
        // Each part must have type='tool-result', toolCallId, toolName, and output with type/value
        return msg.content.every(
          (part: any) =>
            part &&
            typeof part === "object" &&
            part.type === "tool-result" &&
            typeof part.toolCallId === "string" &&
            typeof part.toolName === "string" &&
            part.output &&
            typeof part.output === "object" &&
            typeof part.output.type === "string" &&
            "value" in part.output
        );

      default:
        return false;
    }
  }

  /**
   * Validate all messages in the conversation history.
   * Throws an error if any message doesn't conform to ModelMessage type.
   */
  private validateMessages(): void {
    const invalidMessages: Array<{ index: number; message: unknown }> = [];

    this.messages.forEach((message, index) => {
      if (!this.isValidModelMessage(message)) {
        invalidMessages.push({ index, message });
      }
    });

    if (invalidMessages.length > 0) {
      const indices = invalidMessages.map(m => m.index).join(", ");
      const details = invalidMessages.map(m => `[${m.index}]: ${JSON.stringify(m.message)}`).join("; ");
      const errorMessage = `Invalid ModelMessage(s) found at indices: ${indices}. Details: ${details}`;

      this.notificationService.error(
        `Message validation failed: ${invalidMessages.length} invalid message(s). Please disconnect current agent and create a new agent`
      );
      throw new Error(errorMessage);
    }
  }

  /**
   * Initialize the copilot with the AI model.
   */
  public async initialize(): Promise<void> {
    try {
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy",
      }).chat(this.modelType);

      this.setState(CopilotState.AVAILABLE);
    } catch (error: unknown) {
      this.setState(CopilotState.UNAVAILABLE);
      throw error;
    }
  }

  public sendMessage(message: string): Observable<void> {
    return from(
      (async () => {
        if (!this.model) {
          throw new Error("Copilot not initialized");
        }

        // Guard against sending messages when not available
        if (this.state !== CopilotState.AVAILABLE) {
          throw new Error(`Cannot send message: agent is ${this.state}`);
        }

        this.setState(CopilotState.GENERATING);
        this.shouldStopAfterActionPlan = false;

        const userMessage: UserModelMessage = { role: "user", content: message };
        this.messages.push(userMessage);
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

          const systemPrompt = this.planningMode
            ? COPILOT_SYSTEM_PROMPT + "\n\n" + PLANNING_MODE_PROMPT
            : COPILOT_SYSTEM_PROMPT;

          const { response } = await generateText({
            model: this.model,
            messages: this.messages,
            tools,
            system: systemPrompt,
            stopWhen: ({ steps }) => {
              if (this.state === CopilotState.STOPPING) {
                this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
                return true;
              }
              if (this.shouldStopAfterActionPlan) {
                return true;
              }
              return stepCountIs(50)({ steps });
            },
            onStepFinish: ({ text, toolCalls, toolResults, usage }) => {
              if (this.state === CopilotState.STOPPING) {
                return;
              }

              if (toolCalls && toolCalls.some((call: any) => call.toolName === "actionPlan")) {
                this.shouldStopAfterActionPlan = true;
              }

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

              isFirstStep = false;
            },
          });
          this.messages.push(...response.messages);
          this.agentResponsesSubject.next([...this.agentResponses]);

          this.setState(CopilotState.AVAILABLE);
        } catch (err: any) {
          this.setState(CopilotState.AVAILABLE);
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
   * Create workflow manipulation tools with timeout protection.
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
    const getOperatorOutputSchemaTool = toolWithTimeout(
      createGetOperatorOutputSchemaTool(this.workflowCompilingService)
    );
    const getWorkflowCompilationStateTool = toolWithTimeout(
      createGetWorkflowCompilationStateTool(this.workflowCompilingService)
    );
    const executeWorkflowTool = toolWithTimeout(createExecuteWorkflowTool(this.executeWorkflowService));
    const getExecutionStateTool = toolWithTimeout(
      createGetExecutionStateTool(this.executeWorkflowService, this.workflowActionService, this.workflowConsoleService)
    );
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
    const getComputingUnitStatusTool = toolWithTimeout(
      createGetComputingUnitStatusTool(this.computingUnitStatusService)
    );

    const baseTools: Record<string, any> = {
      addOperator: addOperatorTool,
      addLink: addLinkTool,
      deleteOperator: deleteOperatorTool,
      deleteLink: deleteLinkTool,
      setOperatorProperty: setOperatorPropertyTool,
      setPortProperty: setPortPropertyTool,
      getValidationInfoOfCurrentWorkflow: getValidationInfoOfCurrentWorkflowTool,
      validateOperator: validateOperatorTool,
      listOperatorIds: listOperatorIdsTool,
      listLinks: listLinksTool,
      listAllOperatorTypes: listAllOperatorTypesTool,
      getOperator: getOperatorTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
      getOperatorInputSchema: getOperatorInputSchemaTool,
      getOperatorOutputSchema: getOperatorOutputSchemaTool,
      getWorkflowCompilationState: getWorkflowCompilationStateTool,
      executeWorkflow: executeWorkflowTool,
      getExecutionStateTool: getExecutionStateTool,
      killWorkflow: killWorkflowTool,
      hasOperatorResult: hasOperatorResultTool,
      getOperatorResult: getOperatorResultTool,
      getOperatorResultInfo: getOperatorResultInfoTool,
      getComputingUnitStatus: getComputingUnitStatusTool,
    };

    if (this.planningMode) {
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
      return baseTools;
    }
  }

  public getAgentResponses(): AgentUIMessage[] {
    return [...this.agentResponses];
  }

  public stopGeneration(): void {
    if (this.state !== CopilotState.GENERATING) {
      return;
    }
    this.setState(CopilotState.STOPPING);
  }

  public clearMessages(): void {
    this.messages = [];
    this.agentResponses = [];
    this.agentResponsesSubject.next([...this.agentResponses]);
  }

  public getState(): CopilotState {
    return this.state;
  }

  public async disconnect(): Promise<void> {
    if (this.state === CopilotState.GENERATING) {
      this.stopGeneration();
    }

    this.clearMessages();
    this.setState(CopilotState.UNAVAILABLE);
    this.notificationService.info(`Agent ${this.agentName} is removed successfully`);
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
}
