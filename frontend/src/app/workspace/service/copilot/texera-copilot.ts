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
import { toolWithTimeout } from "./tool/tools-utility";
import * as workflowMetadataTools from "./tool/workflow-metadata-tools";
import * as currentWorkflowEditingObservingTools from "./tool/current-workflow-editing-observing-tools";
import * as currentWorkflowValidationTools from "./tool/current-workflow-validation-tools";
import * as currentWorkflowExecutionTools from "./tool/current-workflow-execution-tools";
import * as actionPlanTools from "./tool/action-plan-tools";
import * as dataInconsistencyTools from "./tool/data-inconsistency-tools";
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
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
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
  private relevantOperators: string[] = [];
  private relevantOperatorsSubject = new BehaviorSubject<string[]>([]);
  public relevantOperators$ = this.relevantOperatorsSubject.asObservable();

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

              // Track relevant operators from listRelevantOperatorIds tool calls
              if (toolCalls && toolResults) {
                for (let i = 0; i < toolCalls.length; i++) {
                  const toolCall = toolCalls[i];
                  if (toolCall.toolName === "listRelevantOperatorIds") {
                    const toolResult = toolResults[i];
                    console.log("result of context switching: ", toolResult);
                    // The actual result is in toolResult.output, not toolResult.result
                    if (toolResult && toolResult.output && toolResult.output.success && toolResult.output.operatorIds) {
                      this.relevantOperators = toolResult.output.operatorIds;
                      this.relevantOperatorsSubject.next([...this.relevantOperators]);
                      console.log("emit: ", this.relevantOperators);
                    }
                  }
                }
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
        this.workflowConsoleService
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
    const actionPlanTool = toolWithTimeout(
      actionPlanTools.createActionPlanTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService,
        this.actionPlanService,
        this.agentId,
        this.agentName
      )
    );
    const updateActionPlanProgressTool = toolWithTimeout(
      actionPlanTools.createUpdateActionPlanProgressTool(this.actionPlanService)
    );
    const getActionPlanTool = toolWithTimeout(actionPlanTools.createGetActionPlanTool(this.actionPlanService));
    const listActionPlansTool = toolWithTimeout(actionPlanTools.createListActionPlansTool(this.actionPlanService));
    const deleteActionPlanTool = toolWithTimeout(actionPlanTools.createDeleteActionPlanTool(this.actionPlanService));
    const updateActionPlanTool = toolWithTimeout(actionPlanTools.createUpdateActionPlanTool(this.actionPlanService));

    // Base tools available in both modes
    const baseTools: Record<string, any> = {
      // meta level knowledge
      listAllOperatorTypes: listAllOperatorTypesTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
      // current workflow editing
      addOperatorToCurrentWorkflow: addOperatorToCurrentWorkflowTool,
      addLinkToCurrentWorkflow: addLinkToCurrentWorkflowTool,
      deleteOperatorInCurrentWorkflow: deleteOperatorInCurrentWorkflowTool,
      deleteLinkInCurrentWorkflow: deleteLinkInCurrentWorkflowTool,
      setOperatorPropertyInCurrentWorkflow: setOperatorPropertyInCurrentWorkflowTool,
      setPortPropertyInCurrentWorkflow: setPortPropertyInCurrentWorkflowTool,
      // current workflow validation
      getCurrentWorkflowValidationInfo: getCurrentWorkflowValidationInfoTool,
      validateCurrentOperator: validateCurrentOperatorTool,
      // current workflow inspecting
      listCurrentRelevantOperatorIds: listCurrentRelevantOperatorIdsTool,
      listCurrentLinks: listCurrentLinksTool,
      getCurrentOperator: getCurrentOperatorTool,
      getCurrentWorkflowCompilationState: getCurrentWorkflowCompilationStateTool,
      // current workflow execution
      executeCurrentWorkflow: executeCurrentWorkflowTool,
      getCurrentExecutionState: getCurrentExecutionStateTool,
      killCurrentWorkflow: killCurrentWorkflowTool,
      hasCurrentOperatorResult: hasCurrentOperatorResultTool,
      getCurrentOperatorResult: getCurrentOperatorResultTool,
      getCurrentOperatorResultInfo: getCurrentOperatorResultInfoTool,
      getCurrentComputingUnitStatus: getCurrentComputingUnitStatusTool,
      // Data inconsistency tools
      addInconsistency: addInconsistencyTool,
      listInconsistencies: listInconsistenciesTool,
      updateInconsistency: updateInconsistencyTool,
      deleteInconsistency: deleteInconsistencyTool,
      clearInconsistencies: clearInconsistenciesTool,
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
    this.relevantOperators = [];
    this.relevantOperatorsSubject.next([]);
  }

  public getState(): CopilotState {
    return this.state;
  }

  public getRelevantOperators(): string[] {
    return [...this.relevantOperators];
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
