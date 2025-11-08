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
  createGetOperatorTool,
  createGetOperatorPropertiesSchemaTool,
  createGetOperatorPortsInfoTool,
  createGetOperatorMetadataTool,
  createGetOperatorInputSchemaTool,
  createGetOperatorOutputSchemaTool,
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
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { COPILOT_SYSTEM_PROMPT } from "./copilot-prompts";
import { NotificationService } from "../../../common/service/notification/notification.service";

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

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private workflowCompilingService: WorkflowCompilingService,
    private notificationService: NotificationService
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

          const { response } = await generateText({
            model: this.model,
            messages: this.messages,
            tools,
            system: COPILOT_SYSTEM_PROMPT,
            stopWhen: ({ steps }) => {
              if (this.state === CopilotState.STOPPING) {
                this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
                return true;
              }
              return stepCountIs(50)({ steps });
            },
            onStepFinish: ({ text, toolCalls, toolResults, usage }) => {
              if (this.state === CopilotState.STOPPING) {
                return;
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
    const listOperatorIdsTool = toolWithTimeout(createListOperatorIdsTool(this.workflowActionService));
    const listLinksTool = toolWithTimeout(createListLinksTool(this.workflowActionService));
    const listAllOperatorTypesTool = toolWithTimeout(createListAllOperatorTypesTool(this.workflowUtilService));
    const getOperatorTool = toolWithTimeout(createGetOperatorTool(this.workflowActionService));
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

    return {
      listAllOperatorTypes: listAllOperatorTypesTool,
      listOperatorIds: listOperatorIdsTool,
      listLinks: listLinksTool,
      getOperator: getOperatorTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
      getOperatorInputSchema: getOperatorInputSchemaTool,
      getOperatorOutputSchema: getOperatorOutputSchemaTool,
    };
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
    return COPILOT_SYSTEM_PROMPT;
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
