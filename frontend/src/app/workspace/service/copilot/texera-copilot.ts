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
import { BehaviorSubject, Observable, from, of, throwError, defer } from "rxjs";
import { map, catchError, tap, switchMap, finalize } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  toolWithTimeout,
  createGetOperatorInCurrentWorkflowTool,
  createGetOperatorPropertiesSchemaTool,
  createGetOperatorPortsInfoTool,
  createGetOperatorMetadataTool,
  createListAllOperatorTypesTool,
  createListLinksInCurrentWorkflowTool,
  createListOperatorsInCurrentWorkflowTool,
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
   * Add an agent UI message and emit to subscribers.
   */
  private emitAgentUIMessage(
    role: "user" | "agent",
    content: string,
    isBegin: boolean,
    isEnd: boolean,
    toolCalls?: any[],
    toolResults?: any[],
    usage?: {
      inputTokens?: number;
      outputTokens?: number;
      totalTokens?: number;
      cachedInputTokens?: number;
    }
  ): void {
    const message: AgentUIMessage = {
      role,
      content,
      isBegin,
      isEnd,
      toolCalls,
      toolResults,
      usage,
    };
    this.agentResponses.push(message);
    this.agentResponsesSubject.next([...this.agentResponses]);
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
    return defer(() => {
      if (!this.model) {
        return throwError(() => new Error("Copilot not initialized"));
      }

      if (this.state !== CopilotState.AVAILABLE) {
        return throwError(() => new Error(`Cannot send message: agent is ${this.state}`));
      }

      this.setState(CopilotState.GENERATING);

      // Add user message to UI
      this.emitAgentUIMessage("user", message, true, true);

      // Add user message to the message history
      const userMessage: UserModelMessage = { role: "user", content: message };
      this.messages.push(userMessage);

      const tools = this.createWorkflowTools();
      let isFirstStep = true;

      return from(
        generateText({
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

            this.emitAgentUIMessage("agent", text || "", isFirstStep, false, toolCalls, toolResults, usage as any);

            isFirstStep = false;
          },
        })
      ).pipe(
        tap(({ response }) => {
          this.messages.push(...response.messages);
          this.agentResponsesSubject.next([...this.agentResponses]);
        }),
        map(() => undefined),
        catchError((err: unknown) => {
          const errorText = `Error: ${err instanceof Error ? err.message : String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);

          this.emitAgentUIMessage("agent", errorText, false, true);

          return throwError(() => err);
        }),
        finalize(() => {
          this.setState(CopilotState.AVAILABLE);
        })
      );
    });
  }

  /**
   * Create workflow manipulation tools with timeout protection.
   */
  private createWorkflowTools(): Record<string, any> {
    const listOperatorsInCurrentWorkflowTool = toolWithTimeout(
      createListOperatorsInCurrentWorkflowTool(this.workflowActionService)
    );
    const listLinksTool = toolWithTimeout(createListLinksInCurrentWorkflowTool(this.workflowActionService));
    const listAllOperatorTypesTool = toolWithTimeout(createListAllOperatorTypesTool(this.workflowUtilService));
    const getOperatorTool = toolWithTimeout(
      createGetOperatorInCurrentWorkflowTool(this.workflowActionService, this.workflowCompilingService)
    );
    const getOperatorPropertiesSchemaTool = toolWithTimeout(
      createGetOperatorPropertiesSchemaTool(this.operatorMetadataService)
    );
    const getOperatorPortsInfoTool = toolWithTimeout(createGetOperatorPortsInfoTool(this.operatorMetadataService));
    const getOperatorMetadataTool = toolWithTimeout(createGetOperatorMetadataTool(this.operatorMetadataService));

    return {
      listAllOperatorTypes: listAllOperatorTypesTool,
      listOperatorsInCurrentWorkflow: listOperatorsInCurrentWorkflowTool,
      listLinksInCurrentWorkflow: listLinksTool,
      getOperatorInCurrentWorkflow: getOperatorTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
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
