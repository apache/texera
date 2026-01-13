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
import { toolWithTimeout } from "./tool/tools-utility";
import * as CurrentWorkflowTools from "./tool/current-workflow-editing-observing-tools";
import * as MetadataTools from "./tool/workflow-metadata-tools";
import { ToolOperatorAccess, parseOperatorAccessFromStep } from "./tool/react-step-operator-parser";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage, stepCountIs } from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { COPILOT_SYSTEM_PROMPT } from "./copilot-prompts";
import { NotificationService } from "../../../common/service/notification/notification.service";

export enum CopilotState {
  UNAVAILABLE = "Unavailable",
  AVAILABLE = "Available",
  GENERATING = "Generating",
  STOPPING = "Stopping",
}

/**
 * Represents a single step in the ReAct (Reasoning and Acting) conversation flow.
 * Each step can be either a user message or an agent response with potential tool calls.
 */
export interface ReActStep {
  messageId: string;
  stepId: number;
  role: "user" | "agent";
  content: string;
  isBegin: boolean;
  isEnd: boolean;
  timestamp: number;
  toolCalls?: any[];
  toolResults?: any[];
  usage?: {
    inputTokens?: number;
    outputTokens?: number;
    totalTokens?: number;
    cachedInputTokens?: number;
  };
  /**
   * Map from tool call index to operator access information, which tracks operators were viewed or modified during the tool call.
   */
  operatorAccess?: Map<number, ToolOperatorAccess>;
}

/**
 * Texera Copilot Service provides AI-powered assistance for workflow creation and manipulation.
 *
 * This service manages a single AI agent instance that can:
 * 1. Interact with users through natural language messages
 * 2. Execute workflow operations using specialized tools
 * 3. Maintain conversation history and state
 *
 * The service communicates with an LLM backend (via LiteLLM) to generate responses and uses
 * workflow tools to perform actions like listing operators, getting operator schemas, and
 * manipulating workflow components.
 *
 * State management includes:
 * - UNAVAILABLE: Agent not initialized
 * - AVAILABLE: Agent ready to receive messages
 * - GENERATING: Agent currently processing and generating response
 * - STOPPING: Agent in the process of stopping generation
 */
@Injectable()
export class TexeraCopilot {
  /**
   * Maximum number of ReAct reasoning/action cycles allowed per generation.
   * Prevents infinite loops and excessive token usage.
   */
  private static readonly MAX_REACT_STEPS = 50;

  private model: any;
  private modelType = "";
  private agentName = "";

  /**
   * Conversation history in LLM API format.
   * Used internally to maintain context for generateText() API calls.
   * Contains the raw message format expected by the AI model.
   */
  private messages: ModelMessage[] = [];

  /**
   * Representing a step in ReAct (Reasoning + Acting).
   * This is what gets displayed in the UI to show the agent's reasoning process.
   * Each step contains messageId (randomly generated UUID) and stepId (incremental from 0).
   */
  private reActSteps: ReActStep[] = [];
  private reActStepsSubject = new BehaviorSubject<ReActStep[]>([]);
  public reActSteps$ = this.reActStepsSubject.asObservable();

  private state = CopilotState.UNAVAILABLE;
  private stateSubject = new BehaviorSubject<CopilotState>(CopilotState.UNAVAILABLE);
  public state$ = this.stateSubject.asObservable();
  private tools: Record<string, any> = {};

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private workflowCompilingService: WorkflowCompilingService,
    private notificationService: NotificationService
  ) {}

  public setAgentInfo(agentName: string): void {
    this.agentName = agentName;
  }

  public setModelType(modelType: string): void {
    this.modelType = modelType;
  }

  private setState(newState: CopilotState): void {
    this.state = newState;
    this.stateSubject.next(newState);
  }

  private emitReActStep(
    messageId: string,
    stepId: number,
    role: "user" | "agent",
    content: string,
    isBegin: boolean,
    isEnd: boolean,
    toolCalls?: any[],
    toolResults?: any[],
    usage?: ReActStep["usage"],
    operatorAccess?: Map<number, ToolOperatorAccess>
  ): void {
    this.reActSteps.push({
      messageId,
      stepId,
      role,
      content,
      isBegin,
      isEnd,
      timestamp: Date.now(),
      toolCalls,
      toolResults,
      usage,
      operatorAccess,
    });
    this.reActStepsSubject.next([...this.reActSteps]);
  }

  public initialize(): Observable<void> {
    return defer(() => {
      try {
        this.model = createOpenAI({
          baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
          // apiKey is required by the library for creating the OpenAI compatible client;
          // For security reason, we store the apiKey at the backend, thus the value is dummy here.
          apiKey: "dummy",
        }).chat(this.modelType);

        // Create tools once during initialization
        this.tools = this.createWorkflowTools();

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

      // Generate unique message ID for this conversation turn
      const messageId = crypto.randomUUID();
      let stepId = 0;

      // Emit user message as first step
      this.emitReActStep(messageId, stepId++, "user", message, true, true);
      this.messages.push({ role: "user", content: message });

      let isFirstStep = true;

      /**
       * Generate text using the AI model with ReAct (Reasoning + Acting) pattern.
       * This is the core of the agent lifecycle with several callbacks:
       *
       * Lifecycle flow:
       * 1. generateText() starts the LLM generation
       * 2. stopWhen() - checked before each step to determine if generation should stop
       * 3. onStepFinish() - called DURING generation after each reasoning/action step (real-time updates)
       * 4. pipe operators - executed AFTER generation completes (final processing)
       */
      return from(
        generateText({
          model: this.model,
          messages: this.messages,
          tools: this.tools,
          system: COPILOT_SYSTEM_PROMPT,
          /**
           * stopWhen - Determines if generation should stop.
           * Called before each step during generation.
           * Returns true to stop, false to continue.
           */
          stopWhen: ({ steps }) => {
            if (this.state === CopilotState.STOPPING) {
              this.notificationService.info(`Agent ${this.agentName} has stopped generation`);
              return true;
            }
            // Stop if step count reaches max limit to prevent infinite loops
            return stepCountIs(TexeraCopilot.MAX_REACT_STEPS)({ steps });
          },
          /**
           * onStepFinish is called DURING generation after each ReAct step completes.
           * This provides real-time updates to the UI as the agent reasons and acts.
           *
           * Each step may include:
           * - text: The agent's reasoning or response text
           * - toolCalls: Tools the agent decided to call
           * - toolResults: Results from executed tools
           * - usage: Token usage for this step
           *
           * Note: This is called multiple times during a single generation,
           * once per reasoning/action cycle.
           */
          onStepFinish: ({ text, toolCalls, toolResults, usage }) => {
            if (this.state === CopilotState.STOPPING) {
              return;
            }

            // Parse operator access from tool results to track viewed/modified operators
            const operatorAccess = parseOperatorAccessFromStep(toolCalls || [], toolResults || []);

            this.emitReActStep(
              messageId,
              stepId++,
              "agent",
              text || "",
              isFirstStep,
              false,
              toolCalls,
              toolResults,
              usage as any,
              operatorAccess
            );
            isFirstStep = false;
          },
        })
      ).pipe(
        /**
         * To this point, generateText has finished.
         * All the responses from AI are recorded in responses variable.
         */
        tap(({ response }) => {
          this.messages.push(...response.messages);
          this.reActStepsSubject.next([...this.reActSteps]);
        }),
        map(() => undefined),
        catchError((err: unknown) => {
          const errorText = `Error: ${err instanceof Error ? err.message : String(err)}`;
          this.messages.push({ role: "assistant", content: errorText });
          this.emitReActStep(messageId, stepId++, "agent", errorText, false, true);
          return throwError(() => err);
        }),
        /**
         * Resets agent state back to AVAILABLE so it can handle new messages.
         */
        finalize(() => {
          this.setState(CopilotState.AVAILABLE);
        })
      );
    });
  }

  private createWorkflowTools(): Record<string, any> {
    const listOperatorsInCurrentWorkflowTool = toolWithTimeout(
      CurrentWorkflowTools.createListOperatorsInCurrentWorkflowTool(this.workflowActionService)
    );
    const listLinksTool = toolWithTimeout(CurrentWorkflowTools.createListCurrentLinksTool(this.workflowActionService));
    const listAllOperatorTypesTool = toolWithTimeout(
      MetadataTools.createListAllOperatorTypesTool(this.workflowUtilService)
    );
    const getOperatorTool = toolWithTimeout(
      CurrentWorkflowTools.createGetCurrentOperatorTool(this.workflowActionService, this.workflowCompilingService)
    );
    const getOperatorPropertiesSchemaTool = toolWithTimeout(
      MetadataTools.createGetOperatorPropertiesSchemaTool(this.operatorMetadataService)
    );
    const getOperatorPortsInfoTool = toolWithTimeout(
      MetadataTools.createGetOperatorPortsInfoTool(this.operatorMetadataService)
    );
    const getOperatorMetadataTool = toolWithTimeout(
      MetadataTools.createGetOperatorMetadataTool(this.operatorMetadataService)
    );

    return {
      [MetadataTools.TOOL_NAME_LIST_ALL_OPERATOR_TYPES]: listAllOperatorTypesTool,
      [CurrentWorkflowTools.TOOL_NAME_LIST_OPERATORS_IN_CURRENT_WORKFLOW]: listOperatorsInCurrentWorkflowTool,
      [CurrentWorkflowTools.TOOL_NAME_LIST_CURRENT_LINKS]: listLinksTool,
      [CurrentWorkflowTools.TOOL_NAME_GET_CURRENT_OPERATOR]: getOperatorTool,
      [MetadataTools.TOOL_NAME_GET_OPERATOR_PROPERTIES_SCHEMA]: getOperatorPropertiesSchemaTool,
      [MetadataTools.TOOL_NAME_GET_OPERATOR_PORTS_INFO]: getOperatorPortsInfoTool,
      [MetadataTools.TOOL_NAME_GET_OPERATOR_METADATA]: getOperatorMetadataTool,
    };
  }

  public getReActSteps(): ReActStep[] {
    return [...this.reActSteps];
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
      this.tools = {}; // Clear tools to free memory
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
    return Object.entries(this.tools).map(([name, tool]) => ({
      name: name,
      description: tool.description || "No description available",
      inputSchema: tool.parameters || {},
    }));
  }
}
