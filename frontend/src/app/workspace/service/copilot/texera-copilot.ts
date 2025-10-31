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
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp";
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
  createListOperatorsTool,
  createListLinksTool,
  createListOperatorTypesTool,
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
  createGetWorkflowValidationErrorsTool,
  createValidateOperatorTool,
  createGetValidOperatorsTool,
  createAddInconsistencyTool,
  createListInconsistenciesTool,
  createUpdateInconsistencyTool,
  createDeleteInconsistencyTool,
  createClearInconsistenciesTool,
  toolWithTimeout,
} from "./workflow-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import { AssistantModelMessage, generateText, type ModelMessage, stepCountIs, UserModelMessage } from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { CopilotCoeditorService } from "./copilot-coeditor.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { COPILOT_SYSTEM_PROMPT } from "./copilot-prompts";
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
import { ActionPlanService } from "../action-plan/action-plan.service";

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
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
export interface AgentResponse {
  content: string;
  isDone: boolean;
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
  // Unique instance identifier for debugging
  private static instanceCounter = 0;
  private readonly instanceId: number;

  private mcpClient?: Client;
  private mcpTools: any[] = [];
  private model: any;
  private modelType: string;

  // Agent identification
  private agentId: string = "";
  private agentName: string = "";

  // PRIVATE message history for AI conversation (not exposed to UI)
  private messages: ModelMessage[] = [];

  // PUBLIC agent responses for UI display
  private agentResponses: AgentResponse[] = [];
  private agentResponsesSubject = new BehaviorSubject<AgentResponse[]>([]);
  public agentResponses$ = this.agentResponsesSubject.asObservable();

  // Copilot state management
  private state: CopilotState = CopilotState.UNAVAILABLE;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private dynamicSchemaService: DynamicSchemaService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private copilotCoeditorService: CopilotCoeditorService,
    private workflowCompilingService: WorkflowCompilingService,
    private validationWorkflowService: ValidationWorkflowService,
    private dataInconsistencyService: DataInconsistencyService,
    private actionPlanService: ActionPlanService
  ) {
    // Assign unique instance ID
    this.instanceId = ++TexeraCopilot.instanceCounter;
    console.log(`TexeraCopilot instance #${this.instanceId} created`);

    // Default model type
    this.modelType = DEFAULT_AGENT_MODEL_ID;
  }

  /**
   * Set the agent identification
   */
  public setAgentInfo(agentId: string, agentName: string): void {
    this.agentId = agentId;
    this.agentName = agentName;
    console.log(`TexeraCopilot instance #${this.instanceId} assigned to agent ${agentId} (${agentName})`);
  }

  /**
   * Set the model type for this agent
   */
  public setModelType(modelType: string): void {
    this.modelType = modelType;
  }

  /**
   * Initialize the copilot with MCP and AI model
   */
  public async initialize(): Promise<void> {
    try {
      // 1. Connect to MCP server
      await this.connectMCP();

      // 2. Initialize OpenAI model with the configured model type
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy",
      }).chat(this.modelType);

      // 3. Set state to Available
      this.state = CopilotState.AVAILABLE;

      console.log("Texera Copilot initialized successfully");
    } catch (error: unknown) {
      console.error("Failed to initialize copilot:", error);
      this.state = CopilotState.UNAVAILABLE;
      throw error;
    }
  }

  /**
   * Connect to MCP server and retrieve tools
   */
  private async connectMCP(): Promise<void> {
    try {
      // Create MCP client with SSE transport for HTTP streaming
      const transport = new StreamableHTTPClientTransport(
        new URL(`${AppSettings.getApiEndpoint()}/${COPILOT_MCP_URL}`, document.baseURI)
      );

      this.mcpClient = new Client(
        {
          name: "texera-copilot-client",
          version: "1.0.0",
        },
        {
          capabilities: {},
        }
      );

      await this.mcpClient.connect(transport);

      // Get all available MCP tools
      const toolsResponse = await this.mcpClient.listTools();
      this.mcpTools = toolsResponse.tools || [];

      console.log(`Connected to MCP server. Retrieved ${this.mcpTools.length} tools.`);
    } catch (error) {
      console.error("Failed to connect to MCP:", error);
      throw error;
    }
  }

  /**
   * Convert MCP tools to AI SDK tool format
   */
  private getMCPToolsForAI(): Record<string, any> {
    const tools: Record<string, any> = {};

    for (const mcpTool of this.mcpTools) {
      tools[mcpTool.name] = {
        description: mcpTool.description || "",
        parameters: mcpTool.inputSchema || {},
        execute: async (args: any) => {
          if (!this.mcpClient) {
            throw new Error("MCP client not connected");
          }
          const result = await this.mcpClient.callTool({
            name: mcpTool.name,
            arguments: args,
          });
          return result.content;
        },
      };
    }

    return tools;
  }

  public sendMessage(message: string): Observable<void> {
    return from(
      (async () => {
        console.log(`TexeraCopilot instance #${this.instanceId} (${this.agentId}) processing message`);
        if (!this.model) {
          throw new Error("Copilot not initialized");
        }

        // Set state to Generating
        this.state = CopilotState.GENERATING;

        // 1) push the user message to PRIVATE history
        const userMessage: UserModelMessage = { role: "user", content: message };
        this.messages.push(userMessage);
        console.log(`TexeraCopilot instance #${this.instanceId} now has ${this.messages.length} messages`);

        try {
          // 2) define tools
          const tools = this.createWorkflowTools();

          // 3) run multi-step with stopWhen to check for user stop request
          const { text, steps, response } = await generateText({
            model: this.model,
            messages: this.messages, // full history
            tools,
            system: COPILOT_SYSTEM_PROMPT,
            // Stop when: user requested stop OR reached 50 steps
            stopWhen: ({ steps }) => {
              // Check if user requested stop
              if (this.state === CopilotState.STOPPING) {
                console.log("Stopping generation due to user request");
                return true;
              }
              // Otherwise use the default step count limit
              return stepCountIs(50)({ steps });
            },
            // optional: observe every completed step (tool calls + results available)
            onStepFinish: ({ text, toolCalls, toolResults, finishReason, usage }) => {
              // Log each step for debugging
              console.debug("step finished", { text, toolCalls, toolResults, finishReason, usage });

              // Emit AgentResponse for this step (not done yet)
              const stepResponse: AgentResponse = {
                content: text || "",
                isDone: false,
                toolCalls: toolCalls,
                toolResults: toolResults,
                usage: usage as any,
              };
              this.agentResponses.push(stepResponse);
              this.agentResponsesSubject.next([...this.agentResponses]);
            },
          });

          // 4) append ALL messages the SDK produced this turn (assistant + tool messages)
          //    This keeps your history perfectly aligned with the SDK's internal state.
          this.messages.push(...response.messages);

          // 5) Emit final AgentResponse with complete content
          const finalResponse: AgentResponse = {
            content: text || "",
            isDone: true,
            usage: (response as any).usage as any,
          };
          this.agentResponses.push(finalResponse);
          this.agentResponsesSubject.next([...this.agentResponses]);

          // 5) optional diagnostics
          if (steps?.length) {
            const totalToolCalls = steps.flatMap(s => s.toolCalls || []).length;
            console.log(`Agent loop finished in ${steps.length} step(s), ${totalToolCalls} tool call(s).`);
          }

          // Clear all copilot presence indicators when generation completes
          this.copilotCoeditorService.clearAll();

          // Set state back to Available
          this.state = CopilotState.AVAILABLE;
        } catch (err: any) {
          // Clear all copilot presence indicators on error
          this.copilotCoeditorService.clearAll();

          // Set state back to Available
          this.state = CopilotState.AVAILABLE;

          // For errors, add to PRIVATE message history
          const errorText = `Error: ${err?.message ?? String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);

          // Emit error as AgentResponse
          const errorResponse: AgentResponse = {
            content: errorText,
            isDone: true,
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
      createAddOperatorTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService,
        this.copilotCoeditorService
      )
    );
    const addLinkTool = toolWithTimeout(createAddLinkTool(this.workflowActionService));
    const actionPlanTool = toolWithTimeout(
      createActionPlanTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService,
        this.copilotCoeditorService,
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
    const listOperatorsTool = toolWithTimeout(
      createListOperatorsTool(this.workflowActionService, this.copilotCoeditorService)
    );
    const listLinksTool = toolWithTimeout(createListLinksTool(this.workflowActionService));
    const listOperatorTypesTool = toolWithTimeout(createListOperatorTypesTool(this.workflowUtilService));
    const getOperatorTool = toolWithTimeout(
      createGetOperatorTool(this.workflowActionService, this.copilotCoeditorService)
    );
    const deleteOperatorTool = toolWithTimeout(
      createDeleteOperatorTool(this.workflowActionService, this.copilotCoeditorService)
    );
    const deleteLinkTool = toolWithTimeout(createDeleteLinkTool(this.workflowActionService));
    const setOperatorPropertyTool = toolWithTimeout(
      createSetOperatorPropertyTool(
        this.workflowActionService,
        this.copilotCoeditorService,
        this.validationWorkflowService
      )
    );
    const setPortPropertyTool = toolWithTimeout(
      createSetPortPropertyTool(this.workflowActionService, this.copilotCoeditorService, this.validationWorkflowService)
    );
    const getOperatorSchemaTool = toolWithTimeout(
      createGetOperatorSchemaTool(this.workflowActionService, this.operatorMetadataService, this.copilotCoeditorService)
    );
    const getOperatorPropertiesSchemaTool = toolWithTimeout(
      createGetOperatorPropertiesSchemaTool(
        this.workflowActionService,
        this.operatorMetadataService,
        this.copilotCoeditorService
      )
    );
    const getOperatorPortsInfoTool = toolWithTimeout(
      createGetOperatorPortsInfoTool(
        this.workflowActionService,
        this.operatorMetadataService,
        this.copilotCoeditorService
      )
    );
    const getOperatorMetadataTool = toolWithTimeout(
      createGetOperatorMetadataTool(
        this.workflowActionService,
        this.operatorMetadataService,
        this.copilotCoeditorService
      )
    );
    const getOperatorInputSchemaTool = toolWithTimeout(
      createGetOperatorInputSchemaTool(this.workflowCompilingService, this.copilotCoeditorService)
    );
    const getWorkflowCompilationStateTool = toolWithTimeout(
      createGetWorkflowCompilationStateTool(this.workflowCompilingService)
    );
    const executeWorkflowTool = toolWithTimeout(createExecuteWorkflowTool(this.executeWorkflowService));
    const getExecutionStateTool = toolWithTimeout(createGetExecutionStateTool(this.executeWorkflowService));
    const killWorkflowTool = toolWithTimeout(createKillWorkflowTool(this.executeWorkflowService));
    const hasOperatorResultTool = toolWithTimeout(
      createHasOperatorResultTool(this.workflowResultService, this.workflowActionService, this.copilotCoeditorService)
    );
    const getOperatorResultTool = toolWithTimeout(
      createGetOperatorResultTool(this.workflowResultService, this.copilotCoeditorService)
    );
    const getOperatorResultInfoTool = toolWithTimeout(
      createGetOperatorResultInfoTool(
        this.workflowResultService,
        this.workflowActionService,
        this.copilotCoeditorService
      )
    );
    const getWorkflowValidationErrorsTool = toolWithTimeout(
      createGetWorkflowValidationErrorsTool(this.validationWorkflowService)
    );
    const validateOperatorTool = toolWithTimeout(
      createValidateOperatorTool(this.validationWorkflowService, this.copilotCoeditorService)
    );
    const getValidOperatorsTool = toolWithTimeout(
      createGetValidOperatorsTool(this.validationWorkflowService, this.workflowActionService)
    );

    // Inconsistency tools
    const addInconsistencyTool = toolWithTimeout(createAddInconsistencyTool(this.dataInconsistencyService));
    const listInconsistenciesTool = toolWithTimeout(createListInconsistenciesTool(this.dataInconsistencyService));
    const updateInconsistencyTool = toolWithTimeout(createUpdateInconsistencyTool(this.dataInconsistencyService));
    const deleteInconsistencyTool = toolWithTimeout(createDeleteInconsistencyTool(this.dataInconsistencyService));
    const clearInconsistenciesTool = toolWithTimeout(createClearInconsistenciesTool(this.dataInconsistencyService));

    // Get MCP tools in AI SDK format
    // const mcpToolsForAI = this.getMCPToolsForAI();

    return {
      // ...mcpToolsForAI,
      addOperator: addOperatorTool,
      addLink: addLinkTool,
      actionPlan: actionPlanTool,
      updateActionPlanProgress: updateActionPlanProgressTool,
      getActionPlan: getActionPlanTool,
      listActionPlans: listActionPlansTool,
      deleteActionPlan: deleteActionPlanTool,
      updateActionPlan: updateActionPlanTool,
      listOperators: listOperatorsTool,
      listLinks: listLinksTool,
      listOperatorTypes: listOperatorTypesTool,
      getOperator: getOperatorTool,
      deleteOperator: deleteOperatorTool,
      deleteLink: deleteLinkTool,
      setOperatorProperty: setOperatorPropertyTool,
      setPortProperty: setPortPropertyTool,
      // getOperatorSchema: getOperatorSchemaTool,
      getOperatorPropertiesSchema: getOperatorPropertiesSchemaTool,
      getOperatorPortsInfo: getOperatorPortsInfoTool,
      getOperatorMetadata: getOperatorMetadataTool,
      getOperatorInputSchema: getOperatorInputSchemaTool,
      getWorkflowCompilationState: getWorkflowCompilationStateTool,
      executeWorkflow: executeWorkflowTool,
      // killWorkflow: killWorkflowTool,
      hasOperatorResult: hasOperatorResultTool,
      getOperatorResult: getOperatorResultTool,
      getOperatorResultInfo: getOperatorResultInfoTool,
      getWorkflowValidationErrors: getWorkflowValidationErrorsTool,
      validateOperator: validateOperatorTool,
      getValidOperators: getValidOperatorsTool,
      // Data inconsistency tools
      addInconsistency: addInconsistencyTool,
      listInconsistencies: listInconsistenciesTool,
      updateInconsistency: updateInconsistencyTool,
      deleteInconsistency: deleteInconsistencyTool,
      clearInconsistencies: clearInconsistenciesTool,
    };
  }

  /**
   * Get agent responses for UI display
   */
  public getAgentResponses(): AgentResponse[] {
    return [...this.agentResponses];
  }

  /**
   * Stop the current generation (async - waits for generation to actually stop)
   */
  public stopGeneration(): void {
    if (this.state !== CopilotState.GENERATING) {
      console.log("Not generating, nothing to stop");
      return;
    }

    // Set state to Stopping - stopWhen callback will detect this and stop generation
    this.state = CopilotState.STOPPING;
    console.log("Stopping generation...");

    // State will be set back to Available when the generation completes via stopWhen
  }

  /**
   * Clear message history and agent responses
   */
  public clearMessages(): void {
    this.messages = [];
    this.agentResponses = [];
    this.agentResponsesSubject.next([...this.agentResponses]);
    console.log("Message history and agent responses cleared");
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

    // Disconnect the MCP client if it exists
    if (this.mcpClient) {
      await this.mcpClient.close();
      this.mcpClient = undefined;
    }

    // Set state to Unavailable
    this.state = CopilotState.UNAVAILABLE;

    console.log("Copilot disconnected");
  }

  /**
   * Check if copilot is connected
   */
  public isConnected(): boolean {
    return this.state !== CopilotState.UNAVAILABLE;
  }
}
