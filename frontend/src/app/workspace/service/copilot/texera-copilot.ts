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
import { Observable } from "rxjs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  createAddOperatorTool,
  createAddLinkTool,
  createActionPlanTool,
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

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
export const AGENT_MODEL_ID = "claude-3.7";

// Message window size: -1 means no limit, positive value keeps only latest N messages
export const MESSAGE_WINDOW_SIZE = 3;

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
 * Agent response structure for streaming intermediate and final results
 */
export interface AgentResponse {
  type: "trace" | "response";
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
 */
@Injectable({
  providedIn: "root",
})
export class TexeraCopilot {
  private mcpClient?: Client;
  private mcpTools: any[] = [];
  private model: any;

  // Message history using AI SDK's ModelMessage type
  private messages: ModelMessage[] = [];

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
    private dataInconsistencyService: DataInconsistencyService
  ) {
    // Don't auto-initialize, wait for user to enable
  }

  /**
   * Initialize the copilot with MCP and AI model
   */
  public async initialize(): Promise<void> {
    try {
      // 1. Connect to MCP server
      await this.connectMCP();

      // 2. Initialize OpenAI model
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy",
      }).chat(AGENT_MODEL_ID);

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

  public sendMessage(message: string): Observable<AgentResponse> {
    return new Observable<AgentResponse>(observer => {
      if (!this.model) {
        observer.error(new Error("Copilot not initialized"));
        return;
      }

      // Set state to Generating
      this.state = CopilotState.GENERATING;

      // 1) push the user message (don't emit to stream - already handled by UI)
      const userMessage: UserModelMessage = { role: "user", content: message };
      this.messages.push(userMessage);

      // Trim messages array to keep only the latest MESSAGE_WINDOW_SIZE messages if window size is set
      if (MESSAGE_WINDOW_SIZE > 0 && this.messages.length > MESSAGE_WINDOW_SIZE) {
        this.messages = this.messages.slice(-MESSAGE_WINDOW_SIZE);
      }

      // 2) define tools (your existing helpers)
      const tools = this.createWorkflowTools();

      // 3) run multi-step with stopWhen to check for user stop request
      generateText({
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

          // If there are tool calls, emit raw trace data
          if (toolCalls && toolCalls.length > 0) {
            const traceResponse: AgentResponse = {
              type: "trace",
              content: text || "",
              isDone: false,
              toolCalls,
              toolResults,
              usage,
            };

            // Emit raw trace data
            observer.next(traceResponse);
          }
        },
      })
        .then(({ text, steps, response }) => {
          // 4) append ALL messages the SDK produced this turn (assistant + tool messages)
          //    This keeps your history perfectly aligned with the SDK's internal state.
          this.messages.push(...response.messages);

          // Trim messages array to keep only the latest MESSAGE_WINDOW_SIZE messages if window size is set
          if (MESSAGE_WINDOW_SIZE > 0 && this.messages.length > MESSAGE_WINDOW_SIZE) {
            this.messages = this.messages.slice(-MESSAGE_WINDOW_SIZE);
          }

          // 5) optional diagnostics
          if (steps?.length) {
            const totalToolCalls = steps.flatMap(s => s.toolCalls || []).length;
            console.log(`Agent loop finished in ${steps.length} step(s), ${totalToolCalls} tool call(s).`);
          }

          // Clear all copilot presence indicators when generation completes
          this.copilotCoeditorService.clearAll();

          // Set state back to Available
          this.state = CopilotState.AVAILABLE;

          // Emit final response with raw data
          const finalResponse: AgentResponse = {
            type: "response",
            content: text,
            isDone: true,
          };
          observer.next(finalResponse);
          observer.complete();
        })
        .catch((err: any) => {
          // Clear all copilot presence indicators on error
          this.copilotCoeditorService.clearAll();

          // Set state back to Available
          this.state = CopilotState.AVAILABLE;

          // For errors, add to message history and propagate error
          const errorText = `Error: ${err?.message ?? String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);

          // Trim messages array to keep only the latest MESSAGE_WINDOW_SIZE messages if window size is set
          if (MESSAGE_WINDOW_SIZE > 0 && this.messages.length > MESSAGE_WINDOW_SIZE) {
            this.messages = this.messages.slice(-MESSAGE_WINDOW_SIZE);
          }

          observer.error(err);
        });
    });
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
        this.copilotCoeditorService
      )
    );
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
   * Get conversation history as ModelMessage array
   */
  public getMessages(): ModelMessage[] {
    return [...this.messages];
  }

  /**
   * Add a message to the conversation history
   */
  public addMessage(message: ModelMessage): void {
    this.messages.push(message);

    // Trim messages array to keep only the latest MESSAGE_WINDOW_SIZE messages if window size is set
    if (MESSAGE_WINDOW_SIZE > 0 && this.messages.length > MESSAGE_WINDOW_SIZE) {
      this.messages = this.messages.slice(-MESSAGE_WINDOW_SIZE);
    }
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
   * Clear message history
   */
  public clearMessages(): void {
    this.messages = [];
    console.log("Message history cleared");
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
