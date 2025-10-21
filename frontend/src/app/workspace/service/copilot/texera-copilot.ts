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
  createListOperatorsTool,
  createListLinksTool,
  createListOperatorTypesTool,
  createGetOperatorTool,
  createDeleteOperatorTool,
  createDeleteLinkTool,
  createSetOperatorPropertyTool,
  createGetOperatorSchemaTool,
  createGetOperatorInputSchemaTool,
  createGetWorkflowCompilationStateTool,
  createExecuteWorkflowTool,
  createGetExecutionStateTool,
  createHasOperatorResultTool,
  createGetOperatorResultSnapshotTool,
  createGetOperatorResultInfoTool,
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

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
export const AGENT_MODEL_ID = "claude-3.7";

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

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService,
    private dynamicSchemaService: DynamicSchemaService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private copilotCoeditorService: CopilotCoeditorService,
    private workflowCompilingService: WorkflowCompilingService
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

      console.log("Texera Copilot initialized successfully");
    } catch (error: unknown) {
      console.error("Failed to initialize copilot:", error);
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

      // 1) push the user message (don't emit to stream - already handled by UI)
      const userMessage: UserModelMessage = { role: "user", content: message };
      this.messages.push(userMessage);

      // 2) define tools (your existing helpers)
      const tools = this.createWorkflowTools();

      // 3) run multi-step with stopWhen
      generateText({
        model: this.model,
        messages: this.messages, // full history
        tools,
        system:
          "You are Texera Copilot, an AI assistant for building and modifying data workflows. " +
          "Your task is helping user explore the data using operators. " +
          "Common operators would be Limit to limit the size of data; " +
          "Aggregate to do some aggregation; and some visualization operator. " +
          "A good generation style is adding an operator, configuring its property and then executing it to make sure each editing is valid. Generate 3-5 operators is enough for every round of generation",
        stopWhen: stepCountIs(50),

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

          // 5) optional diagnostics
          if (steps?.length) {
            const totalToolCalls = steps.flatMap(s => s.toolCalls || []).length;
            console.log(`Agent loop finished in ${steps.length} step(s), ${totalToolCalls} tool call(s).`);
          }

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
          const errorText = `Error: ${err?.message ?? String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);
          observer.error(err);
        });
    });
  }

  /**
   * Create workflow manipulation tools
   */
  private createWorkflowTools(): Record<string, any> {
    const addOperatorTool = createAddOperatorTool(
      this.workflowActionService,
      this.workflowUtilService,
      this.operatorMetadataService,
      this.copilotCoeditorService
    );
    const addLinkTool = createAddLinkTool(this.workflowActionService);
    const listOperatorsTool = createListOperatorsTool(this.workflowActionService, this.copilotCoeditorService);
    const listLinksTool = createListLinksTool(this.workflowActionService);
    const listOperatorTypesTool = createListOperatorTypesTool(this.workflowUtilService);
    const getOperatorTool = createGetOperatorTool(this.workflowActionService, this.copilotCoeditorService);
    const deleteOperatorTool = createDeleteOperatorTool(this.workflowActionService, this.copilotCoeditorService);
    const deleteLinkTool = createDeleteLinkTool(this.workflowActionService);
    const setOperatorPropertyTool = createSetOperatorPropertyTool(
      this.workflowActionService,
      this.copilotCoeditorService
    );
    const getOperatorSchemaTool = createGetOperatorSchemaTool(
      this.workflowActionService,
      this.operatorMetadataService,
      this.copilotCoeditorService
    );
    const getOperatorInputSchemaTool = createGetOperatorInputSchemaTool(
      this.workflowCompilingService,
      this.copilotCoeditorService
    );
    const getWorkflowCompilationStateTool = createGetWorkflowCompilationStateTool(this.workflowCompilingService);
    const executeWorkflowTool = createExecuteWorkflowTool(this.executeWorkflowService);
    const getExecutionStateTool = createGetExecutionStateTool(this.executeWorkflowService);
    const hasOperatorResultTool = createHasOperatorResultTool(
      this.workflowResultService,
      this.workflowActionService,
      this.copilotCoeditorService
    );
    const getOperatorResultSnapshotTool = createGetOperatorResultSnapshotTool(
      this.workflowResultService,
      this.workflowActionService,
      this.copilotCoeditorService
    );
    const getOperatorResultInfoTool = createGetOperatorResultInfoTool(
      this.workflowResultService,
      this.workflowActionService,
      this.copilotCoeditorService
    );

    // Get MCP tools in AI SDK format
    // const mcpToolsForAI = this.getMCPToolsForAI();

    return {
      // ...mcpToolsForAI,
      addOperator: addOperatorTool,
      addLink: addLinkTool,
      listOperators: listOperatorsTool,
      listLinks: listLinksTool,
      listOperatorTypes: listOperatorTypesTool,
      getOperator: getOperatorTool,
      deleteOperator: deleteOperatorTool,
      deleteLink: deleteLinkTool,
      setOperatorProperty: setOperatorPropertyTool,
      getOperatorSchema: getOperatorSchemaTool,
      getOperatorInputSchema: getOperatorInputSchemaTool,
      getWorkflowCompilationState: getWorkflowCompilationStateTool,
      executeWorkflow: executeWorkflowTool,
      getExecutionState: getExecutionStateTool,
      hasOperatorResult: hasOperatorResultTool,
      getOperatorResultSnapshot: getOperatorResultSnapshotTool,
      getOperatorResultInfo: getOperatorResultInfoTool,
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
  }

  /**
   * Disconnect and cleanup copilot resources
   */
  public async disconnect(): Promise<void> {
    // Disconnect the MCP client if it exists
    if (this.mcpClient) {
      await this.mcpClient.close();
      this.mcpClient = undefined;
    }

    console.log("Copilot disconnected");
  }

  /**
   * Check if copilot is connected
   */
  public isConnected(): boolean {
    return this.mcpClient !== undefined && this.model !== undefined;
  }
}
