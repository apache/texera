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
import { Subject, Observable } from "rxjs";
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
} from "./workflow-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import { AssistantModelMessage, generateText, type ModelMessage, stepCountIs, UserModelMessage } from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
export const AGENT_MODEL_ID = "claude-3.7";

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

  // Message stream for real-time updates
  private messageStream = new Subject<ModelMessage>();
  public readonly messages$ = this.messageStream.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowUtilService: WorkflowUtilService,
    private operatorMetadataService: OperatorMetadataService
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

  public sendMessage(message: string): Observable<string> {
    return new Observable<string>(observer => {
      if (!this.model) {
        observer.error(new Error("Copilot not initialized"));
        return;
      }

      // 1) push the user message
      const userMessage: UserModelMessage = { role: "user", content: message };
      this.messages.push(userMessage);
      this.messageStream.next(userMessage);

      // 2) define tools (your existing helpers)
      const tools = this.createWorkflowTools();

      // 3) run multi-step with stopWhen
      generateText({
        model: this.model,
        messages: this.messages, // full history
        tools,
        system: "You are Texera Copilot, an AI assistant for building and modifying data workflows.",
        stopWhen: stepCountIs(10),

        // optional: observe every completed step (tool calls + results available)
        onStepFinish: ({ text, toolCalls, toolResults, finishReason, usage }) => {
          // Log each step for debugging
          console.debug("step finished", { text, toolCalls, toolResults, finishReason, usage });

          // If there are tool calls, send a trace message to the chat
          if (toolCalls && toolCalls.length > 0) {
            const traceContent = toolCalls
              .map(tc => {
                const result = toolResults?.find(tr => tr.toolCallId === tc.toolCallId);
                return `🔧 ${tc.toolName}(${JSON.stringify(tc.args, null, 2)})\n→ ${JSON.stringify(result?.result, null, 2)}`;
              })
              .join("\n\n");

            // Send as a system-style message to the stream
            const traceMessage: AssistantModelMessage = { role: "assistant", content: `[Tool Trace]\n${traceContent}` };
            this.messageStream.next(traceMessage);
          }
        },
      })
        .then(({ text, steps, response }) => {
          // 4) append ALL messages the SDK produced this turn (assistant + tool messages)
          //    This keeps your history perfectly aligned with the SDK's internal state.
          this.messages.push(...response.messages);
          for (const m of response.messages) this.messageStream.next(m);

          // 5) optional diagnostics
          if (steps?.length) {
            const totalToolCalls = steps.flatMap(s => s.toolCalls || []).length;
            console.log(`Agent loop finished in ${steps.length} step(s), ${totalToolCalls} tool call(s).`);
          }

          observer.next(text);
          observer.complete();
        })
        .catch((err: any) => {
          const errorText = `Error: ${err?.message ?? String(err)}`;
          const assistantError: AssistantModelMessage = { role: "assistant", content: errorText };
          this.messages.push(assistantError);
          this.messageStream.next(assistantError);
          observer.next(errorText);
          observer.complete();
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
      this.operatorMetadataService
    );
    const addLinkTool = createAddLinkTool(this.workflowActionService);
    const listOperatorsTool = createListOperatorsTool(this.workflowActionService);
    const listLinksTool = createListLinksTool(this.workflowActionService);
    const listOperatorTypesTool = createListOperatorTypesTool(this.workflowUtilService);
    const getOperatorTool = createGetOperatorTool(this.workflowActionService);
    const deleteOperatorTool = createDeleteOperatorTool(this.workflowActionService);
    const deleteLinkTool = createDeleteLinkTool(this.workflowActionService);
    const setOperatorPropertyTool = createSetOperatorPropertyTool(this.workflowActionService);

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
    this.messageStream.next(message);
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
