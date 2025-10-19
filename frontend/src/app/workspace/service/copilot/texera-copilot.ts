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
import { BehaviorSubject, Subject } from "rxjs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import {
  createAddOperatorTool,
  createAddLinkTool,
  createListOperatorsTool,
  createListLinksTool,
  createListOperatorTypesTool,
} from "./workflow-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";
import { generateText, type ModelMessage} from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
export const AGENT_MODEL_ID = "claude-3.7"

export interface CopilotMessage {
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: Date;
  toolCalls?: any[];
}

export interface CopilotState {
  isEnabled: boolean;
  isConnected: boolean;
  isProcessing: boolean;
  messages: CopilotMessage[];
  thinkingLog: string[];
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

  // State management
  private stateSubject = new BehaviorSubject<CopilotState>({
    isEnabled: false,
    isConnected: false,
    isProcessing: false,
    messages: [],
    thinkingLog: [],
  });

  public readonly state$ = this.stateSubject.asObservable();
  private messageStream = new Subject<CopilotMessage>();
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
  private async initialize(): Promise<void> {
    try {
      // 1. Connect to MCP server
      await this.connectMCP();

      // 2. Initialize OpenAI model
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy"
      }).chat(AGENT_MODEL_ID);

      this.updateState({
        isEnabled: true,
        isConnected: true,
      });

      this.addSystemMessage("Texera Copilot initialized. I can help you build and modify workflows.");
    } catch (error: unknown) {
      console.error("Failed to initialize copilot:", error);
      this.updateState({
        isEnabled: false,
        isConnected: false,
      });
      this.addSystemMessage(`Initialization failed: ${error}`);
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

  /**
   * Send a message to the copilot
   */
  public async sendMessage(message: string): Promise<void> {
    if (!this.model) {
      throw new Error("Copilot not initialized");
    }

    // Add user message
    this.addUserMessage(message);
    this.updateState({ isProcessing: true });

    try {
      // Get workflow manipulation tools
      const addOperatorTool = createAddOperatorTool(
        this.workflowActionService,
        this.workflowUtilService,
        this.operatorMetadataService
      );
      const addLinkTool = createAddLinkTool(this.workflowActionService);
      const listOperatorsTool = createListOperatorsTool(this.workflowActionService);
      const listLinksTool = createListLinksTool(this.workflowActionService);
      const listOperatorTypesTool = createListOperatorTypesTool(this.workflowUtilService);

      // Get MCP tools in AI SDK format
      // const mcpToolsForAI = this.getMCPToolsForAI();

      // Combine all tools
      const allTools = {
        // ...mcpToolsForAI,
        addOperator: addOperatorTool,
        addLink: addLinkTool,
        listOperators: listOperatorsTool,
        listLinks: listLinksTool,
        listOperatorTypes: listOperatorTypesTool,
      };

      // Generate response using Vercel AI SDK
      const response = await generateText({
        model: this.model,
        messages: this.getConversationHistory().map(msg => ({
          role: msg.role as "user" | "assistant" | "system",
          content: msg.content,
        })),
        tools: allTools,
        system: `You are Texera Copilot, an AI assistant for building and modifying data workflows.

CAPABILITIES:
1. Workflow Manipulation (Local Tools):
   - Add/delete operators
   - Connect operators with links
   - Modify operator properties
   - Auto-layout workflows
   - Add comment boxes

2. Operator Discovery (MCP Tools):
   - List available operator types
   - Get operator schemas
   - Search operators by capability
   - Get operator metadata`,
      });

      // Add assistant response
      this.addAssistantMessage(response.text, response.toolCalls);
    } catch (error: any) {
      console.error("Error processing request:", error);
      this.addSystemMessage(`Error: ${error.message}`);
    } finally {
      this.updateState({ isProcessing: false });
    }
  }

  /**
   * Get conversation history
   */
  public getConversationHistory(): CopilotMessage[] {
    return this.stateSubject.getValue().messages;
  }

  /**
   * Clear conversation
   */
  public clearConversation(): void {
    this.updateState({
      messages: [],
      thinkingLog: [],
    });

    this.addSystemMessage("Conversation cleared. Ready for new requests.");
  }

  /**
   * Get thinking/reasoning log
   */
  public getThinkingLog(): string[] {
    return this.stateSubject.getValue().thinkingLog;
  }

  /**
   * Toggle copilot enabled state
   */
  public async toggle(): Promise<void> {
    const currentState = this.stateSubject.getValue();

    if (currentState.isEnabled) {
      await this.disable();
    } else {
      await this.enable();
    }
  }

  /**
   * Enable copilot
   */
  private async enable(): Promise<void> {
    await this.initialize();
  }

  /**
   * Disable copilot
   */
  private async disable(): Promise<void> {
    // Disconnect the MCP client if it exists
    if (this.mcpClient) {
      await this.mcpClient.close();
      this.mcpClient = undefined;
    }

    this.updateState({
      isEnabled: false,
      isConnected: false,
    });

    this.addSystemMessage("Copilot disabled.");
  }

  /**
   * Get current state
   */
  public getState(): CopilotState {
    return this.stateSubject.getValue();
  }

  /**
   * Update thinking log
   */
  private updateThinkingLog(reasoning: string | string[]): void {
    const logs = Array.isArray(reasoning) ? reasoning : [reasoning];
    const timestamp = new Date().toISOString();

    const formattedLogs = logs.map(log => `[${timestamp}] ${log}`);

    const currentState = this.stateSubject.getValue();
    this.updateState({
      thinkingLog: [...currentState.thinkingLog, ...formattedLogs],
    });
  }

  // Message management helpers
  private addUserMessage(content: string): void {
    const message: CopilotMessage = {
      role: "user",
      content,
      timestamp: new Date(),
    };
    this.addMessage(message);
  }

  private addAssistantMessage(content: string, toolCalls?: any[]): void {
    const message: CopilotMessage = {
      role: "assistant",
      content,
      timestamp: new Date(),
      toolCalls,
    };
    this.addMessage(message);
  }

  private addSystemMessage(content: string): void {
    const message: CopilotMessage = {
      role: "system",
      content,
      timestamp: new Date(),
    };
    this.addMessage(message);
  }

  private addMessage(message: CopilotMessage): void {
    const currentState = this.stateSubject.getValue();
    const messages = [...currentState.messages, message];
    this.updateState({ messages });
    this.messageStream.next(message);
  }

  private updateState(partialState: Partial<CopilotState>): void {
    const currentState = this.stateSubject.getValue();
    this.stateSubject.next({
      ...currentState,
      ...partialState,
    });
  }
}
