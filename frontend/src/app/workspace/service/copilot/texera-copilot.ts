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
import { generateText, type ModelMessage } from "ai";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { AppSettings } from "../../../common/app-setting";

// API endpoints as constants
export const COPILOT_MCP_URL = "mcp";
export const AGENT_MODEL_ID = "claude-3.7";

/**
 * Simplified copilot state for UI
 */
export interface CopilotState {
  isEnabled: boolean;
  isConnected: boolean;
  isProcessing: boolean;
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

  // State management
  private stateSubject = new BehaviorSubject<CopilotState>({
    isEnabled: false,
    isConnected: false,
    isProcessing: false,
  });

  public readonly state$ = this.stateSubject.asObservable();

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
  private async initialize(): Promise<void> {
    try {
      // 1. Connect to MCP server
      await this.connectMCP();

      // 2. Initialize OpenAI model
      this.model = createOpenAI({
        baseURL: new URL(`${AppSettings.getApiEndpoint()}`, document.baseURI).toString(),
        apiKey: "dummy",
      }).chat(AGENT_MODEL_ID);

      this.updateState({
        isEnabled: true,
        isConnected: true,
      });

      console.log("Texera Copilot initialized successfully");
    } catch (error: unknown) {
      console.error("Failed to initialize copilot:", error);
      this.updateState({
        isEnabled: false,
        isConnected: false,
      });
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

  /**
   * Send a message to the copilot and get a response
   */
  public async sendMessage(message: string): Promise<string> {
    if (!this.model) {
      throw new Error("Copilot not initialized");
    }

    // Add user message to history
    const userMessage: ModelMessage = {
      role: "user",
      content: message,
    };
    this.messages.push(userMessage);
    this.messageStream.next(userMessage);

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
        messages: [...this.messages],
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

      // Add assistant response to history
      const assistantMessage: ModelMessage = {
        role: "assistant",
        content: response.text,
      };
      this.messages.push(assistantMessage);
      this.messageStream.next(assistantMessage);

      return response.text;
    } catch (error: any) {
      console.error("Error processing request:", error);
      const errorMessage = `Error: ${error.message}`;

      // Add error as assistant message
      const assistantMessage: ModelMessage = {
        role: "assistant",
        content: errorMessage,
      };
      this.messages.push(assistantMessage);
      this.messageStream.next(assistantMessage);

      return errorMessage;
    } finally {
      this.updateState({ isProcessing: false });
    }
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
   * Clear conversation history
   */
  public clearConversation(): void {
    this.messages = [];
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

    console.log("Copilot disabled");
  }

  /**
   * Get current state
   */
  public getState(): CopilotState {
    return this.stateSubject.getValue();
  }

  /**
   * Update state
   */
  private updateState(partialState: Partial<CopilotState>): void {
    const currentState = this.stateSubject.getValue();
    this.stateSubject.next({
      ...currentState,
      ...partialState,
    });
  }
}
