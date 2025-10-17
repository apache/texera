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
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { Mastra, Agent } from "@mastra/core";
import { MCPClient } from "@mastra/mcp";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { createWorkflowTools } from "./workflow-tools";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { createOpenAI } from "@ai-sdk/openai";

// API endpoints as constants
export const COPILOT_MCP_URL = "api/mcp";
export const COPILOT_AGENT_URL = "api/copilot/agent";

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
 * Texera Copilot - A Mastra-based AI assistant for workflow manipulation
 * Combines MCP tools from backend with frontend workflow manipulation tools
 */
@Injectable({
  providedIn: "root",
})
export class TexeraCopilot {
  private mastra: Mastra;
  private agent?: Agent; // Store agent reference
  private mcpClient?: MCPClient; // Keep reference for cleanup
  private mcpTools: Record<string, any> = {};

  // State management (integrated from service)
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
    private operatorMetadataService: OperatorMetadataService
  ) {
    // Initialize Mastra
    this.mastra = new Mastra();

    // Initialize on construction
    this.initialize();
  }

  /**
   * Initialize the copilot with MCP and local tools
   */
  private async initialize(): Promise<void> {
    try {
      // 1. Connect to MCP server using Mastra's MCP client
      await this.connectMCP();

      // 2. Get workflow manipulation tools
      const workflowTools = createWorkflowTools(this.workflowActionService, this.operatorMetadataService);

      // 3. Combine MCP tools with local workflow tools
      const allTools = {
        ...this.mcpTools,
        ...workflowTools,
      };

      // 4. Create and store the agent
      this.agent = new Agent({
        name: "texera-copilot",
        instructions: `You are Texera Copilot, an AI assistant for building and modifying data workflows.

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
   - Get operator metadata

CURRENT WORKFLOW STATE:
- Operators: ${this.workflowActionService.getTexeraGraph().getAllOperators().length}
- Links: ${this.workflowActionService.getTexeraGraph().getAllLinks().length}
- Workflow Name: ${this.workflowActionService.getWorkflowMetadata().name}

GUIDELINES:
- Use meaningful operator IDs (e.g., csv_source_1, filter_age, aggregate_results)
- Validate operator types before adding them
- Ensure proper port connections when creating links
- Use MCP tools to discover available operators first
- Suggest auto-layout after adding multiple operators

WORKFLOW PATTERNS:
- ETL Pipeline: Source → Transform → Sink
- Filtering: Source → Filter → Results
- Aggregation: Source → GroupBy → Aggregate → Results
- Join: Source1 + Source2 → Join → Results`,
        tools: allTools,
        model: createOpenAI({
          baseURL: COPILOT_AGENT_URL,
        })("gpt-4"),
      });

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
      // Create MCP client with HTTP server configuration
      // Note: Auth headers can be added if needed in the future
      const mcpClient = new MCPClient({
        servers: {
          texeraMcp: {
            url: new URL(`${window.location.origin}/${COPILOT_MCP_URL}`),
          },
        },
        timeout: 30000, // 30 second timeout
      });

      // Store reference for cleanup
      this.mcpClient = mcpClient;

      // Get all available MCP tools
      const tools = await mcpClient.getTools();

      // The tools from MCP are already in the correct format
      this.mcpTools = tools || {};

      console.log(`Connected to MCP server. Retrieved ${Object.keys(this.mcpTools).length} tools.`);
    } catch (error) {
      console.error("Failed to connect to MCP:", error);
      throw error;
    }
  }

  /**
   * Send a message to the copilot
   */
  public async sendMessage(message: string): Promise<void> {
    // Check if agent is initialized
    if (!this.agent) {
      throw new Error("Copilot agent not initialized");
    }

    // Add user message
    this.addUserMessage(message);
    this.updateState({ isProcessing: true });

    try {
      // Use Mastra's built-in message handling
      const response = await this.agent.generate(message);

      // Add assistant response
      this.addAssistantMessage(response.text, response.toolCalls);

      // Update thinking log if available
      if (response.reasoning && Array.isArray(response.reasoning)) {
        // Extract text content from reasoning chunks
        const reasoningTexts = response.reasoning.map((chunk: any) =>
          typeof chunk === "string" ? chunk : chunk.content || JSON.stringify(chunk)
        );
        this.updateThinkingLog(reasoningTexts);
      }
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

    // Reset agent conversation

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
      await this.mcpClient.disconnect();
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

  /**
   * Execute a workflow suggestion (convenience method)
   */
  public async suggestWorkflow(description: string): Promise<void> {
    const prompt = `Create a workflow for: ${description}

Please analyze the requirement and:
1. First, list the operators needed
2. Create the workflow step by step
3. Connect the operators appropriately
4. Add helpful comments

Start by checking what operator types are available.`;

    await this.sendMessage(prompt);
  }

  /**
   * Analyze current workflow (convenience method)
   */
  public async analyzeWorkflow(): Promise<void> {
    const prompt = `Analyze the current workflow and provide:
1. A summary of what it does
2. Any potential issues or improvements
3. Missing connections or operators
4. Performance optimization suggestions

Start by getting the workflow statistics and all operators.`;

    await this.sendMessage(prompt);
  }

  /**
   * Get auth token from session/local storage
   */
  private getAuthToken(): string {
    // This should get the actual JWT token from your auth service
    return sessionStorage.getItem("authToken") || "";
  }
}
