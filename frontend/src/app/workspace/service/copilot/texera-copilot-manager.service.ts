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

import { Injectable, Injector } from "@angular/core";
import { TexeraCopilot } from "./texera-copilot";
import { Subject } from "rxjs";
import { ModelMessage } from "ai";

/**
 * Agent info for tracking created agents
 */
export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  instance: TexeraCopilot;
  createdAt: Date;
  messageHistory: ModelMessage[]; // Persist conversation history using AI SDK format
}

/**
 * Available model types for agent creation
 */
export interface ModelType {
  id: string;
  name: string;
  description: string;
  icon: string;
}

/**
 * Service to manage multiple copilot agents
 * Supports multi-agent workflows and agent lifecycle management
 */
@Injectable({
  providedIn: "root",
})
export class TexeraCopilotManagerService {
  // Map from agent ID to agent info
  private agents = new Map<string, AgentInfo>();

  // Counter for generating unique agent IDs
  private agentCounter = 0;

  // Stream for agent creation/deletion events
  private agentChangeSubject = new Subject<void>();
  public agentChange$ = this.agentChangeSubject.asObservable();

  // Available model types
  private modelTypes: ModelType[] = [
    {
      id: "claude-3.7",
      name: "Claude Sonnet 3.7",
      description: "Balanced performance for workflow editing",
      icon: "thunderbolt",
    },
    {
      id: "claude-sonnet-4-5",
      name: "Claude Sonnet 4.5",
      description: "Most capable model for complex planning",
      icon: "star",
    },
  ];

  constructor(private injector: Injector) {}

  /**
   * Create a new agent with the specified model type
   */
  public async createAgent(modelType: string, customName?: string): Promise<AgentInfo> {
    const agentId = `agent-${++this.agentCounter}`;
    const agentName = customName || `Agent ${this.agentCounter}`;

    try {
      // Create new TexeraCopilot instance using Angular's Injector
      const agentInstance = this.createCopilotInstance(modelType);

      // Set agent information
      agentInstance.setAgentInfo(agentId, agentName);

      // Initialize the agent
      await agentInstance.initialize();

      const agentInfo: AgentInfo = {
        id: agentId,
        name: agentName,
        modelType,
        instance: agentInstance,
        createdAt: new Date(),
        messageHistory: [], // Initialize empty message history
      };

      this.agents.set(agentId, agentInfo);
      this.agentChangeSubject.next();

      console.log(`Created agent: ${agentId} with model ${modelType}`);
      return agentInfo;
    } catch (error) {
      console.error(`Failed to create agent with model ${modelType}:`, error);
      throw error;
    }
  }

  /**
   * Get agent by ID
   */
  public getAgent(agentId: string): AgentInfo | undefined {
    return this.agents.get(agentId);
  }

  /**
   * Get all agents
   */
  public getAllAgents(): AgentInfo[] {
    return Array.from(this.agents.values());
  }

  /**
   * Delete agent by ID
   */
  public deleteAgent(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (agent) {
      // Disconnect agent before deletion
      agent.instance.disconnect();
      this.agents.delete(agentId);
      this.agentChangeSubject.next();
      console.log(`Deleted agent: ${agentId}`);
      return true;
    }
    return false;
  }

  /**
   * Get available model types
   */
  public getModelTypes(): ModelType[] {
    return this.modelTypes;
  }

  /**
   * Get agent count
   */
  public getAgentCount(): number {
    return this.agents.size;
  }

  /**
   * Create a copilot instance with proper dependency injection
   * Uses Angular's Injector to dynamically create instances
   */
  private createCopilotInstance(modelType: string): TexeraCopilot {
    // Create a new instance using Angular's Injector
    // This automatically injects all required dependencies
    const copilotInstance = this.injector.get(TexeraCopilot);

    // Set the model type for this instance
    copilotInstance.setModelType(modelType);

    return copilotInstance;
  }
}
