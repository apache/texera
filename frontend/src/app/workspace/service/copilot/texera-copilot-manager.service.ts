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
import { TexeraCopilot, AgentResponse } from "./texera-copilot";
import { Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
// import { CopilotCoeditorService } from "./copilot-coeditor.service"; // Removed - will be replaced
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
import { ActionPlanService } from "../action-plan/action-plan.service";

/**
 * Agent info for tracking created agents
 */
export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  instance: TexeraCopilot;
  createdAt: Date;
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
   * Send a message to a specific agent by ID
   * Messages are automatically updated via the messages$ observable
   */
  public sendMessage(agentId: string, message: string): Observable<void> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.sendMessage(message);
  }

  /**
   * Get the agent responses observable for a specific agent
   * Emits the agent response list on subscribe and updates with new responses
   */
  public getAgentResponsesObservable(agentId: string): Observable<AgentResponse[]> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    console.log(`getAgentResponsesObservable for agent ${agentId}`);
    return agent.instance.agentResponses$;
  }

  /**
   * Get current agent responses snapshot for a specific agent
   */
  public getAgentResponses(agentId: string): AgentResponse[] {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getAgentResponses();
  }

  /**
   * Clear message history for a specific agent
   */
  public clearMessages(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.clearMessages();
  }

  /**
   * Stop generation for a specific agent
   */
  public stopGeneration(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.stopGeneration();
  }

  /**
   * Get agent state
   */
  public getAgentState(agentId: string) {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getState();
  }

  /**
   * Check if agent is connected
   */
  public isAgentConnected(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) {
      return false;
    }
    return agent.instance.isConnected();
  }

  /**
   * Create a copilot instance with proper dependency injection
   * Uses Angular's Injector to dynamically create instances
   * Creates a child injector to ensure each agent gets a unique instance
   */
  private createCopilotInstance(modelType: string): TexeraCopilot {
    // Create a child injector that provides TexeraCopilot with all its dependencies
    // This ensures each call creates a NEW instance instead of reusing a singleton
    const childInjector = Injector.create({
      providers: [
        {
          provide: TexeraCopilot,
          deps: [
            WorkflowActionService,
            WorkflowUtilService,
            OperatorMetadataService,
            DynamicSchemaService,
            ExecuteWorkflowService,
            WorkflowResultService,
            // CopilotCoeditorService, // Removed - will be replaced
            WorkflowCompilingService,
            ValidationWorkflowService,
            DataInconsistencyService,
            ActionPlanService,
          ],
        },
      ],
      parent: this.injector,
    });

    // Get a fresh instance from the child injector
    const copilotInstance = childInjector.get(TexeraCopilot);

    // Set the model type for this instance
    copilotInstance.setModelType(modelType);

    console.log(`Created new TexeraCopilot instance for model ${modelType}`);
    return copilotInstance;
  }
}
