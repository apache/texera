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
import { HttpClient } from "@angular/common/http";
import { TexeraCopilot, AgentUIMessage, CopilotState } from "./texera-copilot";
import { Observable, Subject, catchError, map, of, shareReplay, tap } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ComputingUnitStatusService } from "../computing-unit-status/computing-unit-status.service";
import { WorkflowConsoleService } from "../workflow-console/workflow-console.service";
import { AppSettings } from "../../../common/app-setting";

/**
 * Agent information for tracking created agents.
 */
export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  instance: TexeraCopilot;
  createdAt: Date;
}

/**
 * Available model types for agent creation.
 */
export interface ModelType {
  id: string;
  name: string;
  description: string;
  icon: string;
}

/**
 * LiteLLM Model API response.
 */
interface LiteLLMModel {
  id: string;
  object: string;
  created: number;
  owned_by: string;
}

interface LiteLLMModelsResponse {
  data: LiteLLMModel[];
  object: string;
}

/**
 * Service to manage multiple copilot agents.
 * Supports multi-agent workflows and agent lifecycle management.
 */
@Injectable({
  providedIn: "root",
})
export class TexeraCopilotManagerService {
  private agents = new Map<string, AgentInfo>();
  private agentCounter = 0;
  private agentChangeSubject = new Subject<void>();
  public agentChange$ = this.agentChangeSubject.asObservable();

  private modelTypes$: Observable<ModelType[]> | null = null;

  constructor(
    private injector: Injector,
    private http: HttpClient
  ) {}

  /**
   * Create a new agent with the specified model type.
   */
  public async createAgent(modelType: string, customName?: string): Promise<AgentInfo> {
    const agentId = `agent-${++this.agentCounter}`;
    const agentName = customName || `Agent ${this.agentCounter}`;

    try {
      const agentInstance = this.createCopilotInstance(modelType);
      agentInstance.setAgentInfo(agentId, agentName);
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

      return agentInfo;
    } catch (error) {
      throw error;
    }
  }

  public getAgent(agentId: string): AgentInfo | undefined {
    return this.agents.get(agentId);
  }

  public getAllAgents(): AgentInfo[] {
    return Array.from(this.agents.values());
  }

  public deleteAgent(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (agent) {
      agent.instance.disconnect();
      this.agents.delete(agentId);
      this.agentChangeSubject.next();
      return true;
    }
    return false;
  }

  /**
   * Fetch available models from the API.
   * Returns an Observable that emits the list of available models.
   * Uses shareReplay to cache the result and avoid multiple API calls.
   */
  public fetchModelTypes(): Observable<ModelType[]> {
    if (!this.modelTypes$) {
      this.modelTypes$ = this.http.get<LiteLLMModelsResponse>(`${AppSettings.getApiEndpoint()}/models`).pipe(
        map(response =>
          response.data.map((model: LiteLLMModel) => ({
            id: model.id,
            name: this.formatModelName(model.id),
            description: `Model: ${model.id}`,
            icon: "robot",
          }))
        ),
        catchError((error: unknown) => {
          console.error("Failed to fetch models from API:", error);
          // Return empty array on error
          return of([]);
        }),
        shareReplay(1) // Cache the result
      );
    }
    return this.modelTypes$;
  }

  /**
   * Format model ID into a human-readable name.
   * Example: "claude-3.7" -> "Claude 3.7"
   */
  private formatModelName(modelId: string): string {
    return modelId
      .split("-")
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  }

  public getAgentCount(): number {
    return this.agents.size;
  }

  public sendMessage(agentId: string, message: string): Observable<void> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.sendMessage(message);
  }

  public getAgentResponsesObservable(agentId: string): Observable<AgentUIMessage[]> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.agentResponses$;
  }

  public getAgentResponses(agentId: string): AgentUIMessage[] {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getAgentResponses();
  }

  public clearMessages(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.clearMessages();
  }

  public stopGeneration(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.stopGeneration();
  }

  public getAgentState(agentId: string) {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getState();
  }

  public getAgentStateObservable(agentId: string): Observable<CopilotState> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.state$;
  }

  public isAgentConnected(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) {
      return false;
    }
    return agent.instance.isConnected();
  }

  public getSystemInfo(agentId: string): {
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any }>;
  } {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return {
      systemPrompt: agent.instance.getSystemPrompt(),
      tools: agent.instance.getToolsInfo(),
    };
  }

  /**
   * Create a copilot instance using Angular's dependency injection.
   * Each agent receives a unique instance via a child injector.
   */
  private createCopilotInstance(modelType: string): TexeraCopilot {
    const childInjector = Injector.create({
      providers: [
        {
          provide: TexeraCopilot,
        },
      ],
      parent: this.injector,
    });

    const copilotInstance = childInjector.get(TexeraCopilot);
    copilotInstance.setModelType(modelType);

    return copilotInstance;
  }
}
