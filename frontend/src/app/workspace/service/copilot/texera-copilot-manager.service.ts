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
import { Observable, Subject, catchError, map, of, shareReplay, tap, defer, throwError } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { DataInconsistencyService } from "../data-inconsistency/data-inconsistency.service";
import { ActionPlanService } from "../action-plan/action-plan.service";
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
   * Returns an Observable that emits the created AgentInfo.
   */
  public createAgent(modelType: string, customName?: string): Observable<AgentInfo> {
    return defer(() => {
      const agentId = `agent-${++this.agentCounter}`;
      const agentName = customName || `Agent ${this.agentCounter}`;

      const agentInstance = this.createCopilotInstance(modelType);
      agentInstance.setAgentInfo(agentId, agentName);

      return agentInstance.initialize().pipe(
        map(() => {
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
        }),
        catchError((error: unknown) => {
          return throwError(() => error);
        })
      );
    });
  }

  /**
   * Get an agent by ID.
   * Returns an Observable that emits the AgentInfo or throws if not found.
   */
  public getAgent(agentId: string): Observable<AgentInfo> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return of(agent);
    });
  }

  /**
   * Get all agents.
   * Returns an Observable that emits the array of all AgentInfo.
   */
  public getAllAgents(): Observable<AgentInfo[]> {
    return of(Array.from(this.agents.values()));
  }

  /**
   * Delete an agent by ID.
   * Returns an Observable that emits true if deleted, false if not found.
   */
  public deleteAgent(agentId: string): Observable<boolean> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return of(false);
      }

      return agent.instance.disconnect().pipe(
        map(() => {
          this.agents.delete(agentId);
          this.agentChangeSubject.next();
          return true;
        })
      );
    });
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

  /**
   * Get the count of active agents.
   * Returns an Observable that emits the count.
   */
  public getAgentCount(): Observable<number> {
    return of(this.agents.size);
  }

  /**
   * Send a message to an agent.
   * Returns an Observable that completes when the message is processed.
   */
  public sendMessage(agentId: string, message: string): Observable<void> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return agent.instance.sendMessage(message);
    });
  }

  /**
   * Get the agent responses observable stream.
   * Returns an Observable that emits arrays of AgentUIMessage.
   */
  public getAgentResponsesObservable(agentId: string): Observable<AgentUIMessage[]> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return agent.instance.agentResponses$;
    });
  }

  /**
   * Get the current agent responses.
   * Returns an Observable that emits the current array of AgentUIMessage.
   */
  public getAgentResponses(agentId: string): Observable<AgentUIMessage[]> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return of(agent.instance.getAgentResponses());
    });
  }

  /**
   * Clear all messages for an agent.
   * Returns an Observable that completes when done.
   */
  public clearMessages(agentId: string): Observable<void> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      agent.instance.clearMessages();
      return of(undefined);
    });
  }

  /**
   * Stop generation for an agent.
   * Returns an Observable that completes when done.
   */
  public stopGeneration(agentId: string): Observable<void> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      agent.instance.stopGeneration();
      return of(undefined);
    });
  }

  /**
   * Get the current state of an agent.
   * Returns an Observable that emits the CopilotState.
   */
  public getAgentState(agentId: string): Observable<CopilotState> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return of(agent.instance.getState());
    });
  }

  /**
   * Get the state observable stream for an agent.
   * Returns an Observable that emits CopilotState changes.
   */
  public getAgentStateObservable(agentId: string): Observable<CopilotState> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return agent.instance.state$;
    });
  }

  /**
   * Check if an agent is connected.
   * Returns an Observable that emits a boolean.
   */
  public isAgentConnected(agentId: string): Observable<boolean> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return of(false);
      }
      return of(agent.instance.isConnected());
    });
  }

  public setPlanningMode(agentId: string, planningMode: boolean): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.setPlanningMode(planningMode);
  }

  public getPlanningMode(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getPlanningMode();
  }

  /**
   * Get system information for an agent.
   * Returns an Observable that emits the system prompt and tools.
   */
  public getSystemInfo(agentId: string): Observable<{
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any }>;
  }> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return of({
        systemPrompt: agent.instance.getSystemPrompt(),
        tools: agent.instance.getToolsInfo(),
      });
    });
  }

  public getRelevantOperators(agentId: string): string[] {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getRelevantOperators();
  }

  public getRelevantOperatorsObservable(agentId: string): Observable<string[]> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.relevantOperators$;
  }

  public getMessageStatsObservable(agentId: string): Observable<Map<string, any>> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.messageStats$;
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
