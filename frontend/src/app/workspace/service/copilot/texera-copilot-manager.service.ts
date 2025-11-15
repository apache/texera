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
import { TexeraCopilot, ReActStep, CopilotState } from "./texera-copilot";
import { Observable, Subject, catchError, map, of, shareReplay, tap, defer, throwError, switchMap } from "rxjs";
import { AppSettings } from "../../../common/app-setting";

export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  instance: TexeraCopilot;
  createdAt: Date;
}

export interface ModelType {
  id: string;
  name: string;
  description: string;
  icon: string;
}

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
 * Texera Copilot Manager Service manages multiple AI agent instances for workflow assistance.
 *
 * This service provides centralized management for multiple copilot agents, allowing users to:
 * 1. Create and delete multiple agent instances with different LLM models
 * 2. Route messages to specific agents
 * 3. Track agent states and conversation history
 * 4. Query available LLM models from the backend
 *
 * Each agent is a separate TexeraCopilot instance with its own:
 * - Model configuration (e.g., GPT-4, Claude, etc.)
 * - Conversation history
 * - State (available, generating, stopping, unavailable)
 *
 * The service acts as a registry and coordinator, ensuring proper lifecycle management
 * and providing observable streams for agent changes and state updates.
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

  public createAgent(modelType: string, customName?: string): Observable<AgentInfo> {
    return defer(() => {
      const agentId = `agent-${++this.agentCounter}`;
      const agentName = customName || `Agent ${this.agentCounter}`;

      const agentInstance = this.createCopilotInstance(modelType);
      agentInstance.setAgentInfo(agentName);

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
   * Helper method to get an agent and execute a callback with it.
   * Handles agent lookup and error throwing if not found.
   */
  private withAgent<T>(agentId: string, callback: (agent: AgentInfo) => Observable<T>): Observable<T> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return callback(agent);
    });
  }

  public getAgent(agentId: string): Observable<AgentInfo> {
    return this.withAgent(agentId, agent => of(agent));
  }

  public getAllAgents(): Observable<AgentInfo[]> {
    return of(Array.from(this.agents.values()));
  }
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
          return of([]);
        }),
        shareReplay(1)
      );
    }
    return this.modelTypes$;
  }

  private formatModelName(modelId: string): string {
    return modelId
      .split("-")
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
  }

  public getAgentCount(): Observable<number> {
    return of(this.agents.size);
  }
  public sendMessage(agentId: string, message: string): Observable<void> {
    return this.withAgent(agentId, agent => agent.instance.sendMessage(message));
  }

  public getReActStepsObservable(agentId: string): Observable<ReActStep[]> {
    return this.withAgent(agentId, agent => agent.instance.reActSteps$);
  }

  public getAgentResponses(agentId: string): Observable<ReActStep[]> {
    return this.withAgent(agentId, agent => of(agent.instance.getReActSteps()));
  }

  public clearMessages(agentId: string): Observable<void> {
    return this.withAgent(agentId, agent => {
      agent.instance.clearMessages();
      return of(undefined);
    });
  }

  public stopGeneration(agentId: string): Observable<void> {
    return this.withAgent(agentId, agent => {
      agent.instance.stopGeneration();
      return of(undefined);
    });
  }

  public getAgentState(agentId: string): Observable<CopilotState> {
    return this.withAgent(agentId, agent => of(agent.instance.getState()));
  }

  public getAgentStateObservable(agentId: string): Observable<CopilotState> {
    return this.withAgent(agentId, agent => agent.instance.state$);
  }

  public isAgentConnected(agentId: string): Observable<boolean> {
    return this.withAgent(agentId, agent => of(agent.instance.isConnected())).pipe(catchError(() => of(false)));
  }

  public getSystemInfo(
    agentId: string
  ): Observable<{ systemPrompt: string; tools: Array<{ name: string; description: string; inputSchema: any }> }> {
    return this.withAgent(agentId, agent =>
      of({
        systemPrompt: agent.instance.getSystemPrompt(),
        tools: agent.instance.getToolsInfo(),
      })
    );
  }
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
