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
import { TexeraCopilot, ReActStep, CopilotState, ModelMessage } from "./texera-copilot";
import { Observable, Subject, catchError, map, of, shareReplay, tap, defer, throwError, filter, first } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { DynamicSchemaService } from "../dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { ValidationWorkflowService } from "../validation/validation-workflow.service";
import { AgentActionService } from "../agent-action/agent-action.service";
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
  isBaselineMode: boolean;
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
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

  /**
   * Create a new agent with the specified model type.
   * Returns an Observable that emits the created AgentInfo.
   */
  public createAgent(modelType: string, customName?: string, isBaselineMode: boolean = false): Observable<AgentInfo> {
    return defer(() => {
      const agentId = `agent-${++this.agentCounter}`;
      const agentName = customName || `Agent ${this.agentCounter}`;

      const agentInstance = this.createCopilotInstance(modelType, isBaselineMode);
      agentInstance.setAgentInfo(agentId, agentName);

      return agentInstance.initialize().pipe(
        map(() => {
          const agentInfo: AgentInfo = {
            id: agentId,
            name: agentName,
            modelType,
            isBaselineMode,
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
   * This is a fire-and-forget method that manages the subscription internally.
   * If the agent is currently generating, it will be stopped and this method will wait
   * until the agent is ready before sending the new message.
   * The agent will process the message in the background.
   *
   * @param agentId - The ID of the agent to send the message to
   * @param message - The message to send
   * @param relevantOperatorIds - Optional array of operator IDs to retrieve relevant ReAct steps for context
   */
  public sendMessage(agentId: string, message: string, relevantOperatorIds: string[] = []): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      console.error(`Agent with ID ${agentId} not found`);
      this.notificationService.error(`Agent with ID ${agentId} not found`);
      return;
    }

    // Prepare the final message with relevant steps retrieved from operator IDs
    // User's message is appended at the end for better context
    const prepareFinalMessage = (): string => {
      if (relevantOperatorIds.length === 0) {
        return message;
      }

      const retrievedSteps: any[] = [];

      // Iterate through all agents to find steps that modified these operators
      for (const [sourceAgentId, sourceAgent] of this.agents.entries()) {
        const allSteps = sourceAgent.instance.getReActSteps();

        for (const step of allSteps) {
          if (step.operatorAccess) {
            // Check if this step modified any of the relevant operators
            let hasModifiedRelevantOperator = false;
            step.operatorAccess.forEach(access => {
              for (const opId of access.modifiedOperatorIds) {
                if (relevantOperatorIds.includes(opId)) {
                  hasModifiedRelevantOperator = true;
                  break;
                }
              }
            });

            if (hasModifiedRelevantOperator) {
              retrievedSteps.push({
                agentId: sourceAgentId,
                agentName: sourceAgent.name,
                step: step,
              });
            }
          }
        }
      }

      if (retrievedSteps.length === 0) {
        return message;
      }

      // Sort by timestamp (newest first)
      retrievedSteps.sort((a, b) => b.step.timestamp.getTime() - a.step.timestamp.getTime());
      const stepsJson = JSON.stringify(retrievedSteps, null, 2);

      // Context first, then user's message at the end
      return `Relevant ReAct Steps that modified the operators (Full Data):\n\`\`\`json\n${stepsJson}\n\`\`\`\n\nUser's feedback:\n${message}`;
    };

    // Helper to actually send the message
    const doSendMessage = () => {
      const finalMessage = prepareFinalMessage();
      agent.instance.sendMessage(finalMessage).subscribe({
        error: (error: unknown) => {
          console.error(`Error sending message to agent ${agentId}:`, error);
        },
      });
    };

    // Check if agent is currently generating
    if (agent.instance.getState() === CopilotState.GENERATING) {
      // Stop the agent and wait for it to become available
      agent.instance.stopGeneration();
      this.getAgentStateObservable(agentId)
        .pipe(
          filter(state => state === CopilotState.AVAILABLE),
          first()
        )
        .subscribe(() => doSendMessage());
    } else {
      // Agent is ready, send immediately
      doSendMessage();
    }
  }

  /**
   * Get the ReActSteps observable stream.
   * Returns an Observable that emits arrays of ReActStep.
   */
  public getReActStepsObservable(agentId: string): Observable<ReActStep[]> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      return throwError(() => new Error(`Agent with ID ${agentId} not found`));
    }
    return agent.instance.reActSteps$;
  }

  /**
   * Get the current ReActSteps.
   * Returns an Observable that emits the current array of ReActStep.
   */
  public getReActSteps(agentId: string): Observable<ReActStep[]> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }
      return of(agent.instance.getReActSteps());
    });
  }

  /**
   * Clear all messages for an agent.
   * This is a fire-and-forget method.
   */
  public clearMessages(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      console.error(`Agent with ID ${agentId} not found`);
      this.notificationService.error(`Agent with ID ${agentId} not found`);
      return;
    }
    agent.instance.clearMessages();
  }

  /**
   * Stop generation for an agent.
   * This is a fire-and-forget method.
   */
  public stopGeneration(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      console.error(`Agent with ID ${agentId} not found`);
      this.notificationService.error(`Agent with ID ${agentId} not found`);
      return;
    }
    agent.instance.stopGeneration();
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
    const agent = this.agents.get(agentId);
    if (!agent) {
      return throwError(() => new Error(`Agent with ID ${agentId} not found`));
    }
    return agent.instance.state$;
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

  public setHoveredMessage(agentId: string, step: ReActStep | null): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    agent.instance.setHoveredMessage(step);
  }

  public getHoveredMessageOperatorsObservable(
    agentId: string
  ): Observable<{ viewedOperatorIds: string[]; modifiedOperatorIds: string[] }> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.hoveredMessageOperators$;
  }

  public getMessageStatsObservable(agentId: string): Observable<Map<string, any>> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.messageStats$;
  }

  /**
   * Get all model messages for a specific agent.
   * Used for exporting conversation history.
   */
  public getMessages(agentId: string): ModelMessage[] {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.getMessages();
  }

  public getAgentActionApprovalObservable(
    agentId: string
  ): Observable<{ isWaitingForApproval: boolean; agentActionId?: string }> {
    const agent = this.agents.get(agentId);
    if (!agent) {
      throw new Error(`Agent with ID ${agentId} not found`);
    }
    return agent.instance.agentActionApproval$;
  }

  /**
   * Get ReActSteps that viewed or modified a specific operator.
   * Returns two arrays: steps that viewed the operator and steps that modified it.
   */
  public getReActStepsByOperatorAccess(
    agentId: string,
    operatorId: string
  ): Observable<{ viewedBy: ReActStep[]; modifiedBy: ReActStep[] }> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }

      const allSteps = agent.instance.getReActSteps();
      const viewedBy: ReActStep[] = [];
      const modifiedBy: ReActStep[] = [];

      // Iterate through all steps and check operator access
      for (const step of allSteps) {
        if (step.operatorAccess) {
          step.operatorAccess.forEach(access => {
            if (access.viewedOperatorIds.includes(operatorId) && !viewedBy.includes(step)) {
              viewedBy.push(step);
            }
            if (access.modifiedOperatorIds.includes(operatorId) && !modifiedBy.includes(step)) {
              modifiedBy.push(step);
            }
          });
        }
      }

      return of({ viewedBy, modifiedBy });
    });
  }

  /**
   * Get ReActSteps that depend on a specific step.
   * A step B depends on step A if:
   * - Step A comes earlier (lower stepId or earlier timestamp)
   * - Step A modified some operator IDs
   * - Step B viewed or modified those same operator IDs
   */
  public getDependentReActSteps(agentId: string, messageId: string, stepId: number): Observable<ReActStep[]> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (!agent) {
        return throwError(() => new Error(`Agent with ID ${agentId} not found`));
      }

      const allSteps = agent.instance.getReActSteps();

      // Find the target step
      const targetStep = allSteps.find(s => s.messageId === messageId && s.stepId === stepId);
      if (!targetStep) {
        return of([]);
      }

      // Collect all operator IDs modified by the target step
      const modifiedOperatorIds = new Set<string>();
      if (targetStep.operatorAccess) {
        targetStep.operatorAccess.forEach(access => {
          access.modifiedOperatorIds.forEach(opId => modifiedOperatorIds.add(opId));
        });
      }

      // If target step didn't modify anything, no steps can depend on it
      if (modifiedOperatorIds.size === 0) {
        return of([]);
      }

      // Find all later steps that viewed or modified the same operators
      const dependentSteps: ReActStep[] = [];
      for (const step of allSteps) {
        // Skip if it's the same step or comes before the target step
        if (step.messageId === targetStep.messageId && step.stepId === targetStep.stepId) {
          continue;
        }

        // Check if step comes after target step (by timestamp)
        if (step.timestamp <= targetStep.timestamp) {
          continue;
        }

        // Check if this step viewed or modified any of the target step's modified operators
        let hasAccess = false;
        if (step.operatorAccess) {
          step.operatorAccess.forEach(access => {
            // Check viewedOperatorIds
            for (const opId of access.viewedOperatorIds) {
              if (modifiedOperatorIds.has(opId)) {
                hasAccess = true;
                break;
              }
            }
            // Check modifiedOperatorIds
            for (const opId of access.modifiedOperatorIds) {
              if (modifiedOperatorIds.has(opId)) {
                hasAccess = true;
                break;
              }
            }
          });
        }

        if (hasAccess) {
          dependentSteps.push(step);
        }
      }

      return of(dependentSteps);
    });
  }

  /**
   * Create a copilot instance using Angular's dependency injection.
   * Each agent receives a unique instance via a child injector.
   */
  private createCopilotInstance(modelType: string, isBaselineMode: boolean = false): TexeraCopilot {
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
    copilotInstance.setBaselineMode(isBaselineMode);

    return copilotInstance;
  }
}
