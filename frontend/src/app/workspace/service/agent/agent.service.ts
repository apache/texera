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

import { Injectable, NgZone } from "@angular/core";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import {
  Observable,
  Subject,
  BehaviorSubject,
  catchError,
  filter,
  map,
  of,
  shareReplay,
  defer,
  throwError,
  interval,
  switchMap,
  takeUntil,
} from "rxjs";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { AppSettings } from "../../../common/app-setting";
import { AuthService } from "../../../common/service/user/auth.service";
import { AgentState, ReActStep, ModelMessage } from "./agent-types";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";

/**
 * Agent settings for API (serializable format).
 */
export interface AgentSettingsApi {
  /** Maximum character limit for operator results (uses symmetric truncation) */
  maxOperatorResultCharLimit?: number;
  /** Maximum character limit per cell (truncates individual cell values beyond this limit) */
  maxOperatorResultCellCharLimit?: number;
  /** Serialization mode for operator results */
  operatorResultSerializationMode?: "tsv";
  /** Tool execution timeout in seconds */
  toolTimeoutSeconds?: number;
  /** Workflow execution timeout in minutes */
  executionTimeoutMinutes?: number;
  /** List of disabled tool names */
  disabledTools?: string[];
  /** Maximum number of steps per message */
  maxSteps?: number;
  /** List of allowed operator types (empty = all operators allowed) */
  allowedOperatorTypes?: string[];
}

/**
 * Agent information for tracking created agents (API version).
 */
export interface AgentInfo {
  id: string;
  name: string;
  modelType: string;
  isBaselineMode: boolean;
  createdAt: Date;
  /** State is fetched from API */
  state?: AgentState;
  delegate?: {
    userInfo: { uid: number; name: string; email: string; role: string };
    workflowId?: number;
    workflowName?: string;
  };
  /** Current agent settings */
  settings?: AgentSettingsApi;
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
 * API response types
 */
/**
 * Summary of operator execution results for annotation display.
 */
export interface OperatorResultSummary {
  state: string;
  inputTuples: number;
  outputTuples: number;
  inputPortShapes?: { portIndex: number; rows: number; columns: number }[];
  outputColumns?: number;
  error?: string;
  warnings?: string[];
  consoleLogCount?: number;
  totalRowCount?: number;
  sampleRecords?: Record<string, any>[];
  resultStatistics?: Record<string, string>;
}

interface ApiAgentInfo {
  id: string;
  name: string;
  modelType: string;
  state: string;
  createdAt: string;
  delegate?: {
    userToken: string;
    userInfo: { uid: number; name: string; email: string; role: string };
    workflowId?: number;
    workflowName?: string;
  };
  settings?: AgentSettingsApi;
}

interface ApiAgentListResponse {
  agents: ApiAgentInfo[];
}

interface ApiReActStepsResponse {
  steps: any[];
  state: string;
}

interface ApiMessageResponse {
  response: string;
  steps: any[];
  usage: { inputTokens: number; outputTokens: number; totalTokens: number };
  stats: any;
  stopped: boolean;
  error?: string;
  workflow: any;
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
 * Agent state tracking for observables
 */
interface AgentStateTracking {
  stateSubject: BehaviorSubject<AgentState>;
  reActStepsSubject: BehaviorSubject<ReActStep[]>;
  hoveredMessageSubject: BehaviorSubject<{
    viewedOperatorIds: string[];
    addedOperatorIds: string[];
    modifiedOperatorIds: string[];
  }>;
  /** Current HEAD step ID in the version tree */
  headIdSubject: BehaviorSubject<string | null>;
  workflowSubject: BehaviorSubject<Workflow | null>;
  workflowId?: number;
  stopPolling$: Subject<void>;
  /** When true, workflow updates come from WS — polling is suppressed */
  wsWorkflowActive: boolean;
  /** WebSocket connection for real-time updates */
  websocket?: WebSocket;
  /** Whether this agent is currently active (tab selected) */
  isActive: boolean;
}

/**
 * Manages the workspace's agents via the agent-service HTTP/WebSocket
 * API. Owns the local agent list, per-agent state tracking (ReAct steps, HEAD
 * pointer, workflow snapshot), and the canvas annotation toggles consumed by
 * workflow-editor.
 */
@Injectable({
  providedIn: "root",
})
export class AgentService {
  /** Base URL for agent service API */
  private readonly AGENT_API_BASE = "/api";

  /** Local cache of agent info */
  private agents = new Map<string, AgentInfo>();

  /** State tracking for each agent */
  private agentStateTracking = new Map<string, AgentStateTracking>();

  /** Subject for agent list changes */
  private agentChangeSubject = new Subject<void>();
  public agentChange$ = this.agentChangeSubject.asObservable();

  /** Cached model types */
  private modelTypes$: Observable<ModelType[]> | null = null;

  // ============================================================================
  // Canvas annotation state (port shapes, step badges, scroll-to-step)
  // ============================================================================

  /** Whether to show output port shapes (rows, columns) on operators */
  private showPortShapesSubject = new BehaviorSubject<boolean>(true);
  public showPortShapes$ = this.showPortShapesSubject.asObservable();

  /** Subject emitting scroll-to-step requests */
  private scrollToStepSubject = new Subject<{ agentId: string; messageId: string; stepId: number }>();
  public scrollToStep$ = this.scrollToStepSubject.asObservable();

  constructor(
    private http: HttpClient,
    private notificationService: NotificationService,
    private workflowPersistService: WorkflowPersistService,
    private ngZone: NgZone,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {
    // Sync local cache with backend on service initialization
    // This handles cases where the backend was restarted
    this.syncAgentsWithBackend();
  }

  /**
   * Build HTTP headers for agent-service requests.
   * Includes X-Agent-Workflow-Id for consistent hash routing in k8s.
   */
  private agentHeaders(agentId?: string): { headers: HttpHeaders } {
    let headers = new HttpHeaders();
    if (agentId) {
      const wid = this.agentStateTracking.get(agentId)?.workflowId;
      if (wid !== undefined) {
        headers = headers.set("X-Agent-Workflow-Id", String(wid));
      }
    }
    return { headers };
  }

  /**
   * Sync local agent cache with the backend.
   * Removes any agents from local cache that no longer exist on the backend.
   * This is called on service initialization and handles backend restarts.
   */
  private syncAgentsWithBackend(): void {
    this.http
      .get<ApiAgentListResponse>(`${this.AGENT_API_BASE}/agents`)
      .pipe(catchError(() => of({ agents: [] })))
      .subscribe(response => {
        const backendAgentIds = new Set(response.agents.map(a => a.id));

        // Remove any local agents that don't exist on the backend
        const localAgentIds = Array.from(this.agents.keys());
        for (const localId of localAgentIds) {
          if (!backendAgentIds.has(localId)) {
            this.agents.delete(localId);
            this.stopStatePolling(localId);
          }
        }

        // Update local cache with backend state
        for (const apiAgent of response.agents) {
          const existingAgent = this.agents.get(apiAgent.id);
          if (existingAgent) {
            // Update state from backend
            existingAgent.state = this.mapStateToAgentState(apiAgent.state);
            const tracking = this.agentStateTracking.get(apiAgent.id);
            if (tracking) {
              tracking.stateSubject.next(existingAgent.state);
            }
          }
        }

        // Notify subscribers if there were changes
        if (localAgentIds.length !== this.agents.size) {
          this.agentChangeSubject.next();
        }
      });
  }

  /**
   * Convert API state string to AgentState enum
   */
  private mapStateToAgentState(state: string): AgentState {
    switch (state) {
      case "AVAILABLE":
        return AgentState.AVAILABLE;
      case "GENERATING":
        return AgentState.GENERATING;
      case "STOPPING":
        return AgentState.STOPPING;
      case "UNAVAILABLE":
      default:
        return AgentState.UNAVAILABLE;
    }
  }

  /**
   * Convert API ReActStep to frontend ReActStep format.
   * The backend now sends ReActSteps in the aligned format, so minimal conversion is needed.
   */
  private convertApiReActStep(apiStep: any): ReActStep {
    // Convert operator access from object to Map if present
    let operatorAccess: Map<number, any> | undefined;
    if (apiStep.operatorAccess) {
      operatorAccess = new Map();
      for (const [key, value] of Object.entries(apiStep.operatorAccess)) {
        operatorAccess.set(parseInt(key), value);
      }
    }

    return {
      messageId: apiStep.messageId,
      stepId: apiStep.stepId || 0,
      timestamp: new Date(apiStep.timestamp),
      role: apiStep.role || "agent",
      content: apiStep.content || "",
      isBegin: apiStep.isBegin || false,
      isEnd: apiStep.isEnd || false,
      toolCalls: apiStep.toolCalls,
      toolResults: apiStep.toolResults?.map((tr: any) => ({
        ...tr,
        // Ensure compatibility: backend uses 'output', frontend expects 'result' or 'output'
        result: tr.output || tr.result,
        output: tr.output || tr.result,
      })),
      usage: apiStep.usage,
      inputMessages: apiStep.inputMessages,
      operatorAccess,
      // Versioning fields
      id: apiStep.id || `${apiStep.messageId}-${apiStep.stepId || 0}`,
      parentId: apiStep.parentId,
      messageSource: apiStep.messageSource,
      beforeWorkflowContent: apiStep.beforeWorkflowContent,
      afterWorkflowContent: apiStep.afterWorkflowContent,
    };
  }

  /**
   * Get or create state tracking for an agent.
   * If tracking exists but doesn't have workflowId and one is provided, updates it.
   * Note: WebSocket connection is NOT started automatically - call activateAgent() to connect.
   */
  private getOrCreateStateTracking(agentId: string, workflowId?: number): AgentStateTracking {
    let tracking = this.agentStateTracking.get(agentId);
    if (!tracking) {
      tracking = {
        stateSubject: new BehaviorSubject<AgentState>(AgentState.UNAVAILABLE),
        reActStepsSubject: new BehaviorSubject<ReActStep[]>([]),
        hoveredMessageSubject: new BehaviorSubject<{
          viewedOperatorIds: string[];
          addedOperatorIds: string[];
          modifiedOperatorIds: string[];
        }>({ viewedOperatorIds: [], addedOperatorIds: [], modifiedOperatorIds: [] }),
        headIdSubject: new BehaviorSubject<string | null>(null),
        workflowSubject: new BehaviorSubject<Workflow | null>(null),
        workflowId,
        stopPolling$: new Subject<void>(),
        wsWorkflowActive: false,
        isActive: false,
      };
      this.agentStateTracking.set(agentId, tracking);
      // Note: WebSocket connection is NOT started here - lazy initialization via activateAgent()
    } else if (workflowId && !tracking.workflowId) {
      // Tracking exists but doesn't have workflowId - update it
      tracking.workflowId = workflowId;
    }
    return tracking;
  }

  /**
   * Start workflow polling for an existing tracking.
   * Polls workflow content from backend database every second.
   * Polling is suppressed when the agent service provides workflow via WebSocket.
   */
  private startWorkflowPolling(tracking: AgentStateTracking): void {
    if (!tracking.workflowId) return;

    const wid = tracking.workflowId;
    interval(1000)
      .pipe(
        filter(() => !tracking.wsWorkflowActive),
        switchMap(() => this.workflowPersistService.retrieveWorkflow(wid).pipe(catchError(() => of(null)))),
        takeUntil(tracking.stopPolling$)
      )
      .subscribe(workflow => {
        if (workflow) {
          this.ngZone.run(() => {
            tracking.workflowSubject.next(workflow);
          });
        }
      });
  }

  /**
   * Start WebSocket connection for real-time ReActSteps updates
   */
  private startStatePolling(agentId: string, tracking: AgentStateTracking): void {
    // Build WebSocket URL
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}${this.AGENT_API_BASE}/agents/${agentId}/react`;

    const ws = new WebSocket(wsUrl);
    tracking.websocket = ws;

    ws.onmessage = event => {
      try {
        const message = JSON.parse(event.data);
        this.ngZone.run(() => {
          this.handleWebSocketMessage(agentId, tracking, message);
        });
      } catch (error) {
        console.error("Failed to parse agent WebSocket message:", error);
      }
    };

    ws.onerror = error => {
      console.error(`Agent ${agentId} WebSocket error:`, error);
    };

    ws.onclose = event => {
      // Only clean up if this is still the current websocket; otherwise a rapid
      // deactivate/reactivate may have already swapped it.
      if (tracking.websocket === ws) {
        tracking.websocket = undefined;
        if (event.code !== 1000) {
          tracking.stateSubject.next(AgentState.UNAVAILABLE);
        }
      }
    };

    // Start workflow polling if workflowId is set
    this.startWorkflowPolling(tracking);
  }

  /**
   * Handle incoming WebSocket messages
   */
  private handleWebSocketMessage(agentId: string, tracking: AgentStateTracking, message: any): void {
    switch (message.type) {
      case "init":
        // Initial state and steps
        if (message.state) {
          tracking.stateSubject.next(this.mapStateToAgentState(message.state));
        }
        if (message.steps && Array.isArray(message.steps)) {
          const steps = message.steps.map((s: any) => this.convertApiReActStep(s));
          tracking.reActStepsSubject.next(steps);
        }
        // Handle initial HEAD pointer
        if (message.headId !== undefined) {
          tracking.headIdSubject.next(message.headId);
        }
        // Handle initial workflow content from agent service (ground truth)
        if (message.workflowContent) {
          tracking.wsWorkflowActive = true;
          const workflow: Workflow = {
            ...(message.workflowMetadata || tracking.workflowSubject.getValue() || {}),
            content: message.workflowContent,
          };
          tracking.workflowSubject.next(workflow as Workflow);
        }
        // Handle initial operator results
        if (message.operatorResults) {
          this.updateOperatorResultSummaries(message.operatorResults);
        }
        break;

      case "step":
        // New step received - update existing step or append new one
        if (message.step) {
          const convertedStep = this.convertApiReActStep(message.step);
          const currentSteps = tracking.reActStepsSubject.getValue();

          // Check if step with same messageId and stepId already exists
          const existingIndex = currentSteps.findIndex(
            s => s.messageId === convertedStep.messageId && s.stepId === convertedStep.stepId
          );

          if (existingIndex >= 0) {
            // Update existing step (e.g., when isEnd changes from false to true)
            const updatedSteps = [...currentSteps];
            updatedSteps[existingIndex] = convertedStep;
            tracking.reActStepsSubject.next(updatedSteps);
          } else {
            // Append new step
            tracking.reActStepsSubject.next([...currentSteps, convertedStep]);
          }

          // Advance HEAD to the step's id (each step advances HEAD)
          if (convertedStep.id) {
            tracking.headIdSubject.next(convertedStep.id);
          }

          // If the step has afterWorkflowContent, update the workflow
          if (convertedStep.afterWorkflowContent) {
            tracking.wsWorkflowActive = true;
            const existingWorkflow = tracking.workflowSubject.getValue();
            const workflow = {
              ...(existingWorkflow || {}),
              content: convertedStep.afterWorkflowContent,
            } as Workflow;
            tracking.workflowSubject.next(workflow);
          }
        }
        break;

      case "state":
        // State update
        if (message.state) {
          tracking.stateSubject.next(this.mapStateToAgentState(message.state));
        }
        break;

      case "complete":
        // Message processing complete
        if (message.state) {
          tracking.stateSubject.next(this.mapStateToAgentState(message.state));
        }
        // Update operator results on completion
        if (message.operatorResults) {
          this.updateOperatorResultSummaries(message.operatorResults);
        }
        break;

      case "headChange":
        // HEAD moved (checkout) — update HEAD, visible steps, and workflow
        if (message.headId !== undefined) {
          tracking.headIdSubject.next(message.headId);
        }
        if (message.steps && Array.isArray(message.steps)) {
          const steps = message.steps.map((s: any) => this.convertApiReActStep(s));
          tracking.reActStepsSubject.next(steps);
        }
        // Update workflow content from agent service (ground truth)
        if (message.workflowContent) {
          tracking.wsWorkflowActive = true;
          const workflow: Workflow = {
            ...(message.workflowMetadata || tracking.workflowSubject.getValue() || {}),
            content: message.workflowContent,
          };
          tracking.workflowSubject.next(workflow as Workflow);
        }
        // Update operator results on HEAD change
        if (message.operatorResults) {
          this.updateOperatorResultSummaries(message.operatorResults);
        }
        break;

      case "error":
        // Error occurred
        console.error(`Agent ${agentId} error:`, message.error);

        // If agent not found on backend (e.g., backend restarted), clean up local state
        if (message.error === "Agent not found") {
          this.agents.delete(agentId);
          tracking.stateSubject.next(AgentState.UNAVAILABLE);
          this.stopStatePolling(agentId);
          this.agentChangeSubject.next();
          this.notificationService.warning("Agent was removed (backend may have restarted)");
        } else {
          this.notificationService.error(message.error || "Agent error occurred");
        }
        break;

      default:
        console.warn("Unknown agent WebSocket message type:", message.type);
    }
  }

  /**
   * Stop WebSocket connection and polling for an agent (internal cleanup)
   */
  private stopStatePolling(agentId: string): void {
    const tracking = this.agentStateTracking.get(agentId);
    if (tracking) {
      // Close WebSocket if open
      if (tracking.websocket) {
        tracking.websocket.close();
        tracking.websocket = undefined;
      }
      tracking.stopPolling$.next();
      tracking.stopPolling$.complete();
      this.agentStateTracking.delete(agentId);
    }
  }

  /**
   * Activate an agent - starts WebSocket connection and workflow polling.
   * Call this when the user selects an agent's tab.
   * @param agentId The agent to activate
   * @returns true if activation succeeded, false otherwise
   */
  public activateAgent(agentId: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) {
      return false;
    }

    const tracking = this.getOrCreateStateTracking(agentId, agent.delegate?.workflowId);

    if (tracking.isActive && tracking.websocket) {
      return true;
    }

    tracking.isActive = true;

    if (!tracking.websocket || tracking.websocket.readyState !== WebSocket.OPEN) {
      this.startStatePolling(agentId, tracking);
    }

    return true;
  }

  /**
   * Deactivate an agent - closes WebSocket connection and stops workflow polling.
   * Call this when the user switches away from an agent's tab.
   * @param agentId The agent to deactivate
   */
  public deactivateAgent(agentId: string): void {
    const tracking = this.agentStateTracking.get(agentId);
    if (!tracking) {
      return;
    }

    // Already inactive
    if (!tracking.isActive) {
      return;
    }

    tracking.isActive = false;

    // Close WebSocket connection
    if (tracking.websocket) {
      tracking.websocket.close();
      tracking.websocket = undefined;
    }

    // Stop workflow polling; recreate stopPolling$ for future activations.
    tracking.stopPolling$.next();
    tracking.stopPolling$ = new Subject<void>();
  }

  /**
   * Check if an agent is currently active (has WebSocket connection).
   */
  public isAgentActivelyConnected(agentId: string): boolean {
    const tracking = this.agentStateTracking.get(agentId);
    return tracking?.isActive === true && tracking?.websocket?.readyState === WebSocket.OPEN;
  }

  /**
   * Get all agents that are currently actively connected (have open WebSocket).
   * @returns Array of agent IDs that are actively connected
   */
  public getActivelyConnectedAgentIds(): string[] {
    const connectedIds: string[] = [];
    for (const [agentId, tracking] of this.agentStateTracking) {
      if (tracking.isActive && tracking.websocket?.readyState === WebSocket.OPEN) {
        connectedIds.push(agentId);
      }
    }
    return connectedIds;
  }

  /**
   * Get the workflow ID associated with an agent.
   */
  public getAgentWorkflowId(agentId: string): number | undefined {
    const agent = this.agents.get(agentId);
    return agent?.delegate?.workflowId;
  }

  /**
   * Create a new agent with the specified model type.
   * Uses the user's current auth token for delegate mode.
   * @param modelType - The LLM model type to use
   * @param customName - Optional custom name for the agent
   * @param workflowId - Optional workflow ID for delegate mode
   */
  public createAgent(modelType: string, customName?: string, workflowId?: number): Observable<AgentInfo> {
    return defer(() => {
      const userToken = AuthService.getAccessToken();

      const body: any = {
        modelType,
        name: customName,
      };

      // Include user token and workflowId for delegate mode if available
      if (userToken) {
        body.userToken = userToken;
        if (workflowId !== undefined) {
          body.workflowId = workflowId;
        }
        // Include computing unit ID for workflow execution
        const selectedUnit = this.computingUnitStatusService.getSelectedComputingUnitValue();
        if (selectedUnit) {
          body.computingUnitId = selectedUnit.computingUnit.cuid;
        }
      }

      return this.http.post<ApiAgentInfo>(`${this.AGENT_API_BASE}/agents`, body).pipe(
        map(response => {
          const agentInfo: AgentInfo = {
            id: response.id,
            name: response.name,
            modelType: response.modelType,
            isBaselineMode: false,
            createdAt: new Date(response.createdAt),
            state: this.mapStateToAgentState(response.state),
            delegate: response.delegate
              ? {
                  userInfo: response.delegate.userInfo,
                  workflowId: response.delegate.workflowId,
                  workflowName: response.delegate.workflowName,
                }
              : undefined,
            settings: response.settings,
          };

          this.agents.set(response.id, agentInfo);
          // Pass workflowId to enable workflow polling from backend database
          const tracking = this.getOrCreateStateTracking(response.id, workflowId);
          // Set the initial state from the API response (agent is AVAILABLE after creation)
          tracking.stateSubject.next(agentInfo.state || AgentState.AVAILABLE);
          this.agentChangeSubject.next();

          return agentInfo;
        }),
        catchError((error: unknown) => {
          const err = error as { error?: { error?: string }; message?: string };
          const errorMsg = err.error?.error || err.message || "Failed to create agent";
          this.notificationService.error(errorMsg);
          return throwError(() => new Error(errorMsg));
        })
      );
    });
  }

  /**
   * Get an agent by ID.
   */
  public getAgent(agentId: string): Observable<AgentInfo> {
    return defer(() => {
      const agent = this.agents.get(agentId);
      if (agent) {
        return of(agent);
      }

      // Fetch from API if not in cache
      return this.http.get<ApiAgentInfo>(`${this.AGENT_API_BASE}/agents/${agentId}`, this.agentHeaders(agentId)).pipe(
        map(response => {
          const agentInfo: AgentInfo = {
            id: response.id,
            name: response.name,
            modelType: response.modelType,
            isBaselineMode: false,
            createdAt: new Date(response.createdAt),
            state: this.mapStateToAgentState(response.state),
            delegate: response.delegate
              ? {
                  userInfo: response.delegate.userInfo,
                  workflowId: response.delegate.workflowId,
                  workflowName: response.delegate.workflowName,
                }
              : undefined,
            settings: response.settings,
          };
          this.agents.set(response.id, agentInfo);
          return agentInfo;
        }),
        catchError(() => throwError(() => new Error(`Agent with ID ${agentId} not found`)))
      );
    });
  }

  /**
   * Get all agents.
   * Also syncs local cache with backend - removes any stale agents that no longer exist on the backend.
   */
  public getAllAgents(): Observable<AgentInfo[]> {
    return this.http.get<ApiAgentListResponse>(`${this.AGENT_API_BASE}/agents`).pipe(
      map(response => {
        const agents = response.agents.map(a => ({
          id: a.id,
          name: a.name,
          modelType: a.modelType,
          isBaselineMode: false,
          createdAt: new Date(a.createdAt),
          state: this.mapStateToAgentState(a.state),
          delegate: a.delegate
            ? {
                userInfo: a.delegate.userInfo,
                workflowId: a.delegate.workflowId,
                workflowName: a.delegate.workflowName,
              }
            : undefined,
          settings: a.settings,
        }));

        // Build a set of backend agent IDs for quick lookup
        const backendAgentIds = new Set(agents.map(a => a.id));

        // Remove any local agents that don't exist on the backend
        // This handles the case when agent-service restarts
        const localAgentIds = Array.from(this.agents.keys());
        for (const localId of localAgentIds) {
          if (!backendAgentIds.has(localId)) {
            this.agents.delete(localId);
            this.stopStatePolling(localId);
          }
        }

        // Update local cache with agents from backend
        for (const agent of agents) {
          this.agents.set(agent.id, agent);
        }

        return agents;
      }),
      catchError(() => of(Array.from(this.agents.values())))
    );
  }

  /**
   * Delete an agent by ID.
   */
  public deleteAgent(agentId: string): Observable<boolean> {
    return this.http
      .delete<{ deleted: boolean }>(`${this.AGENT_API_BASE}/agents/${agentId}`, this.agentHeaders(agentId))
      .pipe(
        map(response => {
          if (response.deleted) {
            this.agents.delete(agentId);
            this.stopStatePolling(agentId);
            this.agentChangeSubject.next();
          }
          return response.deleted;
        }),
        catchError(() => {
          this.agents.delete(agentId);
          this.stopStatePolling(agentId);
          this.agentChangeSubject.next();
          return of(true);
        })
      );
  }

  /**
   * Fetch available models from the API.
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

  /**
   * Get the count of active agents.
   */
  public getAgentCount(): Observable<number> {
    return of(this.agents.size);
  }

  /**
   * Send a message to an agent via WebSocket.
   * The message is sent through the WebSocket connection for real-time streaming.
   */
  public sendMessage(agentId: string, message: string, messageSource: "chat" | "feedback" = "chat"): void {
    const agent = this.agents.get(agentId);
    if (!agent) {
      this.notificationService.error(`Agent with ID ${agentId} not found`);
      return;
    }

    const tracking = this.agentStateTracking.get(agentId);
    if (!tracking || !tracking.websocket || tracking.websocket.readyState !== WebSocket.OPEN) {
      this.notificationService.error("WebSocket connection not available");
      return;
    }

    const wsMessage = {
      type: "message",
      content: message,
      messageSource,
    };

    try {
      tracking.websocket.send(JSON.stringify(wsMessage));
    } catch (error) {
      console.error("Failed to send message to agent:", error);
      this.notificationService.error("Failed to send message");
    }
  }

  /**
   * Get the ReActSteps observable stream.
   */
  public getReActStepsObservable(agentId: string): Observable<ReActStep[]> {
    const tracking = this.getOrCreateStateTracking(agentId);
    return tracking.reActStepsSubject.asObservable();
  }

  /**
   * Get the current ReActSteps.
   */
  public getReActSteps(agentId: string): Observable<ReActStep[]> {
    return this.http
      .get<ApiReActStepsResponse>(`${this.AGENT_API_BASE}/agents/${agentId}/react-steps`, this.agentHeaders(agentId))
      .pipe(
        map(response => response.steps.map((s: any) => this.convertApiReActStep(s))),
        catchError(() => of([]))
      );
  }

  /**
   * Clear all messages for an agent.
   */
  public clearMessages(agentId: string): void {
    this.http.post(`${this.AGENT_API_BASE}/agents/${agentId}/clear`, {}, this.agentHeaders(agentId)).subscribe({
      next: () => {
        const tracking = this.agentStateTracking.get(agentId);
        if (tracking) {
          tracking.reActStepsSubject.next([]);
        }
      },
      error: (error: unknown) => {
        console.error(`Error clearing messages for agent ${agentId}:`, error);
      },
    });
  }

  /**
   * Stop generation for an agent via WebSocket.
   */
  public stopGeneration(agentId: string): void {
    const tracking = this.agentStateTracking.get(agentId);
    if (tracking?.websocket && tracking.websocket.readyState === WebSocket.OPEN) {
      // Send stop via WebSocket for immediate effect
      try {
        tracking.websocket.send(JSON.stringify({ type: "stop" }));
      } catch (error) {
        console.error("Failed to send stop command:", error);
      }
    } else {
      // Fallback to HTTP if WebSocket not available
      this.http.post(`${this.AGENT_API_BASE}/agents/${agentId}/stop`, {}, this.agentHeaders(agentId)).subscribe({
        error: (error: unknown) => {
          console.error(`Error stopping agent ${agentId}:`, error);
        },
      });
    }
  }

  /**
   * Get the current state of an agent.
   */
  public getAgentState(agentId: string): Observable<AgentState> {
    return defer(() => {
      const tracking = this.agentStateTracking.get(agentId);
      if (tracking) {
        return of(tracking.stateSubject.getValue());
      }
      return of(AgentState.UNAVAILABLE);
    });
  }

  /**
   * Get the state observable stream for an agent.
   */
  public getAgentStateObservable(agentId: string): Observable<AgentState> {
    const tracking = this.getOrCreateStateTracking(agentId);
    return tracking.stateSubject.asObservable();
  }

  /**
   * Check if an agent is connected.
   */
  public isAgentConnected(agentId: string): Observable<boolean> {
    return this.getAgentState(agentId).pipe(map(state => state !== AgentState.UNAVAILABLE));
  }

  /**
   * Get HEAD step ID observable for an agent.
   */
  public getHeadIdObservable(agentId: string): Observable<string | null> {
    const tracking = this.getOrCreateStateTracking(agentId);
    return tracking.headIdSubject.asObservable();
  }

  /**
   * Get current HEAD step ID for an agent.
   */
  public getHeadId(agentId: string): string | null {
    const tracking = this.agentStateTracking.get(agentId);
    return tracking ? tracking.headIdSubject.getValue() : null;
  }

  /**
   * Checkout to a specific step (move HEAD, restore workflow).
   * The backend broadcasts headChange + visible steps via WebSocket to all clients.
   */
  public checkoutStep(agentId: string, stepId: string): Observable<any> {
    return this.http.post(`${this.AGENT_API_BASE}/agents/${agentId}/checkout`, { stepId });
  }

  /**
   * Get visible steps for an agent (current snapshot).
   */
  public getVisibleSteps(agentId: string): ReActStep[] {
    const tracking = this.agentStateTracking.get(agentId);
    return tracking ? tracking.reActStepsSubject.getValue() : [];
  }

  /**
   * Get system information for an agent (system prompt and tools).
   * Fetches from agent-service API.
   */
  public getSystemInfo(agentId: string): Observable<{
    systemPrompt: string;
    tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
  }> {
    return this.http
      .get<{
        systemPrompt: string;
        tools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }>;
      }>(`${this.AGENT_API_BASE}/agents/${agentId}/system-info`, this.agentHeaders(agentId))
      .pipe(
        catchError(() =>
          of({
            systemPrompt: "Unable to retrieve system prompt",
            tools: [],
          })
        )
      );
  }

  /**
   * Set hovered message (local UI state).
   */
  public setHoveredMessage(agentId: string, step: ReActStep | null): void {
    const tracking = this.agentStateTracking.get(agentId);
    if (tracking) {
      if (step && step.operatorAccess) {
        const viewedOperatorIds: string[] = [];
        const addedOperatorIds: string[] = [];
        const modifiedOperatorIds: string[] = [];

        step.operatorAccess.forEach(access => {
          viewedOperatorIds.push(...access.viewedOperatorIds);
          addedOperatorIds.push(...access.addedOperatorIds);
          modifiedOperatorIds.push(...access.modifiedOperatorIds);
        });

        tracking.hoveredMessageSubject.next({
          viewedOperatorIds: [...new Set(viewedOperatorIds)],
          addedOperatorIds: [...new Set(addedOperatorIds)],
          modifiedOperatorIds: [...new Set(modifiedOperatorIds)],
        });
      } else {
        tracking.hoveredMessageSubject.next({
          viewedOperatorIds: [],
          addedOperatorIds: [],
          modifiedOperatorIds: [],
        });
      }
    }
  }

  /**
   * Get hovered message operators observable.
   */
  public getHoveredMessageOperatorsObservable(
    agentId: string
  ): Observable<{ viewedOperatorIds: string[]; addedOperatorIds: string[]; modifiedOperatorIds: string[] }> {
    const tracking = this.getOrCreateStateTracking(agentId);
    return tracking.hoveredMessageSubject.asObservable();
  }

  /**
   * Get ReActSteps that viewed or modified a specific operator.
   */
  public getReActStepsByOperatorAccess(
    agentId: string,
    operatorId: string
  ): Observable<{ viewedBy: ReActStep[]; modifiedBy: ReActStep[] }> {
    return this.getReActSteps(agentId).pipe(
      map(allSteps => {
        const viewedBy: ReActStep[] = [];
        const modifiedBy: ReActStep[] = [];

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

        return { viewedBy, modifiedBy };
      })
    );
  }

  /**
   * Get workflow observable for an agent.
   * This observable emits the full Workflow object from the backend database
   * whenever the agent's workflow changes.
   */
  public getWorkflowObservable(agentId: string): Observable<Workflow | null> {
    const tracking = this.agentStateTracking.get(agentId);
    if (tracking) {
      return tracking.workflowSubject.asObservable();
    }
    return of(null);
  }

  /**
   * Ensure workflow polling is started for an agent.
   * Call this when you have the workflowId but tracking may have been created without it.
   */
  public ensureWorkflowPolling(agentId: string, workflowId: number): void {
    this.getOrCreateStateTracking(agentId, workflowId);
  }

  /**
   * Get agent settings.
   */
  public getAgentSettings(agentId: string): Observable<AgentSettingsApi> {
    return this.http
      .get<AgentSettingsApi>(`${this.AGENT_API_BASE}/agents/${agentId}/settings`, this.agentHeaders(agentId))
      .pipe(
        catchError(() =>
          of({
            maxOperatorResultCharLimit: 20000,
            maxOperatorResultCellCharLimit: 4000,
            toolTimeoutSeconds: 120,
            executionTimeoutMinutes: 10,
            disabledTools: [],
            maxSteps: 10,
            allowedOperatorTypes: [],
          })
        )
      );
  }

  /**
   * Update agent settings.
   * Only provided values will be updated.
   */
  public updateAgentSettings(agentId: string, settings: Partial<AgentSettingsApi>): Observable<AgentSettingsApi> {
    return this.http
      .patch<AgentSettingsApi>(
        `${this.AGENT_API_BASE}/agents/${agentId}/settings`,
        settings,
        this.agentHeaders(agentId)
      )
      .pipe(
        map(response => {
          // Update local cache if we have this agent
          const agent = this.agents.get(agentId);
          if (agent) {
            agent.settings = response;
          }
          return response;
        }),
        catchError((error: unknown) => {
          const err = error as { error?: { error?: string }; message?: string };
          const errorMsg = err.error?.error || err.message || "Failed to update agent settings";
          this.notificationService.error(errorMsg);
          return throwError(() => new Error(errorMsg));
        })
      );
  }

  /**
   * Get all available operator types for an agent.
   */
  public getAvailableOperatorTypes(agentId: string): Observable<Array<{ type: string; description: string }>> {
    return this.http
      .get<
        Array<{ type: string; description: string }>
      >(`${this.AGENT_API_BASE}/agents/${agentId}/operator-types`, this.agentHeaders(agentId))
      .pipe(catchError(() => of([])));
  }

  // ============================================================================
  // Context Filtering Methods
  // ============================================================================

  /**
   * Get ReActSteps relevant to the specified operator IDs.
   * Fetches from the backend which filters steps based on which operators they affected.
   *
   * @param agentId - The agent ID
   * @param operatorIds - The operator IDs to filter by
   * @returns Observable with filtered ReActSteps
   */
  public getStepsByOperatorIds(agentId: string, operatorIds: string[]): Observable<{ steps: ReActStep[] }> {
    return this.http
      .post<{
        steps: ReActStep[];
      }>(`${this.AGENT_API_BASE}/agents/${agentId}/steps-by-operators`, { operatorIds }, this.agentHeaders(agentId))
      .pipe(
        map(response => ({
          steps: response.steps.map((s: any) => this.convertApiReActStep(s)),
        })),
        catchError(() =>
          of({
            steps: [],
          })
        )
      );
  }

  // ============================================================================
  // Canvas annotation toggles
  // ============================================================================

  /**
   * Toggle whether output port shapes are shown on operators.
   */
  public togglePortShapes(show: boolean): void {
    this.showPortShapesSubject.next(show);
  }

  public getShowPortShapes(): boolean {
    return this.showPortShapesSubject.getValue();
  }

  /**
   * Request scrolling to a specific step in the agent chat.
   */
  public requestScrollToStep(agentId: string, messageId: string, stepId: number): void {
    this.scrollToStepSubject.next({ agentId, messageId, stepId });
  }

  // ============================================================================
  // Operator Result Annotation Methods
  // ============================================================================

  /** Whether operator result annotations are currently visible */
  private resultAnnotationsVisibleSubject = new BehaviorSubject<boolean>(false);
  public resultAnnotationsVisible$ = this.resultAnnotationsVisibleSubject.asObservable();

  /** Current operator result summaries (operatorId → summary) */
  private operatorResultSummariesSubject = new BehaviorSubject<Map<string, OperatorResultSummary>>(new Map());
  public operatorResultSummaries$ = this.operatorResultSummariesSubject.asObservable();

  /**
   * Toggle operator result annotations on/off.
   * When toggling on, fetches the latest results from the active agent.
   */
  public toggleResultAnnotations(agentId?: string): void {
    const newState = !this.resultAnnotationsVisibleSubject.getValue();
    if (newState) {
      const id = agentId ?? this.getActivelyConnectedAgentIds()[0];
      if (!id) {
        // No active agent — nothing to fetch
        return;
      }
      this.fetchOperatorResults(id);
    } else {
      this.resultAnnotationsVisibleSubject.next(false);
    }
  }

  /**
   * Update operator result summaries from a WebSocket or API response.
   */
  private updateOperatorResultSummaries(results: Record<string, OperatorResultSummary>): void {
    const summaries = new Map<string, OperatorResultSummary>();
    for (const [opId, data] of Object.entries(results)) {
      summaries.set(opId, data);
    }
    this.operatorResultSummariesSubject.next(summaries);
  }

  /**
   * Fetch operator results from the backend (fallback if WebSocket data not available).
   */
  public fetchOperatorResults(agentId: string): void {
    this.http
      .get<{ results: Record<string, OperatorResultSummary> }>(
        `${this.AGENT_API_BASE}/agents/${agentId}/operator-results`,
        this.agentHeaders(agentId)
      )
      .pipe(catchError(() => of({ results: {} as Record<string, OperatorResultSummary> })))
      .subscribe(response => {
        this.updateOperatorResultSummaries(response.results);
        this.resultAnnotationsVisibleSubject.next(true);
      });
  }

  /**
   * Get current result annotations visibility.
   */
  public getResultAnnotationsVisible(): boolean {
    return this.resultAnnotationsVisibleSubject.getValue();
  }
}
