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

import {
  Component,
  ViewChild,
  ElementRef,
  Input,
  OnInit,
  AfterViewChecked,
  ChangeDetectorRef,
  OnDestroy,
  OnChanges,
  SimpleChanges,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Subject } from "rxjs";
import { distinctUntilChanged, filter, pairwise, startWith, takeUntil } from "rxjs/operators";
import { CopilotState, ReActStep, CopilotMessageStats } from "../../../service/copilot/copilot-types";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import * as dagre from "dagre";

/**
 * Represents a single node in the version tree (built from ReActSteps).
 */
export interface TimelineNode {
  id: string;
  stepId: string;
  timestamp: Date;
  isHead: boolean;
  isOnHeadPath: boolean;
  /** Multi-line label: one line per tool call, or message summary */
  lines: string[];
  /** Step type for styling */
  stepType: "initial" | "user" | "agent";
  messageSource?: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

/**
 * Represents a node on the vertical time axis, horizontally aligned with a timeline node.
 */
export interface TimeAxisNode {
  /** Y position (same as the corresponding timeline node) */
  y: number;
  /** Time label in HH:MM:SS format */
  timeLabel: string;
  /** Action type for icon display */
  actionType?: string;
  /** Agent name for tooltip on robot icons */
  agentName?: string;
}

/**
 * Represents an edge between parent and child nodes in the action tree.
 */
export interface TreeEdge {
  sourceId: string;
  targetId: string;
  /** SVG path data for the edge */
  path: string;
  isOnHeadPath: boolean;
}

@UntilDestroy()
@Component({
  selector: "texera-agent-chat",
  templateUrl: "agent-chat.component.html",
  styleUrls: ["agent-chat.component.scss"],
})
export class AgentChatComponent implements OnInit, AfterViewChecked, OnDestroy, OnChanges {
  @Input() agentInfo!: AgentInfo;
  @Input() isActive: boolean = false;
  @ViewChild("messageContainer", { static: false }) messageContainer?: ElementRef;
  @ViewChild("messageInput", { static: false }) messageInput?: ElementRef;
  @ViewChild("timelineContainer", { static: false }) timelineContainer?: ElementRef;

  /** All steps (for timeline rendering) */
  public agentResponses: ReActStep[] = [];
  /** Steps on the HEAD path only (for chat rendering) */
  public visibleSteps: ReActStep[] = [];
  public currentMessage = "";
  private shouldScrollToBottom = false;
  public isDetailsModalVisible = false;
  public selectedResponse: ReActStep | null = null;
  public hoveredMessageIndex: number | null = null;
  public isSystemInfoModalVisible = false;
  public systemPrompt: string = "";
  public availableTools: Array<{ name: string; description: string; inputSchema: any; enabled: boolean }> = [];
  public agentState: CopilotState = CopilotState.UNAVAILABLE;
  public isStatsModalVisible = false;
  public messageStats: CopilotMessageStats[] = [];


  // Tree-related properties
  public timelineNodes: TimelineNode[] = [];
  public treeEdges: TreeEdge[] = [];
  public timeAxisNodes: TimeAxisNode[] = [];
  public treeCanvasWidth: number = 200;
  public treeHeight: number = 100;
  public treePanelWidth: number = 260;
  public treePanelCollapsed: boolean = false;
  public showPortShapes: boolean = true;

  // Current HEAD step ID in the version tree
  public currentHeadId: string | null = null;

  // Hover diff state — tracks which operators had their expanded layout replaced with diff
  private hoveredDiffOperatorIds: string[] = [];
  private currentHoverDiff: any = null;

  // System info modal state (with editing capabilities)
  public isEditingSystemPrompt = false;
  public editingSystemPrompt = "";
  public settingsMaxCharLimit = 20000; // Default max characters for operator results
  public settingsMaxCellCharLimit = 4000; // Default max characters per cell
  public settingsSerializationMode: "json" | "table" | "toon" = "table"; // Serialization mode for results
  public settingsToolTimeoutSeconds = 120; // 2 minutes default
  public settingsExecutionTimeoutMinutes = 10; // 10 minutes default
  public settingsMaxSteps = 10; // Default max steps per message
  public settingsAgentMode: "code" | "general" = "code"; // Agent operating mode
  public settingsFineGrainedPrompt = false; // Use fine-grained prompts with atomic operation constraints
  public settingsEnableContextOptimization = false; // Enable context optimization
  public settingsFrontierDepth = 1; // Frontier depth for context optimization
  public settingsMinimumResultCharLimit = 0; // Lower bound for log-decay trimming (0 = fully skip non-frontier)
  public settingsCacheEnabled = true; // Whether to enable operator result caching
  public settingsExecutionBackend: "texera" | "hamilton" = "texera"; // Execution backend
  public settingsLatestOnly = false; // Keep only latest tool call/result per operator
  public settingsDynamicDepthEnabled = false; // Automatically compute frontier depth
  public settingsParallelToolCalls = false; // Allow multiple tool calls per response
  public settingsOptionalResultRetrieval = false; // Make retrieveResult optional per call
  public settingsNoExecutionMetadata = false; // Omit execution metadata from results
  public settingsSimplifiedTools = false; // Do not register getCurrentWorkflow tool
  public settingsNoActionDetail = false; // Replace code/properties with placeholder in message history
  public settingsNoLogFallback = false; // Non-frontier operators use minimum limit directly
  public settingsCarryMetadata = false; // Include per-column statistics in execution metadata
  public settingsAllowedOperatorTypes: string[] = []; // Allowed operator types for general mode
  public allAvailableOperatorTypes: Array<{ type: string; description: string }> = []; // All operator types from backend
  public operatorTypeSearchQuery = ""; // Search filter for operator types
  public agentInternalState: object | null = null;
  public isLoadingAgentState = false;

  // Tool panel state
  public expandedToolName: string | null = null;

  // Step badge toggle state (legacy - kept for compatibility)
  public showStepBadges = false;

  // Message highlighting state
  public highlightedMessageId: string | null = null;

  // Track if we disabled auto-persist so we can re-enable it on destroy
  private disabledAutoPersist = false;

  // Subject to control workflow subscription lifecycle
  private stopWorkflowSubscription$ = new Subject<void>();

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private cdr: ChangeDetectorRef,
    private workflowVersionService: WorkflowVersionService,
    private workflowPersistService: WorkflowPersistService
  ) {}

  ngOnInit(): void {
    if (!this.agentInfo) {
      return;
    }

    // Ensure workflow polling is started if we have a workflowId
    // This handles agents created via API that weren't created through the UI
    const workflowId = this.agentInfo.delegate?.workflowId;
    if (workflowId) {
      this.copilotManagerService.ensureWorkflowPolling(this.agentInfo.id, workflowId);
    }

    // Get the current state from manager service
    this.copilotManagerService
      .getAgentState(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
        // Immediately trigger change detection to show the current state
        this.cdr.detectChanges();
      });

    // Then subscribe to agent state changes (BehaviorSubject will immediately emit current value)
    this.copilotManagerService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentState = state;
        // Force immediate change detection
        this.cdr.detectChanges();
      });

    // Subscribe to ReActSteps
    this.copilotManagerService
      .getReActStepsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(steps => {
        const previousLength = this.visibleSteps.length;
        this.agentResponses = steps;
        this.updateVisibleSteps();
        this.shouldScrollToBottom = true;

        // Rebuild timeline nodes whenever responses change
        this.buildTimelineNodes();

        // Automatically highlight the latest visible step
        if (this.visibleSteps.length > 0) {
          const latestIndex = this.visibleSteps.length - 1;
          const previousLatestIndex = previousLength - 1;

          if (
            this.hoveredMessageIndex === null ||
            this.hoveredMessageIndex === previousLatestIndex ||
            this.hoveredMessageIndex >= this.visibleSteps.length
          ) {
            this.setHoveredMessage(latestIndex);
          }
        }

        // Trigger change detection
        this.cdr.detectChanges();
      });

    // Subscribe to HEAD changes
    this.copilotManagerService
      .getHeadIdObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(headId => {
        this.currentHeadId = headId;
        this.updateVisibleSteps();
        this.buildTimelineNodes();
        this.cdr.detectChanges();
      });

    // Subscribe to message stats changes
    this.copilotManagerService
      .getMessageStatsObservable(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(statsMap => {
        this.messageStats = Array.from(statsMap.values());
        this.cdr.detectChanges();
      });

    // Subscribe to agent state changes to manage auto-persist
    // Disable auto-persist when agent is GENERATING, re-enable when AVAILABLE
    this.copilotManagerService
      .getAgentStateObservable(this.agentInfo.id)
      .pipe(startWith(CopilotState.UNAVAILABLE), pairwise(), untilDestroyed(this))
      .subscribe(([previousState, currentState]) => {
        // When agent starts generating, disable auto-persist
        if (currentState === CopilotState.GENERATING && previousState !== CopilotState.GENERATING) {
          this.workflowPersistService.setWorkflowPersistFlag(false);
          this.disabledAutoPersist = true;
          console.log("[AgentChat] Disabled auto-persist during agent generation");
        }

        // When agent finishes (becomes AVAILABLE from GENERATING/STOPPING), re-enable auto-persist
        if (
          currentState === CopilotState.AVAILABLE &&
          (previousState === CopilotState.GENERATING || previousState === CopilotState.STOPPING)
        ) {
          this.workflowPersistService.setWorkflowPersistFlag(true);
          this.disabledAutoPersist = false;
          console.log("[AgentChat] Re-enabled auto-persist after agent finished");
        }
      });

    // Note: Workflow subscription is started/stopped via ngOnChanges based on isActive
    // This prevents automatic workflow switching when multiple agents are running

    // Start workflow subscription if already active
    if (this.isActive) {
      this.startWorkflowSubscription();
    }

    // Subscribe to step badge toggle state (legacy - kept for compatibility)
    this.copilotManagerService.showStepBadges$.pipe(untilDestroyed(this)).subscribe(show => {
      this.showStepBadges = show;
      this.cdr.detectChanges();
    });

    // Subscribe to scroll-to-step requests
    this.copilotManagerService.scrollToStep$.pipe(untilDestroyed(this)).subscribe(({ agentId, messageId, stepId }) => {
      if (agentId === this.agentInfo.id) {
        this.scrollToStep(messageId, stepId);
      }
    });

    // Subscribe to message highlighting state
    this.copilotManagerService.highlightedMessageId$.pipe(untilDestroyed(this)).subscribe(messageId => {
      this.highlightedMessageId = messageId;
      this.cdr.detectChanges();
    });

  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["isActive"]) {
      if (this.isActive) {
        this.startWorkflowSubscription();
      } else {
        this.stopWorkflowSubscription();
      }
    }
  }

  /**
   * Start subscribing to workflow changes from the agent.
   * Only called when this agent tab is active.
   */
  private startWorkflowSubscription(): void {
    if (!this.agentInfo) {
      return;
    }

    // Stop any existing subscription first
    this.stopWorkflowSubscription$.next();

    this.copilotManagerService
      .getWorkflowObservable(this.agentInfo.id)
      .pipe(
        filter(workflow => workflow !== null),
        distinctUntilChanged((prev, curr) => {
          // Compare workflow content to avoid unnecessary reloads
          if (!prev || !curr) return false;
          return JSON.stringify(prev.content) === JSON.stringify(curr.content);
        }),
        takeUntil(this.stopWorkflowSubscription$),
        untilDestroyed(this)
      )
      .subscribe(workflow => {
        if (workflow) {
          // Reload the workflow in the workspace with preserveViewport=true
          // to keep the user's current view position
          console.log("[AgentChat] Reloading workflow from backend (active agent)");
          this.workflowActionService.reloadWorkflow(workflow, false, true);
        }
      });

    console.log(`[AgentChat] Started workflow subscription for agent ${this.agentInfo.id}`);
  }

  /**
   * Stop subscribing to workflow changes.
   * Called when switching away from this agent tab.
   */
  private stopWorkflowSubscription(): void {
    this.stopWorkflowSubscription$.next();
    console.log(`[AgentChat] Stopped workflow subscription for agent ${this.agentInfo?.id}`);
  }

  ngOnDestroy(): void {
    // Stop workflow subscription
    this.stopWorkflowSubscription$.next();
    this.stopWorkflowSubscription$.complete();

    // Re-enable auto-persist if we disabled it
    if (this.disabledAutoPersist) {
      this.workflowPersistService.setWorkflowPersistFlag(true);
      console.log("[AgentChat] Re-enabled auto-persist on component destroy");
    }
  }

  ngAfterViewChecked(): void {
    if (this.shouldScrollToBottom) {
      this.scrollToBottom();
      this.shouldScrollToBottom = false;
    }
  }

  public setHoveredMessage(index: number | null): void {
    // When unhovered (null), automatically revert to latest step
    if (index === null && this.visibleSteps.length > 0) {
      index = this.visibleSteps.length - 1;
    }

    this.hoveredMessageIndex = index;
    // Notify the copilot service about the hovered message
    const hoveredStep = index !== null && index >= 0 ? this.visibleSteps[index] : null;
    this.copilotManagerService.setHoveredMessage(this.agentInfo.id, hoveredStep);
  }

  public showResponseDetails(response: ReActStep): void {
    this.selectedResponse = response;
    this.isDetailsModalVisible = true;
  }

  public closeDetailsModal(): void {
    this.isDetailsModalVisible = false;
    this.selectedResponse = null;
  }

  public showSystemInfo(): void {
    this.refreshSystemInfo();
    this.isSystemInfoModalVisible = true;
  }

  /**
   * Refresh system info from the agent.
   */
  private refreshSystemInfo(): void {
    this.copilotManagerService
      .getSystemInfo(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(systemInfo => {
        this.systemPrompt = systemInfo.systemPrompt;
        this.availableTools = systemInfo.tools;
        this.isEditingSystemPrompt = false;
        this.editingSystemPrompt = "";
        this.expandedToolName = null;
      });

    // Fetch settings from server
    this.copilotManagerService
      .getAgentSettings(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(settings => {
        this.settingsMaxCharLimit = settings.maxOperatorResultCharLimit ?? 20000;
        this.settingsMaxCellCharLimit = settings.maxOperatorResultCellCharLimit ?? 4000;
        this.settingsSerializationMode = settings.operatorResultSerializationMode ?? "table";
        this.settingsToolTimeoutSeconds = settings.toolTimeoutSeconds ?? 120;
        this.settingsExecutionTimeoutMinutes = settings.executionTimeoutMinutes ?? 10;
        this.settingsMaxSteps = settings.maxSteps ?? 10;
        this.settingsAgentMode = settings.agentMode ?? "code";
        this.settingsFineGrainedPrompt = settings.fineGrainedPrompt ?? false;
        this.settingsEnableContextOptimization = settings.enableContextOptimization ?? false;
        this.settingsFrontierDepth = settings.frontierDepth ?? 1;
        this.settingsMinimumResultCharLimit = settings.minimumResultCharLimit ?? 0;
        this.settingsCacheEnabled = settings.cacheEnabled ?? true;
        this.settingsExecutionBackend = settings.executionBackend ?? "texera";
        this.settingsLatestOnly = settings.latestOnly ?? false;
        this.settingsDynamicDepthEnabled = settings.dynamicDepthEnabled ?? false;
        this.settingsParallelToolCalls = settings.parallelToolCalls ?? false;
        this.settingsOptionalResultRetrieval = settings.optionalResultRetrieval ?? false;
        this.settingsNoExecutionMetadata = settings.noExecutionMetadata ?? false;
        this.settingsSimplifiedTools = settings.simplifiedTools ?? false;
        this.settingsNoActionDetail = settings.noActionDetail ?? false;
        this.settingsNoLogFallback = settings.noLogFallback ?? false;
        this.settingsCarryMetadata = settings.carryMetadata ?? false;
        this.settingsAllowedOperatorTypes = settings.allowedOperatorTypes ?? [];
      });

    // Fetch all available operator types
    this.copilotManagerService
      .getAvailableOperatorTypes(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(types => {
        this.allAvailableOperatorTypes = types.sort((a, b) => a.type.localeCompare(b.type));
      });

    // Also load agent internal state
    this.loadAgentInternalState();
  }

  /**
   * Load agent internal state from the server.
   */
  public loadAgentInternalState(): void {
    this.isLoadingAgentState = true;
    this.copilotManagerService
      .getAgentInternalState(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        this.agentInternalState = state;
        this.isLoadingAgentState = false;
      });
  }

  public closeSystemInfoModal(): void {
    this.isSystemInfoModalVisible = false;
    this.isEditingSystemPrompt = false;
  }

  public showStatsModal(): void {
    this.isStatsModalVisible = true;
  }

  public closeStatsModal(): void {
    this.isStatsModalVisible = false;
  }

  public formatJson(data: any): string {
    return JSON.stringify(data, null, 2);
  }

  public getExecutionTime(stat: CopilotMessageStats): string {
    if (!stat.endTime) {
      return "Running...";
    }
    const duration = stat.endTime.getTime() - stat.startTime.getTime();
    const seconds = Math.floor(duration / 1000);
    const ms = duration % 1000;
    return `${seconds}.${ms.toString().padStart(3, "0")}s`;
  }

  public getStatusColor(status: string): string {
    switch (status) {
      case "completed":
        return "#52c41a";
      case "running":
        return "#1890ff";
      case "error":
        return "#ff4d4f";
      case "stopped":
        return "#faad14";
      default:
        return "#8c8c8c";
    }
  }

  public getToolResult(response: ReActStep, toolCallIndex: number): any {
    if (!response.toolResults || toolCallIndex >= response.toolResults.length) {
      return null;
    }
    const toolResult = response.toolResults[toolCallIndex];
    return toolResult.output || toolResult.result || toolResult;
  }

  public getToolOperatorAccess(
    response: ReActStep,
    toolCallIndex: number
  ): { viewedOperatorIds: string[]; modifiedOperatorIds: string[] } | null {
    if (!response.operatorAccess) {
      return null;
    }
    return response.operatorAccess.get(toolCallIndex) || null;
  }

  public hasOperatorAccess(response: ReActStep): boolean {
    return !!response.operatorAccess && response.operatorAccess.size > 0;
  }

  public getTotalInputTokens(): number {
    for (let i = this.visibleSteps.length - 1; i >= 0; i--) {
      const response = this.visibleSteps[i];
      if (response.usage?.inputTokens !== undefined) {
        return response.usage.inputTokens;
      }
    }
    return 0;
  }

  public getTotalOutputTokens(): number {
    for (let i = this.visibleSteps.length - 1; i >= 0; i--) {
      const response = this.visibleSteps[i];
      if (response.usage?.outputTokens !== undefined) {
        return response.usage.outputTokens;
      }
    }
    return 0;
  }

  /**
   * Send a message to the agent via the copilot manager service.
   */
  public sendMessage(): void {
    if (!this.currentMessage.trim() || !this.canSendMessage()) {
      return;
    }

    const userMessage = this.currentMessage.trim();
    this.currentMessage = "";

    // Send to copilot via manager service (fire-and-forget)
    this.copilotManagerService.sendMessage(this.agentInfo.id, userMessage);
  }

  /**
   * Check if messages can be sent (only when agent is available).
   */
  public canSendMessage(): boolean {
    return this.agentState === CopilotState.AVAILABLE;
  }

  /**
   * Get the NG-ZORRO icon type based on current agent state.
   */
  public getStateIcon(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "check-circle";
      case CopilotState.GENERATING:
      case CopilotState.STOPPING:
        return "sync";
      case CopilotState.UNAVAILABLE:
      default:
        return "close-circle";
    }
  }

  /**
   * Get the icon color based on current agent state.
   */
  public getStateIconColor(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "#52c41a";
      case CopilotState.GENERATING:
      case CopilotState.STOPPING:
        return "#1890ff";
      case CopilotState.UNAVAILABLE:
      default:
        return "#ff4d4f";
    }
  }

  /**
   * Get the tooltip text for the state icon.
   */
  public getStateTooltip(): string {
    switch (this.agentState) {
      case CopilotState.AVAILABLE:
        return "Agent is ready";
      case CopilotState.GENERATING:
        return "Agent is generating response...";
      case CopilotState.STOPPING:
        return "Agent is stopping...";
      case CopilotState.UNAVAILABLE:
        return "Agent is unavailable";
      default:
        return "Agent status unknown";
    }
  }

  public onEnterPress(event: KeyboardEvent): void {
    if (!event.shiftKey) {
      event.preventDefault();
      this.sendMessage();
    }
  }

  private scrollToBottom(): void {
    if (this.messageContainer) {
      const element = this.messageContainer.nativeElement;
      element.scrollTop = element.scrollHeight;
    }
  }

  public stopGeneration(): void {
    this.copilotManagerService.stopGeneration(this.agentInfo.id);
  }

  public clearMessages(): void {
    this.copilotManagerService.clearMessages(this.agentInfo.id);
  }

  /**
   * Export the conversation history as a JSON file in TraceContent format.
   * This format is compatible with the import/replay functionality.
   */
  public exportMessages(): void {
    if (this.visibleSteps.length === 0) {
      this.notificationService.warning("No messages to export");
      return;
    }

    // Fetch raw AI SDK messages from the backend for proper export format
    this.copilotManagerService
      .getMessages(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (response: { messages: any[] }) => {
          const messages = response.messages;
          // Find the last assistant response text for the "response" field
          let lastResponse = "";
          for (let i = messages.length - 1; i >= 0; i--) {
            const msg = messages[i];
            if (msg.role === "assistant") {
              if (typeof msg.content === "string") {
                lastResponse = msg.content;
                break;
              } else if (Array.isArray(msg.content)) {
                const textPart = msg.content.find((p: any) => p.type === "text");
                if (textPart) {
                  lastResponse = textPart.text;
                  break;
                }
              }
            }
          }

          // Export in TraceContent format (compatible with import/replay)
          const exportData = {
            response: lastResponse,
            messages: messages,
          };

          const jsonString = JSON.stringify(exportData, null, 2);
          const blob = new Blob([jsonString], { type: "application/json" });
          const url = URL.createObjectURL(blob);

          // Create a temporary link and trigger download
          const link = document.createElement("a");
          link.href = url;
          link.download = `${this.agentInfo.name}-trace-${new Date().toISOString().slice(0, 19).replace(/:/g, "-")}.json`;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

          // Clean up the URL object
          URL.revokeObjectURL(url);

          this.notificationService.success(`Exported ${messages.length} messages`);
        },
        error: (err: unknown) => {
          console.error("Failed to export messages:", err);
          this.notificationService.error("Failed to export messages");
        },
      });
  }

  /**
   * Export the ReAct steps as a JSON file.
   * Fetches steps from the backend to get clean JSON (without Map objects).
   */
  public exportReActSteps(): void {
    if (this.visibleSteps.length === 0) {
      this.notificationService.warning("No ReAct steps to export");
      return;
    }

    this.copilotManagerService
      .getReActSteps(this.agentInfo.id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (steps: ReActStep[]) => {
          // Convert steps to plain objects (handle Map -> object for operatorAccess)
          const exportSteps = steps.map(step => {
            const plain: any = { ...step };
            if (step.operatorAccess) {
              const accessObj: Record<string, any> = {};
              step.operatorAccess.forEach((value, key) => {
                accessObj[key] = value;
              });
              plain.operatorAccess = accessObj;
            }
            return plain;
          });

          const exportData = {
            agentId: this.agentInfo.id,
            agentName: this.agentInfo.name,
            modelType: this.agentInfo.modelType,
            exportedAt: new Date().toISOString(),
            stepCount: exportSteps.length,
            steps: exportSteps,
          };

          const jsonString = JSON.stringify(exportData, null, 2);
          const blob = new Blob([jsonString], { type: "application/json" });
          const url = URL.createObjectURL(blob);

          const link = document.createElement("a");
          link.href = url;
          link.download = `${this.agentInfo.name}-react-steps-${new Date().toISOString().slice(0, 19).replace(/:/g, "-")}.json`;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);

          URL.revokeObjectURL(url);

          this.notificationService.success(`Exported ${exportSteps.length} ReAct steps`);
        },
        error: (err: unknown) => {
          console.error("Failed to export ReAct steps:", err);
          this.notificationService.error("Failed to export ReAct steps");
        },
      });
  }

  public isGenerating(): boolean {
    return this.agentState === CopilotState.GENERATING;
  }

  public isAvailable(): boolean {
    return this.agentState === CopilotState.AVAILABLE;
  }

  public isConnected(): boolean {
    return this.agentState !== CopilotState.UNAVAILABLE;
  }

  public isStopping(): boolean {
    return this.agentState === CopilotState.STOPPING;
  }

  /**
   * Show diff highlighting when hovering over a tree node.
   * Uses step's beforeWorkflowContent/afterWorkflowContent directly.
   */
  public onTimelineNodeMouseEnter(node: TimelineNode): void {
    const step = this.agentResponses.find(s => s.id === node.stepId);
    if (!step?.beforeWorkflowContent || !step?.afterWorkflowContent) return;

    const diff = this.workflowVersionService.getWorkflowsDifference(
      step.beforeWorkflowContent,
      step.afterWorkflowContent
    );
    this.workflowVersionService.highlightOpVersionDiffSimple(diff, step.beforeWorkflowContent);
    this.currentHoverDiff = diff;

    // For modified operators, render code diff
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    if (paper && diff.modified?.length) {
      for (const opId of diff.modified) {
        const beforeOp = step.beforeWorkflowContent.operators?.find(
          (o: OperatorPredicate) => o.operatorID === opId
        );
        const afterOp = step.afterWorkflowContent.operators?.find(
          (o: OperatorPredicate) => o.operatorID === opId
        );
        if (beforeOp && afterOp) {
          this.jointUIService.applyDiffLayout(paper, opId, beforeOp, afterOp);
          this.hoveredDiffOperatorIds.push(opId);
        }
      }
    }
  }

  /**
   * Clear diff highlighting when mouse leaves.
   */
  public onTimelineNodeMouseLeave(): void {
    this.clearHoverDiff();
  }

  /**
   * Clear any active hover diff highlighting.
   */
  private clearHoverDiff(): void {
    if (this.currentHoverDiff) {
      this.workflowVersionService.unhighlightOpVersionDiff(this.currentHoverDiff);
      this.currentHoverDiff = null;
    }

    // Restore normal expanded layout for operators that had diff view
    if (this.hoveredDiffOperatorIds.length > 0) {
      const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
      if (paper) {
        for (const opId of this.hoveredDiffOperatorIds) {
          try {
            const graph = this.workflowActionService.getTexeraGraph();
            const operator = graph.getOperator(opId);
            if (operator) {
              this.jointUIService.applyExpandedLayout(paper, opId, operator);
            }
          } catch {
            // Operator may have been removed from graph (e.g., after checkout)
          }
        }
      }
      this.hoveredDiffOperatorIds = [];
    }
  }

  // =====================
  // Timeline Methods
  // =====================

  /** Sentinel ID for the initial step — must match backend constant. */
  private static readonly INITIAL_STEP_ID = "step-initial";

  /**
   * Recompute visibleSteps: only steps on the ancestor path from root to HEAD.
   */
  private updateVisibleSteps(): void {
    if (!this.currentHeadId || this.agentResponses.length === 0) {
      this.visibleSteps = this.agentResponses;
      return;
    }
    // Walk parentId chain from HEAD to root to build the ancestor set
    const stepMap = new Map(this.agentResponses.map(s => [s.id, s]));
    const ancestorIds = new Set<string>();
    let current: string | undefined = this.currentHeadId;
    while (current) {
      ancestorIds.add(current);
      current = stepMap.get(current)?.parentId;
    }
    this.visibleSteps = this.agentResponses.filter(s => ancestorIds.has(s.id));
  }

  /**
   * Build timeline nodes from visible ReActSteps.
   */
  private buildTimelineNodes(): void {
    const steps = this.agentResponses;

    // Filter steps to those that should appear as nodes
    const visibleSteps = steps.filter(step => {
      if (step.role === "user") return true;
      if (step.role === "agent" && step.toolCalls && step.toolCalls.length > 0) return true;
      if (step.role === "agent" && step.isEnd) return true;
      return false;
    });

    if (visibleSteps.length === 0 && this.currentHeadId !== AgentChatComponent.INITIAL_STEP_ID) {
      this.timelineNodes = [];
      this.treeEdges = [];
      this.timeAxisNodes = [];
      this.treeCanvasWidth = 200;
      this.treeHeight = 100;
      return;
    }

    // Create synthetic initial step
    const earliestTime = visibleSteps.length > 0
      ? new Date(Math.min(...visibleSteps.map(s => new Date(s.timestamp).getTime())) - 1)
      : new Date();
    const initialStep: ReActStep = {
      id: AgentChatComponent.INITIAL_STEP_ID,
      messageId: "",
      stepId: -1,
      timestamp: earliestTime,
      role: "agent",
      content: "",
      isBegin: false,
      isEnd: false,
    };

    // Merge: prepend the initial step
    const allSteps = [initialStep, ...visibleSteps.filter(s => s.id !== AgentChatComponent.INITIAL_STEP_ID)];

    // Build the HEAD ancestor path for highlighting
    const headPath = new Set<string>();
    if (this.currentHeadId) {
      const stepMap = new Map(allSteps.map(s => [s.id, s]));
      let current: string | undefined = this.currentHeadId;
      while (current) {
        headPath.add(current);
        current = stepMap.get(current)?.parentId;
      }
    }

    // Sort steps chronologically
    const sortedSteps = [...allSteps].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );

    const stepChronIndex = new Map<string, number>();
    sortedSteps.forEach((step, idx) => stepChronIndex.set(step.id, idx));

    // Constants for layout
    const uniformNodeWidth = 90;
    const constantRowSpacing = 40;
    const marginX = 16;
    const marginY = 16;
    const timeAxisWidth = 72;

    // Use dagre for horizontal positioning
    const g = new dagre.graphlib.Graph();
    g.setGraph({
      rankdir: "TB",
      nodesep: 24,
      ranksep: constantRowSpacing,
      marginx: marginX + timeAxisWidth,
      marginy: marginY,
    });
    g.setDefaultEdgeLabel(() => ({}));

    for (const step of allSteps) {
      const lines = this.getStepLines(step);
      const nodeHeight = Math.max(24, lines.length * 18);
      g.setNode(step.id, { width: uniformNodeWidth, height: nodeHeight });
    }

    const stepIds = new Set(allSteps.map(s => s.id));
    for (const step of allSteps) {
      if (step.parentId && stepIds.has(step.parentId)) {
        g.setEdge(step.parentId, step.id);
      }
    }

    dagre.layout(g);

    // Override Y positions with constant spacing
    const nodePositions = new Map<string, { x: number; y: number }>();
    for (const step of allSteps) {
      const dagreNode = g.node(step.id);
      const chronIdx = stepChronIndex.get(step.id) ?? 0;
      const y = marginY + chronIdx * constantRowSpacing + 12;
      nodePositions.set(step.id, { x: dagreNode.x, y });
    }

    // Build TimelineNodes
    this.timelineNodes = allSteps.map(step => {
      const pos = nodePositions.get(step.id)!;
      const lines = this.getStepLines(step);
      const nodeHeight = Math.max(24, lines.length * 18);
      const stepType: "initial" | "user" | "agent" =
        step.id === AgentChatComponent.INITIAL_STEP_ID ? "initial" :
        step.role === "user" ? "user" : "agent";
      return {
        id: step.id,
        stepId: step.id,
        timestamp: step.timestamp,
        isHead: step.id === this.currentHeadId,
        isOnHeadPath: headPath.has(step.id),
        lines,
        stepType,
        messageSource: step.messageSource,
        x: pos.x,
        y: pos.y,
        width: uniformNodeWidth,
        height: nodeHeight,
      };
    });

    // Build time axis nodes
    this.timeAxisNodes = sortedSteps.map((step, idx) => {
      const y = marginY + idx * constantRowSpacing + 12;
      const d = new Date(step.timestamp);
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      const ss = String(d.getSeconds()).padStart(2, "0");
      const actionType = step.id === AgentChatComponent.INITIAL_STEP_ID ? "initial" :
        step.role === "user" ? "user_request" : "agent_response";
      return {
        y,
        timeLabel: `${hh}:${mm}:${ss}`,
        actionType,
        agentName: this.agentInfo?.name || "Agent",
      };
    });

    // Build edges with SVG paths
    const nodeMap = new Map(this.timelineNodes.map(n => [n.id, n]));
    this.treeEdges = g.edges().map(e => {
      const source = nodeMap.get(e.v)!;
      const target = nodeMap.get(e.w)!;
      const isOnPath = headPath.has(e.v) && headPath.has(e.w);
      const halfH = source.height / 2;
      const targetHalfH = target.height / 2;
      const path =
        source.x === target.x
          ? `M ${source.x} ${source.y + halfH} L ${target.x} ${target.y - targetHalfH}`
          : `M ${source.x} ${source.y + halfH} C ${source.x} ${(source.y + target.y) / 2}, ${target.x} ${(source.y + target.y) / 2}, ${target.x} ${target.y - targetHalfH}`;
      return { sourceId: e.v, targetId: e.w, path, isOnHeadPath: isOnPath };
    });

    // Compute total tree canvas dimensions
    const maxX = Math.max(...this.timelineNodes.map(n => n.x + n.width / 2), 200);
    const maxY = this.timeAxisNodes.length > 0
      ? this.timeAxisNodes[this.timeAxisNodes.length - 1].y + 24
      : 100;
    this.treeCanvasWidth = Math.max(200, maxX + marginX);
    this.treeHeight = maxY + marginY;
  }

  /** Get display lines for a step in the timeline. */
  private getStepLines(step: ReActStep): string[] {
    if (step.id === AgentChatComponent.INITIAL_STEP_ID) return ["Ready"];
    if (step.role === "user") {
      return [step.messageSource === "feedback" ? "Feedback" : "Input Task"];
    }
    if (step.toolCalls && step.toolCalls.length > 0) {
      return step.toolCalls.map((tc: any) => {
        const opId = tc.input?.operatorId || "";
        const toolLabel = this.getToolLabel(tc.toolName);
        return opId ? `${toolLabel} ${opId}` : toolLabel;
      });
    }
    if (step.isEnd) return ["Task Done"];
    return ["Thinking..."];
  }

  /** Get short label for a tool name. */
  private getToolLabel(toolName: string): string {
    switch (toolName) {
      case "addOperator":
      case "createOrModifyOperator":
        return "Add";
      case "modifyOperator":
        return "Modify";
      case "deleteFromWorkflow":
        return "Delete";
      case "executeOperator":
        return "Execute";
      default:
        return toolName;
    }
  }

  /**
   * Handle click on a timeline node.
   * Checkout to that step (move HEAD). The backend broadcasts a headChange WS message
   * which includes the workflow content.
   */
  public onTimelineNodeClick(node: TimelineNode): void {
    this.clearHoverDiff();

    this.copilotManagerService.checkoutStep(this.agentInfo.id, node.stepId).subscribe({
      next: () => console.log(`[Timeline] Checked out step ${node.stepId}`),
      error: err => console.error("[Timeline] Checkout failed:", err),
    });
  }

  /** Start resizing the tree panel by dragging the handle. */
  public togglePortShapes(): void {
    this.showPortShapes = !this.showPortShapes;
    this.copilotManagerService.togglePortShapes(this.showPortShapes);
  }

  public onResizeStart(event: MouseEvent): void {
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = this.treePanelWidth;

    const onMouseMove = (e: MouseEvent) => {
      const delta = e.clientX - startX;
      this.treePanelWidth = Math.max(120, Math.min(500, startWidth + delta));
    };

    const onMouseUp = () => {
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }

  /**
   * Scroll chat messages to a specific step index.
   */
  private scrollToMessage(stepIndex: number): void {
    if (!this.messageContainer) {
      return;
    }

    const container = this.messageContainer.nativeElement;
    const messages = container.querySelectorAll(".message");

    if (stepIndex >= 0 && stepIndex < messages.length) {
      messages[stepIndex].scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }


  // =====================
  // System Info Modal Editing Methods
  // =====================

  /**
   * Start editing the system prompt.
   */
  public startEditingSystemPrompt(): void {
    this.editingSystemPrompt = this.systemPrompt;
    this.isEditingSystemPrompt = true;
  }

  /**
   * Cancel editing the system prompt.
   */
  public cancelEditingSystemPrompt(): void {
    this.isEditingSystemPrompt = false;
    this.editingSystemPrompt = "";
  }

  /**
   * Save the edited system prompt.
   * Note: System prompt is managed server-side in API mode.
   */
  public saveSystemPrompt(): void {
    // In API mode, system prompt is managed server-side
    this.systemPrompt = this.editingSystemPrompt;
    this.isEditingSystemPrompt = false;
    this.notificationService.info("System prompt editing is managed server-side");
  }

  /**
   * Reset system prompt to default.
   * Note: System prompt is managed server-side in API mode.
   */
  public resetSystemPromptToDefault(): void {
    // In API mode, system prompt is managed server-side
    this.refreshSystemInfo();
    this.notificationService.info("System prompt is managed server-side");
  }

  /**
   * Toggle a specific tool's enabled state.
   * Note: Tool settings are managed server-side in API mode.
   */
  public toggleToolEnabled(tool: { name: string; enabled: boolean }): void {
    // In API mode, tool settings are managed server-side
    this.notificationService.info("Tool settings are managed server-side");
  }

  /**
   * Enable all tools.
   * Note: Tool settings are managed server-side in API mode.
   */
  public enableAllTools(): void {
    // In API mode, tool settings are managed server-side
    this.notificationService.info("Tool settings are managed server-side");
  }

  /**
   * Disable all tools.
   * Note: Tool settings are managed server-side in API mode.
   */
  public disableAllTools(): void {
    // In API mode, tool settings are managed server-side
    this.notificationService.info("Tool settings are managed server-side");
  }

  /**
   * Get count of enabled tools.
   */
  public getEnabledToolsCount(): number {
    return this.availableTools.filter(t => t.enabled).length;
  }

  /**
   * Save the max character limit.
   */
  public saveMaxCharLimit(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        maxOperatorResultCharLimit: this.settingsMaxCharLimit,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max character limit saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the max cell character limit.
   */
  public saveMaxCellCharLimit(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        maxOperatorResultCellCharLimit: this.settingsMaxCellCharLimit,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max cell character limit saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the serialization mode.
   */
  public saveSerializationMode(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        operatorResultSerializationMode: this.settingsSerializationMode,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            `Result serialization mode set to ${this.settingsSerializationMode.toUpperCase()}`
          ),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the tool execution timeout.
   */
  public saveToolTimeout(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        toolTimeoutSeconds: this.settingsToolTimeoutSeconds,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Tool timeout saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the workflow execution timeout.
   */
  public saveExecutionTimeout(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        executionTimeoutMinutes: this.settingsExecutionTimeoutMinutes,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Execution timeout saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the max steps per message setting.
   */
  public saveMaxSteps(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        maxSteps: this.settingsMaxSteps,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Max steps saved"),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the agent mode setting.
   */
  public saveAgentMode(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        agentMode: this.settingsAgentMode,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsAgentMode === "code" ? "Code mode enabled" : "General mode enabled"
          ),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the fine-grained prompt setting.
   */
  public saveFineGrainedPrompt(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        fineGrainedPrompt: this.settingsFineGrainedPrompt,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsFineGrainedPrompt ? "Fine-grained prompts enabled" : "Standard prompts enabled"
          ),
        error: () => {}, // Error already handled by service
      });
  }

  /**
   * Save the context optimization setting.
   */
  public saveContextOptimization(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        enableContextOptimization: this.settingsEnableContextOptimization,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsEnableContextOptimization ? "Context optimization enabled" : "Context optimization disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the frontier depth setting.
   */
  public saveFrontierDepth(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        frontierDepth: this.settingsFrontierDepth,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Frontier depth saved"),
        error: () => {},
      });
  }

  /**
   * Save the minimum result character limit setting (lower bound for log-decay).
   */
  public saveMinimumResultCharLimit(): void {
    if (this.settingsMinimumResultCharLimit > this.settingsMaxCharLimit) {
      this.notificationService.error(
        `Minimum result limit (${this.settingsMinimumResultCharLimit}) cannot exceed max operator result limit (${this.settingsMaxCharLimit})`
      );
      return;
    }
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        minimumResultCharLimit: this.settingsMinimumResultCharLimit,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => this.notificationService.success("Minimum result character limit saved"),
        error: () => {},
      });
  }

  /**
   * Save the cache enabled setting.
   */
  public saveCacheEnabled(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        cacheEnabled: this.settingsCacheEnabled,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsCacheEnabled ? "Operator result caching enabled" : "Operator result caching disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the execution backend setting.
   */
  public saveExecutionBackend(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        executionBackend: this.settingsExecutionBackend,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsExecutionBackend === "hamilton"
              ? "Execution backend set to Hamilton"
              : "Execution backend set to Texera"
          ),
        error: () => {},
      });
  }

  /**
   * Save the latest-only filter setting.
   */
  public saveLatestOnly(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        latestOnly: this.settingsLatestOnly,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsLatestOnly ? "Latest-only filter enabled" : "Latest-only filter disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the dynamic depth enabled setting.
   */
  public saveDynamicDepthEnabled(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        dynamicDepthEnabled: this.settingsDynamicDepthEnabled,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsDynamicDepthEnabled ? "Dynamic depth enabled" : "Dynamic depth disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the parallel tool calls setting.
   */
  public saveParallelToolCalls(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        parallelToolCalls: this.settingsParallelToolCalls,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsParallelToolCalls ? "Parallel tool calls enabled" : "Parallel tool calls disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the optional result retrieval setting.
   */
  public saveOptionalResultRetrieval(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        optionalResultRetrieval: this.settingsOptionalResultRetrieval,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsOptionalResultRetrieval
              ? "Optional result retrieval enabled"
              : "Optional result retrieval disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the no execution metadata setting.
   */
  public saveNoExecutionMetadata(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        noExecutionMetadata: this.settingsNoExecutionMetadata,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsNoExecutionMetadata ? "Execution metadata hidden" : "Execution metadata shown"
          ),
        error: () => {},
      });
  }

  /**
   * Save the no action detail setting.
   */
  public saveNoActionDetail(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        noActionDetail: this.settingsNoActionDetail,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsNoActionDetail ? "No action detail enabled" : "No action detail disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the no log fallback setting.
   */
  public saveNoLogFallback(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        noLogFallback: this.settingsNoLogFallback,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsNoLogFallback ? "No log fallback enabled" : "No log fallback disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the carry metadata setting.
   */
  public saveCarryMetadata(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        carryMetadata: this.settingsCarryMetadata,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsCarryMetadata
              ? "Carry metadata enabled"
              : "Carry metadata disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Save the simplified tools setting.
   */
  public saveSimplifiedTools(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        simplifiedTools: this.settingsSimplifiedTools,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () =>
          this.notificationService.success(
            this.settingsSimplifiedTools ? "Simplified tools enabled" : "Simplified tools disabled"
          ),
        error: () => {},
      });
  }

  /**
   * Toggle an operator type in the allowed list and save.
   */
  public toggleOperatorType(operatorType: string, enabled: boolean): void {
    if (enabled) {
      if (!this.settingsAllowedOperatorTypes.includes(operatorType)) {
        this.settingsAllowedOperatorTypes = [...this.settingsAllowedOperatorTypes, operatorType];
      }
    } else {
      this.settingsAllowedOperatorTypes = this.settingsAllowedOperatorTypes.filter(t => t !== operatorType);
    }
    this.saveAllowedOperatorTypes();
  }

  /**
   * Check if an operator type is enabled (in allowed list).
   */
  public isOperatorTypeEnabled(operatorType: string): boolean {
    return this.settingsAllowedOperatorTypes.includes(operatorType);
  }

  /**
   * Enable all operator types.
   */
  public enableAllOperatorTypes(): void {
    this.settingsAllowedOperatorTypes = this.allAvailableOperatorTypes.map(op => op.type);
    this.saveAllowedOperatorTypes();
  }

  /**
   * Deselect all operator types.
   */
  public deselectAllOperatorTypes(): void {
    this.settingsAllowedOperatorTypes = [];
    this.saveAllowedOperatorTypes();
  }

  /**
   * Get filtered operator types based on search query.
   */
  public getFilteredOperatorTypes(): Array<{ type: string; description: string }> {
    if (!this.operatorTypeSearchQuery) {
      return this.allAvailableOperatorTypes;
    }
    const query = this.operatorTypeSearchQuery.toLowerCase();
    return this.allAvailableOperatorTypes.filter(
      op => op.type.toLowerCase().includes(query) || op.description.toLowerCase().includes(query)
    );
  }

  /**
   * Save allowed operator types to backend.
   */
  private saveAllowedOperatorTypes(): void {
    this.copilotManagerService
      .updateAgentSettings(this.agentInfo.id, {
        allowedOperatorTypes: this.settingsAllowedOperatorTypes,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          const count = this.settingsAllowedOperatorTypes.length;
          this.notificationService.success(count === 0 ? "All operators enabled" : `${count} operators enabled`);
        },
        error: () => {},
      });
  }

  /**
   * Handle tool panel expand/collapse.
   */
  public onToolPanelChange(toolName: string, expanded: boolean): void {
    this.expandedToolName = expanded ? toolName : null;
  }

  /**
   * Format tool input schema for display.
   * Handles Zod schemas by extracting their JSON schema representation.
   */
  public formatToolSchema(schema: any): string {
    try {
      // Check if it's a Zod schema (has _def property)
      if (schema && schema._def) {
        // Extract the shape from Zod object schema
        if (schema._def.typeName === "ZodObject" && schema._def.shape) {
          const shape = typeof schema._def.shape === "function" ? schema._def.shape() : schema._def.shape;
          const properties: Record<string, any> = {};

          for (const [key, value] of Object.entries(shape)) {
            properties[key] = this.extractZodSchemaInfo(value);
          }

          return JSON.stringify({ type: "object", properties }, null, 2);
        }
        // For other Zod types, try to extract basic info
        return JSON.stringify(this.extractZodSchemaInfo(schema), null, 2);
      }

      // If it's already a plain object (JSON schema), stringify directly
      return JSON.stringify(schema, null, 2);
    } catch (e) {
      return "Unable to display schema";
    }
  }

  /**
   * Extract schema information from a Zod schema definition.
   */
  private extractZodSchemaInfo(zodSchema: any): any {
    if (!zodSchema || !zodSchema._def) {
      return { type: "unknown" };
    }

    const def = zodSchema._def;
    const result: any = {};

    // Add description if available
    if (def.description) {
      result.description = def.description;
    }

    switch (def.typeName) {
      case "ZodString":
        result.type = "string";
        break;
      case "ZodNumber":
        result.type = "number";
        break;
      case "ZodBoolean":
        result.type = "boolean";
        break;
      case "ZodArray":
        result.type = "array";
        if (def.type) {
          result.items = this.extractZodSchemaInfo(def.type);
        }
        break;
      case "ZodObject":
        result.type = "object";
        if (def.shape) {
          const shape = typeof def.shape === "function" ? def.shape() : def.shape;
          result.properties = {};
          for (const [key, value] of Object.entries(shape)) {
            result.properties[key] = this.extractZodSchemaInfo(value);
          }
        }
        break;
      case "ZodOptional":
        const innerOptional = this.extractZodSchemaInfo(def.innerType);
        return { ...innerOptional, optional: true };
      case "ZodDefault":
        const innerDefault = this.extractZodSchemaInfo(def.innerType);
        return { ...innerDefault, default: def.defaultValue?.() };
      case "ZodEnum":
        result.type = "enum";
        result.values = def.values;
        break;
      default:
        result.type = def.typeName?.replace("Zod", "").toLowerCase() || "unknown";
    }

    return result;
  }

  // =====================
  // Step Badge Feature Methods
  // =====================

  /**
   * Toggle step badges visibility on operators.
   */
  public toggleStepBadges(): void {
    this.showStepBadges = !this.showStepBadges;
    this.copilotManagerService.toggleStepBadges(this.showStepBadges);
  }

  /**
   * Scroll to a specific step in the chat by messageId and stepId.
   */
  private scrollToStep(messageId: string, stepId: number): void {
    // Find the step index in visibleSteps
    const stepIndex = this.visibleSteps.findIndex(step => step.messageId === messageId && step.stepId === stepId);

    if (stepIndex >= 0) {
      this.scrollToMessage(stepIndex);
      // Highlight the message briefly
      this.setHoveredMessage(stepIndex);
    }
  }

  // =====================
  // Message Highlighting Methods
  // =====================

  /**
   * Check if a message is currently highlighted for region display.
   */
  public isMessageHighlighted(messageId: string): boolean {
    return this.highlightedMessageId === messageId;
  }

  /**
   * Toggle highlighting for a message.
   * When highlighted, all operators affected by this message's steps
   * will be shown with a region highlight on the canvas.
   */
  public toggleMessageHighlight(messageId: string): void {
    if (this.highlightedMessageId === messageId) {
      // Toggle off
      this.copilotManagerService.setHighlightedMessage(null);
    } else {
      // Toggle on - highlight this message
      this.copilotManagerService.setHighlightedMessage(messageId);
    }
  }
}
