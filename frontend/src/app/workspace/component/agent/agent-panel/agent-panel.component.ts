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

import { Component, HostListener, Input, OnDestroy, OnInit, OnChanges, SimpleChanges } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzResizeEvent, NzResizableDirective, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { AgentService, AgentInfo } from "../../../service/agent/agent.service";
import { AgentPanelControlService } from "../../../service/agent/agent-panel-control.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { calculateTotalTranslate3d } from "../../../../common/util/panel-dock";
import { NgIf, NgClass, NgFor } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzTabsComponent, NzTabBarExtraContentDirective, NzTabComponent, NzTabDirective } from "ng-zorro-antd/tabs";
import { AgentRegistrationComponent } from "./agent-registration/agent-registration.component";
import { AgentChatComponent } from "./agent-chat/agent-chat.component";
import { FormlyRepeatDndComponent } from "../../../../common/formly/repeat-dnd/repeat-dnd.component";

/** localStorage key — value is a JSON map of `{ [workflowId: number]: agentId: string }`. */
const ACTIVE_AGENT_BY_WORKFLOW_STORAGE_KEY = "agent-panel-active-agent-by-workflow";

@UntilDestroy()
@Component({
  selector: "texera-agent-panel",
  templateUrl: "agent-panel.component.html",
  styleUrls: ["agent-panel.component.scss"],
  imports: [
    NgIf,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    CdkDrag,
    NzResizableDirective,
    NzMenuDirective,
    NgClass,
    NzMenuItemComponent,
    CdkDragHandle,
    NzTabsComponent,
    NzTabBarExtraContentDirective,
    NzTabComponent,
    NzTabDirective,
    AgentRegistrationComponent,
    NgFor,
    AgentChatComponent,
    NzResizeHandlesComponent,
    FormlyRepeatDndComponent,
  ],
})
export class AgentPanelComponent implements OnInit, OnDestroy, OnChanges {
  protected readonly window = window;
  private static readonly MIN_PANEL_WIDTH = 400;
  private static readonly MIN_PANEL_HEIGHT = 450;

  /**
   * Optional agent ID to activate when the panel loads.
   * When provided (from agent dashboard), the panel will open
   * and switch to this agent's tab automatically.
   */
  @Input() agentIdToActivate?: string;

  // Panel dimensions and position
  width: number = 0; // Start with 0 to show docked button
  height = Math.max(AgentPanelComponent.MIN_PANEL_HEIGHT, window.innerHeight * 0.7);
  id = -1;
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  isDocked = true;

  // Tab management
  selectedTabIndex: number = 0; // 0 = registration tab, 1+ = agent tabs
  agents: AgentInfo[] = [];

  // Active agent tracking - only one agent can be connected at a time
  activeAgentId: string | null = null;

  constructor(
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private agentPanelControlService: AgentPanelControlService
  ) {}

  ngOnInit(): void {
    this.loadPanelSettings();

    // Listen for external toggle requests (e.g., from the floating assistant button).
    // openPanel() broadcasts the new state internally, so subscribers stay in sync.
    this.agentPanelControlService.toggleRequest$.pipe(untilDestroyed(this)).subscribe(() => {
      this.openPanel();
      // Opening the panel is a good moment to restore the previously selected agent
      // if init-time restore didn't catch it (e.g., agents loaded after ngOnInit).
      if (this.width > 0 && !this.activeAgentId) {
        this.restoreLastActiveAgent();
      }
    });

    // Sync initial state to the control service so subscribers know the starting value.
    this.agentPanelControlService.setOpenState(this.width > 0);

    // Subscribe to agent changes
    this.agentService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.agentService
        .getAllAgents()
        .pipe(untilDestroyed(this))
        .subscribe(agents => {
          this.agents = agents;
          // Try to activate the agent if agentIdToActivate is set
          this.tryActivateAgentFromInput();
          // If still no active agent, retry the localStorage restore — agents
          // might've shown up after the initial load.
          if (!this.activeAgentId) {
            this.restoreLastActiveAgent();
          }
        });
    });

    // Load initial agents
    this.agentService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => {
        this.agents = agents;
        // Try to activate the agent if agentIdToActivate is set
        this.tryActivateAgentFromInput();
        // Otherwise, restore the agent previously bound to *this* workflow.
        if (!this.activeAgentId) {
          this.restoreLastActiveAgent();
        }
      });

    // Workflow metadata can load (or change to a different workflow) after the
    // panel is already mounted. When that happens, deactivate the current agent
    // (so we don't leave the wrong binding active) and restore the one bound to
    // the new workflow.
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
        const targetAgentId = this.getStoredAgentIdForWorkflow(wid);
        if (this.activeAgentId && this.activeAgentId !== targetAgentId) {
          this.deactivateCurrentAgent();
        }
        if (!this.activeAgentId) this.restoreLastActiveAgent();
      });
  }

  /**
   * Restore the agent previously bound to the *current* workflow. Falls back to
   * the registration tab when there's no saved binding for this workflow, when
   * the bound agent no longer exists, or when no workflow id is available yet.
   */
  private restoreLastActiveAgent(): void {
    const currentWorkflowId = this.workflowActionService.getWorkflowMetadata()?.wid;
    const storedAgentId = this.getStoredAgentIdForWorkflow(currentWorkflowId);
    console.log(
      `[AgentPanel] restoreLastActiveAgent: wid=${currentWorkflowId}, storedId=${storedAgentId}`
    );
    if (!storedAgentId) {
      // No binding for this workflow → drop the user on the registration tab so
      // they can either pick a different agent or create a new one.
      this.selectedTabIndex = 0;
      return;
    }
    const agentIndex = this.agents.findIndex(a => a.id === storedAgentId);
    console.log(
      `[AgentPanel] restoreLastActiveAgent: agentIndex = ${agentIndex}, agents.length = ${this.agents.length}`
    );
    if (agentIndex === -1) {
      // Stale binding — the agent was deleted elsewhere. Clean it up.
      this.clearStoredAgentIdForWorkflow(currentWorkflowId);
      this.selectedTabIndex = 0;
      return;
    }
    const agent = this.agents[agentIndex];
    this.switchToAgent(agent.id, agentIndex + 1); // +1 because tab 0 is registration
  }

  // ----- Per-workflow agent binding (localStorage map) -----

  private loadAgentByWorkflowMap(): Record<string, string> {
    try {
      const raw = localStorage.getItem(ACTIVE_AGENT_BY_WORKFLOW_STORAGE_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, string>;
      }
    } catch {
      // Ignore malformed storage.
    }
    return {};
  }

  private saveAgentByWorkflowMap(map: Record<string, string>): void {
    try {
      localStorage.setItem(ACTIVE_AGENT_BY_WORKFLOW_STORAGE_KEY, JSON.stringify(map));
    } catch {
      // Storage unavailable; ignore.
    }
  }

  private getStoredAgentIdForWorkflow(wid: number | undefined): string | undefined {
    if (wid === undefined) return undefined;
    const map = this.loadAgentByWorkflowMap();
    return map[String(wid)];
  }

  private setStoredAgentIdForWorkflow(wid: number | undefined, agentId: string): void {
    if (wid === undefined) return; // Unsaved workflows aren't persisted.
    const map = this.loadAgentByWorkflowMap();
    map[String(wid)] = agentId;
    this.saveAgentByWorkflowMap(map);
  }

  private clearStoredAgentIdForWorkflow(wid: number | undefined): void {
    if (wid === undefined) return;
    const map = this.loadAgentByWorkflowMap();
    if (delete map[String(wid)]) this.saveAgentByWorkflowMap(map);
  }

  /** Remove a given agentId from every workflow binding (used on agent delete). */
  private clearStoredAgentEverywhere(agentId: string): void {
    const map = this.loadAgentByWorkflowMap();
    let changed = false;
    for (const key of Object.keys(map)) {
      if (map[key] === agentId) {
        delete map[key];
        changed = true;
      }
    }
    if (changed) this.saveAgentByWorkflowMap(map);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["agentIdToActivate"] && this.agentIdToActivate) {
      this.tryActivateAgentFromInput();
    }
  }

  /**
   * Try to activate the agent specified by agentIdToActivate input.
   * Opens the panel and switches to the agent's tab.
   */
  private tryActivateAgentFromInput(): void {
    if (!this.agentIdToActivate || this.agents.length === 0) {
      return;
    }

    const agentIndex = this.agents.findIndex(agent => agent.id === this.agentIdToActivate);
    if (agentIndex === -1) {
      return;
    }

    // Open the panel if it's closed
    if (this.width === 0) {
      this.width = AgentPanelComponent.MIN_PANEL_WIDTH;
    }

    // Switch to the agent's tab and activate it
    const agent = this.agents[agentIndex];

    // Deactivate previous agent if any
    if (this.activeAgentId) {
      this.agentService.deactivateAgent(this.activeAgentId);
    }

    // Activate the specified agent
    this.activeAgentId = agent.id;
    this.agentService.activateAgent(agent.id);
    this.selectedTabIndex = agentIndex + 1; // +1 because tab 0 is registration

    // Clear the input so we don't re-activate on every change
    this.agentIdToActivate = undefined;
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    // Deactivate any active agent before destroying
    this.deactivateCurrentAgent();
    this.savePanelSettings();
  }

  /**
   * Open the panel from docked state
   */
  public openPanel(): void {
    if (this.width === 0) {
      // Open panel
      this.width = AgentPanelComponent.MIN_PANEL_WIDTH;
    } else {
      // Close panel (dock it)
      this.width = 0;
      this.isDocked = true;
    }
    // Notify the floating assistant button so it can show/hide itself.
    this.agentPanelControlService.setOpenState(this.width > 0);
  }

  /**
   * Handle agent creation - activates and switches to the new agent
   */
  public onAgentCreated(agentId: string): void {
    // Deactivate previous agent if any
    if (this.activeAgentId) {
      this.agentService.deactivateAgent(this.activeAgentId);
    }

    // Set the new agent as active immediately
    this.activeAgentId = agentId;
    this.agentService.activateAgent(agentId);
    const currentWorkflowId = this.workflowActionService.getWorkflowMetadata()?.wid;
    this.setStoredAgentIdForWorkflow(currentWorkflowId, agentId);

    // Fetch the latest agent list and switch to the new agent's tab
    this.agentService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe(agents => {
        this.agents = agents;
        const agentIndex = agents.findIndex(agent => agent.id === agentId);
        if (agentIndex !== -1) {
          this.selectedTabIndex = agentIndex + 1; // +1 because tab 0 is registration
        }
      });
  }

  /**
   * Handle tab selection change - validates workflow compatibility before switching
   */
  public onTabSelectChange(index: number): void {
    // Tab 0 is registration - always allow
    if (index === 0) {
      this.deactivateCurrentAgent();
      this.selectedTabIndex = 0;
      return;
    }

    // Get the agent for this tab (index - 1 because tab 0 is registration)
    const agentIndex = index - 1;
    if (agentIndex < 0 || agentIndex >= this.agents.length) {
      return;
    }

    const agent = this.agents[agentIndex];
    const agentWorkflowId = agent.delegate?.workflowId;
    const currentWorkflowId = this.workflowActionService.getWorkflowMetadata().wid;

    // If agent has a workflow ID, check if it matches the current workflow
    if (agentWorkflowId !== undefined && agentWorkflowId !== 0) {
      if (currentWorkflowId !== agentWorkflowId) {
        // Block switching - workflow mismatch
        this.notificationService.warning(
          `Cannot switch to agent "${agent.name}": It's working on a different workflow. ` +
            `Open workflow #${agentWorkflowId} to interact with this agent.`
        );
        return;
      }
    }

    // Workflow matches or agent has no workflow - allow switch
    this.switchToAgent(agent.id, index);
  }

  /**
   * Switch to a specific agent tab
   */
  private switchToAgent(agentId: string, tabIndex: number): void {
    // Skip if already on this agent and tab
    if (this.activeAgentId === agentId && this.selectedTabIndex === tabIndex) {
      return;
    }

    // Deactivate previous agent only if switching to a different agent
    if (this.activeAgentId !== agentId) {
      this.deactivateCurrentAgent();
    }

    // Activate new agent
    this.activeAgentId = agentId;
    this.agentService.activateAgent(agentId);
    this.selectedTabIndex = tabIndex;
    // Remember the binding for the *current* workflow so each workflow has its
    // own restored agent on re-entry.
    const currentWorkflowId = this.workflowActionService.getWorkflowMetadata()?.wid;
    this.setStoredAgentIdForWorkflow(currentWorkflowId, agentId);
  }

  /**
   * Deactivate the currently active agent
   */
  private deactivateCurrentAgent(): void {
    if (this.activeAgentId) {
      this.agentService.deactivateAgent(this.activeAgentId);
      this.activeAgentId = null;
    }
  }

  /**
   * Check if an agent's workflow matches the current workspace workflow
   */
  public canSwitchToAgent(agent: AgentInfo): boolean {
    const agentWorkflowId = agent.delegate?.workflowId;
    if (agentWorkflowId === undefined || agentWorkflowId === 0) {
      return true; // Agent has no workflow - always allow
    }
    const currentWorkflowId = this.workflowActionService.getWorkflowMetadata().wid;
    return currentWorkflowId === agentWorkflowId;
  }

  /**
   * Delete an agent
   */
  public deleteAgent(agentId: string, event: Event): void {
    event.stopPropagation(); // Prevent tab switch

    if (confirm("Are you sure you want to delete this agent?")) {
      const agentIndex = this.agents.findIndex(agent => agent.id === agentId);

      // Deactivate if this is the active agent
      if (this.activeAgentId === agentId) {
        this.deactivateCurrentAgent();
      }

      // Remove this agent from any workflow bindings so we don't try to restore it.
      this.clearStoredAgentEverywhere(agentId);

      // Must subscribe to the observable for it to execute
      this.agentService
        .deleteAgent(agentId)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            // If we're on the deleted agent's tab, switch to registration
            if (agentIndex !== -1 && this.selectedTabIndex === agentIndex + 1) {
              this.selectedTabIndex = 0;
            } else if (this.selectedTabIndex > agentIndex + 1) {
              // Adjust selected index if we deleted a tab before the current one
              this.selectedTabIndex--;
            }
          },
          error: (error: unknown) => {
            console.error("Failed to delete agent:", error);
          },
        });
    }
  }

  /**
   * Handle panel resize
   */
  onResize({ width, height }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
    });
  }

  /**
   * Handle drag start
   */
  handleDragStart(): void {
    this.isDocked = false;
  }

  /**
   * Load panel settings from localStorage
   */
  private loadPanelSettings(): void {
    const savedWidth = localStorage.getItem("agent-panel-width");
    const savedHeight = localStorage.getItem("agent-panel-height");
    const savedStyle = localStorage.getItem("agent-panel-style");
    const savedDocked = localStorage.getItem("agent-panel-docked");

    // Only restore width if the panel was not docked
    if (savedDocked === "false" && savedWidth) {
      const parsedWidth = Number(savedWidth);
      if (!isNaN(parsedWidth) && parsedWidth >= AgentPanelComponent.MIN_PANEL_WIDTH) {
        this.width = parsedWidth;
      }
    }

    if (savedHeight) {
      const parsedHeight = Number(savedHeight);
      if (!isNaN(parsedHeight) && parsedHeight >= AgentPanelComponent.MIN_PANEL_HEIGHT) {
        this.height = parsedHeight;
      }
    }

    if (savedStyle) {
      const container = document.getElementById("agent-container");
      if (container) {
        container.style.cssText = savedStyle;
        const translates = container.style.transform;
        const [xOffset, yOffset] = calculateTotalTranslate3d(translates);
        this.returnPosition = { x: -xOffset, y: -yOffset };
        this.isDocked = this.dragPosition.x === this.returnPosition.x && this.dragPosition.y === this.returnPosition.y;
      }
    }
  }

  /**
   * Save panel settings to localStorage
   */
  private savePanelSettings(): void {
    localStorage.setItem("agent-panel-width", String(this.width));
    localStorage.setItem("agent-panel-height", String(this.height));
    localStorage.setItem("agent-panel-docked", String(this.width === 0));

    const container = document.getElementById("agent-container");
    if (container) {
      localStorage.setItem("agent-panel-style", container.style.cssText);
    }
  }
}
