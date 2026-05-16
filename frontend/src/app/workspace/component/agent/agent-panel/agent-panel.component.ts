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
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { NgIf, NgFor } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabsComponent, NzTabBarExtraContentDirective, NzTabComponent, NzTabDirective } from "ng-zorro-antd/tabs";
import { AgentRegistrationComponent } from "./agent-registration/agent-registration.component";
import { AgentChatComponent } from "./agent-chat/agent-chat.component";

@UntilDestroy()
@Component({
  selector: "texera-agent-panel",
  templateUrl: "agent-panel.component.html",
  styleUrls: ["agent-panel.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzResizableDirective,
    NzTabsComponent,
    NzTabBarExtraContentDirective,
    NzTabComponent,
    NzTabDirective,
    AgentRegistrationComponent,
    AgentChatComponent,
    NzResizeHandlesComponent,
  ],
})
export class AgentPanelComponent implements OnInit, OnDestroy, OnChanges {
  static readonly MIN_PANEL_WIDTH = 380;

  /**
   * Optional agent ID to activate when the panel loads.
   * When provided (from agent dashboard), the panel will open
   * and switch to this agent's tab automatically.
   */
  @Input() agentIdToActivate?: string;

  // Width of the collapsed tab strip in px
  private static readonly TAB_WIDTH = 28;

  // Separate open state from width so *ngIf doesn't destroy the panel during resize
  isPanelOpen = false;

  // Sidebar width in px (only meaningful when isPanelOpen is true)
  _width: number = AgentPanelComponent.MIN_PANEL_WIDTH;
  get width(): number { return this._width; }
  set width(v: number) {
    const clamped = Math.max(AgentPanelComponent.MIN_PANEL_WIDTH, v);
    this._width = clamped;
  }

  private applyWidth(open: boolean): void {
    const panelWidth = open ? this._width : 0;
    document.body.style.paddingRight = open
      ? `${this._width}px`
      : `${AgentPanelComponent.TAB_WIDTH}px`;
    this.agentService.setAgentPanelOpen(open);
  }

  private resizeId = -1;

  // Tab management
  selectedTabIndex: number = 0; // 0 = registration tab, 1+ = agent tabs
  agents: AgentInfo[] = [];

  // Active agent tracking - only one agent can be connected at a time
  activeAgentId: string | null = null;

  constructor(
    private agentService: AgentService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.loadPanelSettings();

    // Subscribe to agent changes
    this.agentService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.agentService
        .getAllAgents()
        .pipe(untilDestroyed(this))
        .subscribe(agents => {
          this.agents = agents;
          // Try to activate the agent if agentIdToActivate is set
          this.tryActivateAgentFromInput();
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
      });

    // Open the panel when requested (e.g. on first login)
    this.agentService.openPanel$.pipe(untilDestroyed(this)).subscribe(() => {
      if (!this.isPanelOpen) {
        this.isPanelOpen = true;
        this.applyWidth(true);
      }
    });
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
    if (!this.isPanelOpen) {
      this.isPanelOpen = true;
      this.applyWidth(true);
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
    this.deactivateCurrentAgent();
    this.savePanelSettings();
    document.body.style.paddingRight = "";
  }

  /** Used by the tryActivateAgentFromInput and openPanel$ to check open state */
  get isOpen(): boolean { return this.isPanelOpen; }

  public openPanel(): void {
    this.isPanelOpen = !this.isPanelOpen;
    this.applyWidth(this.isPanelOpen);
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

    // Switch to the agent regardless of which page the user is on.
    // The agent works from any page via the chat panel — no navigation needed.
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

  onResize({ width }: NzResizeEvent): void {
    if (!width || width < AgentPanelComponent.MIN_PANEL_WIDTH) return;
    cancelAnimationFrame(this.resizeId);
    this.resizeId = requestAnimationFrame(() => {
      this._width = width;
      // Update body padding in real-time during drag
      document.body.style.paddingRight = `${width}px`;
    });
  }

  onResizeEnd(): void {
    this.savePanelSettings();
  }

  private loadPanelSettings(): void {
    const savedWidth = localStorage.getItem("agent-panel-width");
    const savedOpen = localStorage.getItem("agent-panel-open");
    if (savedWidth) {
      const w = Number(savedWidth);
      if (!isNaN(w) && w >= AgentPanelComponent.MIN_PANEL_WIDTH) {
        this._width = w;
      }
    }
    this.isPanelOpen = savedOpen === "true";
    this.applyWidth(this.isPanelOpen);
  }

  private savePanelSettings(): void {
    localStorage.setItem("agent-panel-width", String(this._width));
    localStorage.setItem("agent-panel-open", String(this.isPanelOpen));
  }
}
