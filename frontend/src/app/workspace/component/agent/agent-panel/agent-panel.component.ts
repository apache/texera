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
