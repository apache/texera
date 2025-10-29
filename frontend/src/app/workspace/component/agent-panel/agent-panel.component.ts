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

import { Component, HostListener, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { TexeraCopilotManagerService, AgentInfo } from "../../service/copilot/texera-copilot-manager.service";

@UntilDestroy()
@Component({
  selector: "texera-agent-panel",
  templateUrl: "agent-panel.component.html",
  styleUrls: ["agent-panel.component.scss"],
})
export class AgentPanelComponent implements OnInit, OnDestroy {
  protected readonly window = window;
  private static readonly MIN_PANEL_WIDTH = 400;
  private static readonly MIN_PANEL_HEIGHT = 450;

  // Panel dimensions and position
  width = AgentPanelComponent.MIN_PANEL_WIDTH;
  height = Math.max(AgentPanelComponent.MIN_PANEL_HEIGHT, window.innerHeight * 0.7);
  id = -1;
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };
  isDocked = true;

  // Tab management
  selectedTabIndex: number = 0; // 0 = registration tab, 1+ = agent tabs
  agents: AgentInfo[] = [];

  constructor(private copilotManagerService: TexeraCopilotManagerService) {}

  ngOnInit(): void {
    // Load saved panel dimensions and position
    const savedWidth = localStorage.getItem("agent-panel-width");
    const savedHeight = localStorage.getItem("agent-panel-height");
    const savedStyle = localStorage.getItem("agent-panel-style");

    if (savedWidth) this.width = Number(savedWidth);
    if (savedHeight) this.height = Number(savedHeight);

    if (savedStyle) {
      const container = document.getElementById("agent-container");
      if (container) {
        container.style.cssText = savedStyle;
        const translates = container.style.transform;
        const [xOffset, yOffset] = this.calculateTotalTranslate3d(translates);
        this.returnPosition = { x: -xOffset, y: -yOffset };
        this.isDocked = this.dragPosition.x === this.returnPosition.x && this.dragPosition.y === this.returnPosition.y;
      }
    }

    // Subscribe to agent changes
    this.copilotManagerService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.agents = this.copilotManagerService.getAllAgents();
    });

    // Load initial agents
    this.agents = this.copilotManagerService.getAllAgents();
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    // Save panel state
    localStorage.setItem("agent-panel-width", String(this.width));
    localStorage.setItem("agent-panel-height", String(this.height));

    const container = document.getElementById("agent-container");
    if (container) {
      localStorage.setItem("agent-panel-style", container.style.cssText);
    }
  }

  /**
   * Handle agent creation
   */
  public onAgentCreated(agentId: string): void {
    // The agent is already added to the agents array by the manager service
    // Find the index of the newly created agent and switch to that tab
    // Tab index 0 is registration, so agent tabs start at index 1
    const agentIndex = this.agents.findIndex(agent => agent.id === agentId);
    if (agentIndex !== -1) {
      this.selectedTabIndex = agentIndex + 1; // +1 because tab 0 is registration
    }
  }

  /**
   * Delete an agent
   */
  public deleteAgent(agentId: string, event: Event): void {
    event.stopPropagation(); // Prevent tab switch

    if (confirm("Are you sure you want to delete this agent?")) {
      const agentIndex = this.agents.findIndex(agent => agent.id === agentId);
      this.copilotManagerService.deleteAgent(agentId);

      // If we're on the deleted agent's tab, switch to registration
      if (agentIndex !== -1 && this.selectedTabIndex === agentIndex + 1) {
        this.selectedTabIndex = 0;
      } else if (this.selectedTabIndex > agentIndex + 1) {
        // Adjust selected index if we deleted a tab before the current one
        this.selectedTabIndex--;
      }
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
   * Reset panel to docked position
   */
  resetPanelPosition(): void {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
    this.isDocked = true;
  }

  /**
   * Handle drag start
   */
  handleDragStart(): void {
    this.isDocked = false;
  }

  /**
   * Calculate total translate3d from transform string
   */
  private calculateTotalTranslate3d(transformString: string): [number, number, number] {
    if (!transformString) return [0, 0, 0];

    const regex = /translate3d\(([^,]+),\s*([^,]+),\s*([^)]+)\)/g;
    let match;
    let totalX = 0,
      totalY = 0,
      totalZ = 0;

    while ((match = regex.exec(transformString)) !== null) {
      totalX += parseFloat(match[1]);
      totalY += parseFloat(match[2]);
      totalZ += parseFloat(match[3]);
    }

    return [totalX, totalY, totalZ];
  }
}
