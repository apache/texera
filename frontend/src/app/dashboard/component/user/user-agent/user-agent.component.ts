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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Router } from "@angular/router";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import {
  TexeraCopilotManagerService,
  AgentInfo,
} from "../../../../workspace/service/copilot/texera-copilot-manager.service";
import { CopilotState } from "../../../../workspace/service/copilot/copilot-types";
import { UserService } from "../../../../common/service/user/user.service";
import { DASHBOARD_USER_AGENT } from "../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-user-agent",
  templateUrl: "user-agent.component.html",
  styleUrls: ["user-agent.component.scss"],
})
export class UserAgentComponent implements OnInit {
  public agents: AgentInfo[] = [];
  public isLoading = false;
  public isLogin = false;

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private userService: UserService,
    private router: Router,
    private modalService: NzModalService,
    private message: NzMessageService
  ) {
    this.isLogin = this.userService.isLogin();
  }

  ngOnInit(): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        if (this.isLogin) {
          this.loadAgents();
        }
      });

    // Subscribe to agent changes
    this.copilotManagerService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      this.loadAgents();
    });

    // Initial load
    if (this.isLogin) {
      this.loadAgents();
    }
  }

  /**
   * Load all agents from the service
   */
  loadAgents(): void {
    this.isLoading = true;
    this.copilotManagerService
      .getAllAgents()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: agents => {
          this.agents = agents.sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
          this.isLoading = false;
        },
        error: () => {
          this.isLoading = false;
          this.message.error("Failed to load agents");
        },
      });
  }

  /**
   * Navigate to an agent's workspace
   */
  openAgent(agent: AgentInfo): void {
    if (agent.delegate?.workflowId) {
      // Navigate to the agent's workflow with agent ID as query param
      this.router.navigate([`${DASHBOARD_USER_AGENT}/${agent.id}`], {
        queryParams: { wid: agent.delegate.workflowId },
      });
    } else {
      this.message.warning("This agent is not associated with a workflow");
    }
  }

  /**
   * Delete an agent
   */
  deleteAgent(agent: AgentInfo, event: Event): void {
    event.stopPropagation();

    this.modalService.confirm({
      nzTitle: "Delete Agent",
      nzContent: `Are you sure you want to delete agent "${agent.name}"?`,
      nzOkText: "Delete",
      nzOkDanger: true,
      nzOnOk: () => {
        this.copilotManagerService
          .deleteAgent(agent.id)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: () => {
              this.message.success("Agent deleted successfully");
              this.loadAgents();
            },
            error: () => {
              this.message.error("Failed to delete agent");
            },
          });
      },
    });
  }

  /**
   * Get the state display text
   */
  getStateText(agent: AgentInfo): string {
    switch (agent.state) {
      case CopilotState.AVAILABLE:
        return "Available";
      case CopilotState.GENERATING:
        return "Generating";
      case CopilotState.STOPPING:
        return "Stopping";
      case CopilotState.UNAVAILABLE:
      default:
        return "Unavailable";
    }
  }

  /**
   * Get the state badge color
   */
  getStateColor(agent: AgentInfo): string {
    switch (agent.state) {
      case CopilotState.AVAILABLE:
        return "success";
      case CopilotState.GENERATING:
        return "processing";
      case CopilotState.STOPPING:
        return "warning";
      case CopilotState.UNAVAILABLE:
      default:
        return "default";
    }
  }

  /**
   * Format date for display
   */
  formatDate(date: Date): string {
    return date.toLocaleString();
  }
}
