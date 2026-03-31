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

import { Component, OnInit, OnDestroy, ViewChild } from "@angular/core";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzMessageService } from "ng-zorro-antd/message";
import {
  TexeraCopilotManagerService,
  AgentInfo,
} from "../../../../workspace/service/copilot/texera-copilot-manager.service";
import { WorkspaceComponent } from "../../../../workspace/component/workspace.component";
import { DASHBOARD_USER_AGENT } from "../../../../app-routing.constant";

/**
 * Component that wraps the workspace to connect to a specific agent.
 * When navigating to /dashboard/user/agent/:agentId, this component:
 * 1. Loads the agent's associated workflow
 * 2. Activates the WebSocket connection to the agent
 * 3. Opens the agent panel automatically
 */
@UntilDestroy()
@Component({
  selector: "texera-user-agent-workspace",
  template: `
    <texera-workspace
      #workspace
      [agentIdToActivate]="agentId"></texera-workspace>
  `,
  styles: [
    `
      :host {
        display: block;
        width: 100%;
        height: 100%;
      }
    `,
  ],
})
export class UserAgentWorkspaceComponent implements OnInit, OnDestroy {
  @ViewChild("workspace") workspaceComponent?: WorkspaceComponent;

  agentId?: string;
  workflowId?: number;
  private agent?: AgentInfo;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private copilotManagerService: TexeraCopilotManagerService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    // Get agent ID from route params
    this.route.params.pipe(untilDestroyed(this)).subscribe(params => {
      const agentIdParam = params["agentId"];
      if (agentIdParam) {
        this.agentId = agentIdParam;
        this.loadAgent();
      }
    });

    // Get workflow ID from query params (fallback)
    this.route.queryParams.pipe(untilDestroyed(this)).subscribe(params => {
      if (params["wid"]) {
        this.workflowId = parseInt(params["wid"], 10);
      }
    });
  }

  ngOnDestroy(): void {
    // Deactivate agent when leaving
    if (this.agentId) {
      this.copilotManagerService.deactivateAgent(this.agentId);
    }
  }

  /**
   * Load the agent and its associated workflow
   */
  private loadAgent(): void {
    if (!this.agentId) return;

    this.copilotManagerService
      .getAgent(this.agentId)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: agent => {
          this.agent = agent;

          // Get workflow ID from agent or query params
          const wid = agent.delegate?.workflowId || this.workflowId;

          if (!wid) {
            this.message.error("Agent is not associated with a workflow");
            this.router.navigate([DASHBOARD_USER_AGENT]);
            return;
          }

          // Activate the agent connection
          this.copilotManagerService.activateAgent(this.agentId!);
        },
        error: () => {
          this.message.error("Failed to load agent");
          this.router.navigate([DASHBOARD_USER_AGENT]);
        },
      });
  }
}
