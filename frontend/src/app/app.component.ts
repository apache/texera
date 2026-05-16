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
import { Router } from "@angular/router";
import { filter, switchMap, take } from "rxjs";
import { GuiConfigService } from "./common/service/gui-config.service";
import { AgentService } from "./workspace/service/agent/agent.service";
import { UserService } from "./common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-root",
  template: `
    <div
      *ngIf="!configLoaded"
      id="config-error">
      <h1>Configuration Error</h1>
      <p>Failed to load gui's configuration.</p>
      <p>Please ensure the ConfigService is running and accessible.</p>
      <button (click)="retry()">Retry</button>
    </div>
    <router-outlet *ngIf="configLoaded"></router-outlet>
    <texera-agent-panel *ngIf="configLoaded && copilotEnabled"></texera-agent-panel>
  `,
  standalone: false,
})
export class AppComponent implements OnInit {
  configLoaded = false;

  constructor(
    private config: GuiConfigService,
    private agentService: AgentService,
    private userService: UserService,
    private router: Router
  ) {
    try {
      void this.config.env;
      this.configLoaded = true;
    } catch {
      this.configLoaded = false;
    }
  }

  ngOnInit(): void {
    // Listen for agent-driven navigation requests and execute them in the browser.
    this.agentService.navigate$.pipe(untilDestroyed(this)).subscribe(url => {
      this.router.navigateByUrl(url);
    });

    if (!this.configLoaded || !this.copilotEnabled) return;

    // Auto-open the agent panel when the user logs in and has no agents yet.
    this.userService
      .userChanged()
      .pipe(
        untilDestroyed(this),
        filter(user => user !== undefined), // only when logged in
        switchMap(() =>
          // Re-fetch agent list fresh after login (bypasses cached empty state)
          this.agentService.getAllAgents().pipe(take(1))
        )
      )
      .subscribe(agents => {
        if (agents.length === 0) {
          // Delay so router finishes navigating to the dashboard before opening
          setTimeout(() => this.agentService.requestOpenPanel(), 700);
        }
      });
  }

  get copilotEnabled(): boolean {
    return this.config.env.copilotEnabled;
  }

  retry(): void {
    window.location.reload();
  }
}
