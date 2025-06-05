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
import { GuiConfigService } from "./common/service/gui-config.service";
import { DASHBOARD_ABOUT } from "./app-routing.constant";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-root",
  template: `
    <div
      *ngIf="configLoaded === false"
      id="config-error">
      <h1>Configuration Error</h1>
      <p>Failed to load application configuration.</p>
      <p>Please ensure the backend server is running and accessible.</p>
      <button (click)="retry()">Retry</button>
    </div>
    <router-outlet *ngIf="configLoaded"></router-outlet>
  `,
})
export class AppComponent implements OnInit {
  /**
   * configLoaded state:
   *   undefined -> still loading
   *   true      -> loaded successfully
   *   false     -> failed to load
   */
  configLoaded?: boolean;

  constructor(
    private config: GuiConfigService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.config
      .load()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.configLoaded = true;
          if (this.config.env.userSystemEnabled && this.router.url === "/") {
            this.router.navigateByUrl(DASHBOARD_ABOUT);
          }
        },
        error: (err: unknown) => {
          console.error("GUI config failed to load:", err);
          this.configLoaded = false;
        },
      });
  }

  retry(): void {
    window.location.reload();
  }
}
