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

import { Component } from "@angular/core";
import { GuiConfigService } from "./common/service/gui-config.service";
import { UntilDestroy } from "@ngneat/until-destroy";

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
  `,
})
export class AppComponent {
  configLoaded = false;

  constructor(private config: GuiConfigService) {
    // determine whether configuration was successfully loaded by APP_INITIALIZER
    try {
      // accessing env will throw if not loaded
      void this.config.env;
      this.configLoaded = true;
    } catch {
      this.configLoaded = false;
    }
  }

  retry(): void {
    window.location.reload();
  }
}
