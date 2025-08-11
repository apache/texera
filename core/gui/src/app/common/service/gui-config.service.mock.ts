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

import { Injectable } from "@angular/core";
import { Observable, of } from "rxjs";
import { GuiConfig } from "../type/gui-config";

/**
 * Mock GuiConfigService for testing purposes.
 * Provides default configuration values without requiring HTTP calls.
 */
@Injectable()
export class MockGuiConfigService {
  private _config: GuiConfig = {
    exportExecutionResultEnabled: false,
    autoAttributeCorrectionEnabled: false,
    userSystemEnabled: true,
    selectingFilesFromDatasetsEnabled: false,
    localLogin: true,
    googleLogin: true,
    inviteOnly: false,
    userPresetEnabled: true,
    workflowExecutionsTrackingEnabled: false,
    linkBreakpointEnabled: false,
    asyncRenderingEnabled: false,
    timetravelEnabled: false,
    productionSharedEditingServer: false,
    pythonLanguageServerPort: "3000",
    defaultDataTransferBatchSize: 100,
    workflowEmailNotificationEnabled: false,
    sharingComputingUnitEnabled: false,
    operatorConsoleMessageBufferSize: 1000,
    defaultLocalUser: { username: "", password: "" },
  };

  get env(): GuiConfig {
    return this._config;
  }

  load(): Observable<GuiConfig> {
    return of(this._config);
  }

  setConfig(config: Partial<GuiConfig>): void {
    this._config = { ...this._config, ...config };
  }
}
