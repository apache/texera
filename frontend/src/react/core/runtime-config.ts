/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ApiClient } from "./api-client";

/** Runtime switches are owned by the config service, not by the React build. */
export interface GuiConfig {
  localLogin?: boolean;
  googleLogin?: boolean;
  orcidLogin?: boolean;
  appleLogin?: boolean;
  inviteOnly?: boolean;
  emailVerification?: boolean;
  defaultLocalUser?: { username?: string; password?: string };
  attributionEnabled?: boolean;
  warehouseEnabled?: boolean;
  formViewEnabled?: boolean;
  copilotEnabled?: boolean;
  aiFixModel?: string;
  userPresetEnabled?: boolean;
  exportExecutionResultEnabled?: boolean;
  selectingFilesFromDatasetsEnabled?: boolean;
  workflowExecutionsTrackingEnabled?: boolean;
  linkBreakpointEnabled?: boolean;
  asyncRenderingEnabled?: boolean;
  timetravelEnabled?: boolean;
  productionSharedEditingServer?: boolean;
  pythonLanguageServerPort?: string;
  defaultDataTransferBatchSize?: number;
  defaultExecutionMode?: string;
  workflowEmailNotificationEnabled?: boolean;
  sharingComputingUnitEnabled?: boolean;
  operatorConsoleMessageBufferSize?: number;
  activeTimeInMinutes?: number;
  limitColumns?: number;
  pythonNotebookMigrationEnabled?: boolean;
  rNotebookMigrationEnabled?: boolean;
  pythonNotebookMigrationTimeoutMinutes?: number;
  deploymentVersionCheckEnabled?: boolean;
  [key: string]: unknown;
}

type Source = "preLogin" | "gui" | "amber" | "userSystem";
const MERGE_ORDER: Source[] = ["preLogin", "gui", "amber", "userSystem"];

export class RuntimeConfig {
  private sources: Partial<Record<Source, GuiConfig>> = {};
  private merged: GuiConfig = {};
  private revision = 0;

  constructor(private readonly api: ApiClient) {}

  get env(): Readonly<GuiConfig> {
    return this.merged;
  }

  async load(hasToken: boolean, signal?: AbortSignal): Promise<Readonly<GuiConfig>> {
    await this.loadPreLogin(signal);
    if (hasToken) {
      try {
        await this.loadPostLogin(signal);
      } catch (error) {
        if (signal?.aborted) throw error;
        // Backend guards still enforce access when protected config cannot be fetched.
      }
    }
    return this.env;
  }

  async loadPreLogin(signal?: AbortSignal): Promise<Readonly<GuiConfig>> {
    this.sources.preLogin = await this.api.get<GuiConfig>("config/pre-login", { signal });
    this.rebuild();
    return this.env;
  }

  async loadPostLogin(signal?: AbortSignal): Promise<Readonly<GuiConfig>> {
    const revision = ++this.revision;
    const [gui, amber, userSystem] = await Promise.all([
      this.api.get<GuiConfig>("config/gui", { signal }),
      this.api.get<GuiConfig>("config/amber", { signal }),
      this.api.get<GuiConfig>("config/user-system", { signal }),
    ]);
    if (revision !== this.revision) return this.env;
    this.sources = { ...this.sources, gui, amber, userSystem };
    this.rebuild();
    return this.env;
  }

  clearAuthenticated(): void {
    ++this.revision;
    this.sources = { preLogin: this.sources.preLogin };
    this.rebuild();
  }

  private rebuild(): void {
    this.merged = MERGE_ORDER.reduce((merged, source) => ({ ...merged, ...this.sources[source] }), {});
  }
}
