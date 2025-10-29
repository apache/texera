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

import { Component, EventEmitter, Output } from "@angular/core";
import { TexeraCopilotManagerService, ModelType } from "../../../service/copilot/texera-copilot-manager.service";

@Component({
  selector: "texera-agent-registration",
  templateUrl: "agent-registration.component.html",
  styleUrls: ["agent-registration.component.scss"],
})
export class AgentRegistrationComponent {
  @Output() agentCreated = new EventEmitter<string>(); // Emit agent ID when created

  public modelTypes: ModelType[] = [];
  public selectedModelType: string | null = null;
  public customAgentName: string = "";

  constructor(private copilotManagerService: TexeraCopilotManagerService) {
    this.modelTypes = this.copilotManagerService.getModelTypes();
  }

  /**
   * Select a model type
   */
  public selectModelType(modelTypeId: string): void {
    this.selectedModelType = modelTypeId;
  }

  public isCreating: boolean = false;

  /**
   * Create a new agent with the selected model type
   */
  public async createAgent(): Promise<void> {
    if (!this.selectedModelType || this.isCreating) {
      return;
    }

    this.isCreating = true;

    try {
      const agentInfo = await this.copilotManagerService.createAgent(
        this.selectedModelType,
        this.customAgentName || undefined
      );

      // Emit event with agent ID
      this.agentCreated.emit(agentInfo.id);

      // Reset selection
      this.selectedModelType = null;
      this.customAgentName = "";
    } catch (error) {
      console.error("Failed to create agent:", error);
      // TODO: Show error notification
      alert("Failed to create agent. Please check the console for details.");
    } finally {
      this.isCreating = false;
    }
  }

  /**
   * Check if create button should be enabled
   */
  public canCreate(): boolean {
    return this.selectedModelType !== null && !this.isCreating;
  }
}
