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

import { Component, EventEmitter, OnDestroy, OnInit, Output } from "@angular/core";
import { TexeraCopilotManagerService, ModelType } from "../../../service/copilot/texera-copilot-manager.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { Subject, takeUntil } from "rxjs";

@Component({
  selector: "texera-agent-registration",
  templateUrl: "agent-registration.component.html",
  styleUrls: ["agent-registration.component.scss"],
})
export class AgentRegistrationComponent implements OnInit, OnDestroy {
  @Output() agentCreated = new EventEmitter<string>();

  public modelTypes: ModelType[] = [];
  public selectedModelType: string | null = null;
  public customAgentName: string = "";
  public isLoadingModels: boolean = false;
  public hasLoadingError: boolean = false;

  private destroy$ = new Subject<void>();

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.isLoadingModels = true;
    this.hasLoadingError = false;

    this.copilotManagerService
      .fetchModelTypes()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: models => {
          this.modelTypes = models;
          this.isLoadingModels = false;
          if (models.length === 0) {
            this.hasLoadingError = true;
            this.notificationService.error("No models available. Please check the LiteLLM configuration.");
          }
        },
        error: (error: unknown) => {
          this.isLoadingModels = false;
          this.hasLoadingError = true;
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.notificationService.error(`Failed to fetch models: ${errorMessage}`);
        },
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public selectModelType(modelTypeId: string): void {
    this.selectedModelType = modelTypeId;
  }

  public isCreating: boolean = false;

  /**
   * Create a new agent with the selected model type.
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

      this.agentCreated.emit(agentInfo.id);
      this.selectedModelType = null;
      this.customAgentName = "";
    } catch (error) {
      this.notificationService.error(`Failed to create agent: ${error}`);
    } finally {
      this.isCreating = false;
    }
  }

  public canCreate(): boolean {
    return this.selectedModelType !== null && !this.isCreating;
  }
}
