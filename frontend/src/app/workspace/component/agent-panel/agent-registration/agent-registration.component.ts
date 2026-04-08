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
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { ComputingUnitStatusService } from "../../../service/computing-unit-status/computing-unit-status.service";
import { ComputingUnitState } from "../../../types/computing-unit-connection.interface";
import { WorkflowContent } from "../../../../common/type/workflow";
import { Subject, takeUntil } from "rxjs";
import { NzUploadFile } from "ng-zorro-antd/upload";

interface TraceContent {
  response: string;
  messages: any[];
}

@Component({
  selector: "texera-agent-registration",
  templateUrl: "agent-registration.component.html",
  styleUrls: ["agent-registration.component.scss"],
})
export class AgentRegistrationComponent implements OnInit, OnDestroy {
  @Output() agentCreated = new EventEmitter<string>();

  public modelTypes: ModelType[] = [];
  public selectedModelType: string | null = null;
  public customAgentName: string = "Bob";
  public isLoadingModels: boolean = false;
  public hasLoadingError: boolean = false;

  public traceFileList: NzUploadFile[] = [];
  public traceContent: TraceContent | null = null;
  public computingUnitConnected: boolean = false;

  private destroy$ = new Subject<void>();

  constructor(
    private copilotManagerService: TexeraCopilotManagerService,
    private notificationService: NotificationService,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private guiConfigService: GuiConfigService,
    private computingUnitStatusService: ComputingUnitStatusService
  ) {}

  ngOnInit(): void {
    this.isLoadingModels = true;
    this.hasLoadingError = false;

    this.computingUnitStatusService
      .getStatus()
      .pipe(takeUntil(this.destroy$))
      .subscribe(status => {
        this.computingUnitConnected = status === ComputingUnitState.Running;
      });

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
   * Handle trace file upload. Supports wrapped format ({ response, messages }) or plain array.
   */
  public beforeUpload = (file: NzUploadFile): boolean => {
    const reader = new FileReader();
    reader.onload = (e: ProgressEvent<FileReader>) => {
      try {
        const parsed = JSON.parse(e.target?.result as string);
        let content: TraceContent;

        if (Array.isArray(parsed)) {
          content = { response: "", messages: parsed };
        } else if (parsed.messages && Array.isArray(parsed.messages)) {
          content = parsed as TraceContent;
        } else {
          this.notificationService.error("Invalid trace file: expected array of messages or object with messages array");
          return;
        }

        this.traceContent = content;
        this.traceFileList = [file];
        this.notificationService.success(`Trace loaded: ${content.messages.length} messages`);
      } catch {
        this.notificationService.error("Invalid JSON file");
        this.traceContent = null;
        this.traceFileList = [];
      }
    };
    reader.readAsText(file as unknown as File);
    return false;
  };

  public clearTrace(): void {
    this.traceContent = null;
    this.traceFileList = [];
  }

  /**
   * Create a new agent with the selected model type.
   * If a trace is loaded, creates a new workflow and initiates replay.
   */
  public createAgent(): void {
    if (!this.selectedModelType || this.isCreating) {
      return;
    }

    this.isCreating = true;

    if (this.traceContent) {
      this.createAgentWithReplay();
      return;
    }

    const workflowMetadata = this.workflowActionService.getWorkflowMetadata();
    const workflowId = workflowMetadata?.wid;

    this.copilotManagerService
      .createAgent(this.selectedModelType!, this.customAgentName || undefined, workflowId)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: agentInfo => {
          this.agentCreated.emit(agentInfo.id);
          this.resetForm();
        },
        error: (error: unknown) => {
          this.notificationService.error(`Failed to create agent: ${error}`);
          this.isCreating = false;
        },
      });
  }

  private createAgentWithReplay(): void {
    const workflowName = `Imported - ${new Date().toISOString().split("T")[0]}`;
    const emptyWorkflowContent: WorkflowContent = {
      operators: [],
      commentBoxes: [],
      links: [],
      operatorPositions: {},
      settings: {
        dataTransferBatchSize: this.guiConfigService.env.defaultDataTransferBatchSize,
      },
    };

    this.workflowPersistService
      .createWorkflow(emptyWorkflowContent, workflowName)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: createdWorkflow => {
          const workflowId = createdWorkflow.workflow.wid;
          this.copilotManagerService
            .createAgent(this.selectedModelType!, this.customAgentName || undefined, workflowId)
            .pipe(takeUntil(this.destroy$))
            .subscribe({
              next: agentInfo => {
                this.copilotManagerService.activateAgent(agentInfo.id);
                setTimeout(() => {
                  if (this.traceContent) {
                    this.copilotManagerService.sendReplayMessage(agentInfo.id, this.traceContent);
                  }
                  this.agentCreated.emit(agentInfo.id);
                  this.resetForm();
                }, 500);
              },
              error: (error: unknown) => {
                this.notificationService.error(`Failed to create agent: ${error}`);
                this.isCreating = false;
              },
            });
        },
        error: (error: unknown) => {
          this.notificationService.error(`Failed to create workflow: ${error}`);
          this.isCreating = false;
        },
      });
  }

  private resetForm(): void {
    this.selectedModelType = null;
    this.customAgentName = "";
    this.traceContent = null;
    this.traceFileList = [];
    this.isCreating = false;
  }

  public canCreate(): boolean {
    return this.selectedModelType !== null && !this.isCreating && this.computingUnitConnected;
  }
}
