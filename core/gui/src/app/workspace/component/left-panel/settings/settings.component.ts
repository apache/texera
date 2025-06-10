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
import { FormBuilder, FormGroup, Validators } from "@angular/forms";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";

@UntilDestroy()
@Component({
  selector: "texera-settings",
  templateUrl: "./settings.component.html",
  styleUrls: ["./settings.component.scss"],
})
export class SettingsComponent implements OnInit {
  settingsForm!: FormGroup;
  currentDataTransferBatchSize!: number;
  isSaving: boolean = false;

  constructor(
    private fb: FormBuilder,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private userService: UserService,
    private notificationService: NotificationService,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    this.currentDataTransferBatchSize =
      this.workflowActionService.getWorkflowContent().settings.dataTransferBatchSize ||
      this.config.env.defaultDataTransferBatchSize;

    this.settingsForm = this.fb.group({
      dataTransferBatchSize: [this.currentDataTransferBatchSize, [Validators.required, Validators.min(1)]],
    });

    this.workflowActionService
      .workflowChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentDataTransferBatchSize =
          this.workflowActionService.getWorkflowContent().settings.dataTransferBatchSize ||
          this.config.env.defaultDataTransferBatchSize;
        this.settingsForm.patchValue(
          { dataTransferBatchSize: this.currentDataTransferBatchSize },
          { emitEvent: false }
        );
      });
  }

  public confirmUpdateDataTransferBatchSize(dataTransferBatchSize: number): void {
    if (dataTransferBatchSize > 0) {
      this.workflowActionService.setWorkflowDataTransferBatchSize(dataTransferBatchSize);
      if (this.userService.isLogin()) {
        this.persistWorkflow();
      }
    }
  }

  public persistWorkflow(): void {
    this.isSaving = true;
    this.workflowPersistService
      .persistWorkflow(this.workflowActionService.getWorkflow())
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      })
      .add(() => (this.isSaving = false));
  }
}
