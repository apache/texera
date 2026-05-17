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

import { Component, inject, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { Clipboard } from "@angular/cdk/clipboard";
import { NzModalRef, NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzAlertModule } from "ng-zorro-antd/alert";
import { NzSpinModule } from "ng-zorro-antd/spin";
import { PublishApiService, PublishedWorkflow } from "../../service/publish-api/publish-api.service";
import { NotificationService } from "../../../common/service/notification/notification.service";

export interface PublishApiDialogData {
  workflowId: number;
  workflowName: string;
}

@Component({
  selector: "texera-publish-api-dialog",
  templateUrl: "./publish-api-dialog.component.html",
  styleUrls: ["./publish-api-dialog.component.scss"],
  imports: [CommonModule, FormsModule, NzButtonModule, NzInputModule, NzIconModule, NzAlertModule, NzSpinModule],
})
export class PublishApiDialogComponent implements OnInit {
  protected readonly modal = inject(NzModalRef<PublishApiDialogComponent>);
  protected readonly data = inject<PublishApiDialogData>(NZ_MODAL_DATA);

  public published?: PublishedWorkflow;
  public isPublishing = false;
  public errorMessage: string | null = null;
  public showApiKey = false;

  constructor(
    private publishApi: PublishApiService,
    private notification: NotificationService,
    private clipboard: Clipboard
  ) {}

  ngOnInit(): void {
    this.published = this.publishApi.getPublished(this.data.workflowId);
  }

  public get maskedApiKey(): string {
    if (!this.published) return "";
    const k = this.published.apiKey;
    if (this.showApiKey) return k;
    if (k.length <= 8) return "•".repeat(k.length);
    return `${k.slice(0, 4)}${"•".repeat(k.length - 8)}${k.slice(-4)}`;
  }

  public get curlCommand(): string {
    return this.published ? this.publishApi.buildCurlCommand(this.published) : "";
  }

  public publish(): void {
    this.errorMessage = null;
    this.isPublishing = true;
    this.publishApi.publish(this.data.workflowId, this.data.workflowName).subscribe({
      next: entry => {
        this.published = entry;
        this.isPublishing = false;
        this.notification.success("Workflow published as API");
      },
      error: err => {
        this.errorMessage = err?.message ?? "Publish failed.";
        this.isPublishing = false;
      },
    });
  }

  public republish(): void {
    this.publish();
  }

  public copy(value: string, label: string): void {
    if (!value) return;
    this.clipboard.copy(value);
    this.notification.success(`${label} copied to clipboard`);
  }

  public close(): void {
    this.modal.close();
  }
}
