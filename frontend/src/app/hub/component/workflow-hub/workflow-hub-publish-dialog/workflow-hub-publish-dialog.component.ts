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
 */

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalRef } from "ng-zorro-antd/modal";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzFormModule } from "ng-zorro-antd/form";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { catchError, of } from "rxjs";
import { WorkflowHubService } from "../workflow-hub.service";
import { WORKFLOW_HUB_CATEGORIES, WorkflowHubCategory } from "../workflow-hub.types";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { UserService } from "../../../../common/service/user/user.service";
import { DashboardWorkflow } from "../../../../dashboard/type/dashboard-workflow.interface";

@UntilDestroy()
@Component({
  selector: "texera-workflow-hub-publish-dialog",
  templateUrl: "./workflow-hub-publish-dialog.component.html",
  styleUrls: ["./workflow-hub-publish-dialog.component.scss"],
  imports: [CommonModule, FormsModule, NzInputModule, NzButtonModule, NzSelectModule, NzFormModule],
})
export class WorkflowHubPublishDialogComponent implements OnInit {
  readonly categories = WORKFLOW_HUB_CATEGORIES.filter(c => c.key !== "all");

  myWorkflows: DashboardWorkflow[] = [];
  selectedWid?: number;
  title = "";
  description = "";
  category: WorkflowHubCategory = "education";
  tagsRaw = "";
  loading = false;
  publishing = false;

  constructor(
    private modalRef: NzModalRef,
    private hubService: WorkflowHubService,
    private workflowPersistService: WorkflowPersistService,
    private userService: UserService,
    private message: NzMessageService
  ) {}

  ngOnInit(): void {
    this.loading = true;
    this.workflowPersistService
      .retrieveWorkflowsBySessionUser()
      .pipe(
        catchError(() => of([] as DashboardWorkflow[])),
        untilDestroyed(this)
      )
      .subscribe(list => {
        this.myWorkflows = list || [];
        this.loading = false;
      });
  }

  onWorkflowChange(): void {
    const w = this.myWorkflows.find(x => x.workflow.wid === this.selectedWid);
    if (w) {
      this.title = w.workflow.name;
      this.description = w.workflow.description || "";
    }
  }

  canPublish(): boolean {
    return !!this.title.trim() && !!this.description.trim();
  }

  publish(): void {
    if (!this.canPublish() || this.publishing) return;
    this.publishing = true;
    const tags = this.tagsRaw
      .split(",")
      .map(t => t.trim())
      .filter(t => t.length > 0);

    const user = this.userService.getCurrentUser();
    const authorName = user?.name || user?.email || "Anonymous";

    const operatorsFromWorkflow: string[] = (() => {
      const w = this.myWorkflows.find(x => x.workflow.wid === this.selectedWid);
      if (!w || !w.workflow?.content) return [];
      try {
        const content = typeof w.workflow.content === "string" ? JSON.parse(w.workflow.content as any) : w.workflow.content;
        const ops = (content?.operators ?? []) as Array<{ operatorType?: string }>;
        return ops.map(o => o.operatorType || "Operator").slice(0, 16);
      } catch {
        return [];
      }
    })();

    this.hubService.publishEntry({
      workflowId: this.selectedWid,
      authorName,
      title: this.title.trim(),
      description: this.description.trim(),
      category: this.category,
      tags,
      operators: operatorsFromWorkflow.length > 0 ? operatorsFromWorkflow : ["CSVFileScan", "Projection", "View"],
    });

    this.message.success(`"${this.title.trim()}" published to the Workflow Hub.`);
    this.publishing = false;
    this.modalRef.close();
  }

  cancel(): void {
    this.modalRef.close();
  }
}
