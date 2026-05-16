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
import { ActivatedRoute, Router, RouterLink } from "@angular/router";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzMessageService } from "ng-zorro-antd/message";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowHubService } from "../workflow-hub.service";
import { WorkflowHubEntry } from "../workflow-hub.types";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { ExecutionMode, WorkflowContent } from "../../../../common/type/workflow";
import { GuiConfigService } from "../../../../common/service/gui-config.service";

@UntilDestroy()
@Component({
  selector: "texera-workflow-hub-detail",
  templateUrl: "./workflow-hub-detail.component.html",
  styleUrls: ["./workflow-hub-detail.component.scss"],
  imports: [CommonModule, RouterLink, NzButtonModule, NzIconModule, NzTagModule, NzTooltipModule],
})
export class WorkflowHubDetailComponent implements OnInit {
  entry?: WorkflowHubEntry;
  starred = false;
  forking = false;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private hubService: WorkflowHubService,
    private workflowPersistService: WorkflowPersistService,
    private message: NzMessageService,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    this.route.paramMap.pipe(untilDestroyed(this)).subscribe(params => {
      const id = params.get("id");
      if (!id) {
        this.router.navigate(["/dashboard/hub/workflow-hub"]);
        return;
      }
      const found = this.hubService.getEntry(id);
      if (!found) {
        this.message.error("Workflow not found.");
        this.router.navigate(["/dashboard/hub/workflow-hub"]);
        return;
      }
      this.entry = found;
      this.starred = this.hubService.isStarred(id);
      this.hubService.recordView(id);
    });

    this.hubService
      .entries$()
      .pipe(untilDestroyed(this))
      .subscribe(entries => {
        if (this.entry) {
          const refreshed = entries.find(e => e.id === this.entry!.id);
          if (refreshed) this.entry = refreshed;
        }
      });
    this.hubService
      .stars$()
      .pipe(untilDestroyed(this))
      .subscribe(set => {
        if (this.entry) this.starred = set.has(this.entry.id);
      });
  }

  toggleStar(): void {
    if (!this.entry) return;
    this.hubService.toggleStar(this.entry.id);
  }

  forkWorkflow(): void {
    if (!this.entry || this.forking) return;
    this.forking = true;
    const entry = this.entry;
    const forkName = `[Fork] ${entry.title}`;

    const onSuccess = (newWid: number | undefined) => {
      this.hubService.recordFork(entry.id);
      this.message.success(`Forked "${entry.title}" to your workflows.`);
      this.forking = false;
      if (newWid !== undefined) {
        this.router.navigate(["/dashboard/user/workflow", newWid]);
      } else {
        this.router.navigate(["/dashboard/user/workflow"]);
      }
    };

    const onError = (err: unknown) => {
      this.forking = false;
      console.error("Workflow fork failed", err);
      this.message.error("Could not create the forked workflow. Are you signed in?");
    };

    if (entry.workflowId !== undefined) {
      this.workflowPersistService.duplicateWorkflow([entry.workflowId]).subscribe({
        next: dupes => onSuccess(dupes?.[0]?.workflow?.wid),
        error: onError,
      });
    } else {
      // Seed entry — no backend wid. Create a real workflow with empty content so
      // the user can open it, see it in their Workflows page, and start editing.
      this.workflowPersistService.createWorkflow(this.buildEmptyContent(), forkName).subscribe({
        next: created => onSuccess(created?.workflow?.wid),
        error: onError,
      });
    }
  }

  private buildEmptyContent(): WorkflowContent {
    return {
      operators: [],
      operatorPositions: {},
      links: [],
      commentBoxes: [],
      settings: {
        dataTransferBatchSize: this.config.env?.defaultDataTransferBatchSize ?? 400,
        executionMode: this.config.env?.defaultExecutionMode ?? ExecutionMode.PIPELINED,
      },
    };
  }

  get publishedDateLabel(): string {
    if (!this.entry) return "";
    return new Date(this.entry.publishedAt).toLocaleDateString(undefined, {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
  }
}
