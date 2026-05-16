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
    private message: NzMessageService
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

    const proceedWithLocalFork = () => {
      // Fallback path: no backend wid available — record the fork count and route to a new local workspace.
      this.hubService.recordFork(this.entry!.id);
      this.message.success(`Forked "${this.entry!.title}" — open in workspace to start editing.`);
      this.forking = false;
      this.router.navigate(["/dashboard/user/workflow"]);
    };

    if (this.entry.workflowId !== undefined) {
      this.workflowPersistService.duplicateWorkflow([this.entry.workflowId]).subscribe({
        next: dupes => {
          this.hubService.recordFork(this.entry!.id);
          this.message.success(`Forked "${this.entry!.title}" to your workflows.`);
          this.forking = false;
          const newWid = dupes?.[0]?.workflow?.wid;
          if (newWid !== undefined) {
            this.router.navigate(["/dashboard/user/workflow", newWid]);
          } else {
            this.router.navigate(["/dashboard/user/workflow"]);
          }
        },
        error: () => {
          this.message.warning("Fork via backend failed — falling back to local copy.");
          proceedWithLocalFork();
        },
      });
    } else {
      proceedWithLocalFork();
    }
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
