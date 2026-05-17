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
import { OperatorMetadataService } from "../../../../workspace/service/operator-metadata/operator-metadata.service";
import { WorkflowUtilService } from "../../../../workspace/service/workflow-graph/util/workflow-util.service";
import { firstValueFrom } from "rxjs";
import { OperatorPredicate, OperatorLink, Point, CommentBox } from "../../../../workspace/types/workflow-common.interface";

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
    private config: GuiConfigService,
    private operatorMetadataService: OperatorMetadataService,
    private workflowUtilService: WorkflowUtilService
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

  async forkWorkflow(): Promise<void> {
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
      // Real published workflow — duplicate via the backend so content/permissions are copied properly.
      this.workflowPersistService.duplicateWorkflow([entry.workflowId]).subscribe({
        next: dupes => onSuccess(dupes?.[0]?.workflow?.wid),
        error: onError,
      });
      return;
    }

    // Seed entry: no backend wid. Build a real WorkflowContent from the entry's
    // sampleOperators (which are real Texera operator types), then create the
    // workflow with that content so it isn't blank in the workspace.
    try {
      const content = await this.buildContentFromSeed(entry.sampleOperators ?? []);
      this.workflowPersistService.createWorkflow(content, forkName).subscribe({
        next: created => onSuccess(created?.workflow?.wid),
        error: onError,
      });
    } catch (err) {
      onError(err);
    }
  }

  /** Builds a real WorkflowContent: a horizontal chain of real operators connected by links. */
  private async buildContentFromSeed(sampleOperatorTypes: string[]): Promise<WorkflowContent> {
    // Ensure operator metadata is loaded (needed by WorkflowUtilService.getNewOperatorPredicate).
    await firstValueFrom(this.operatorMetadataService.getOperatorMetadata());

    const knownTypes = new Set(this.workflowUtilService.getOperatorTypeList());
    const validTypes = sampleOperatorTypes.filter(t => knownTypes.has(t));

    const operators: OperatorPredicate[] = [];
    const operatorPositions: { [key: string]: Point } = {};
    const links: OperatorLink[] = [];
    const commentBoxes: CommentBox[] = [];

    const xStart = 200;
    const xStep = 220;
    const y = 240;

    let prev: OperatorPredicate | undefined;
    validTypes.forEach((opType, i) => {
      const op = this.workflowUtilService.getNewOperatorPredicate(opType);
      operators.push(op);
      operatorPositions[op.operatorID] = { x: xStart + i * xStep, y };

      if (prev && prev.outputPorts.length > 0 && op.inputPorts.length > 0) {
        links.push({
          linkID: this.workflowUtilService.getLinkRandomUUID(),
          source: { operatorID: prev.operatorID, portID: prev.outputPorts[0].portID },
          target: { operatorID: op.operatorID, portID: op.inputPorts[0].portID },
        });
      }
      prev = op;
    });

    // Any sample types we couldn't resolve become annotation comment boxes so the user can see
    // what was intended.
    const skipped = sampleOperatorTypes.filter(t => !knownTypes.has(t));
    if (skipped.length > 0) {
      commentBoxes.push({
        commentBoxID: this.workflowUtilService.getCommentBoxRandomUUID(),
        comments: [
          {
            content: `Forked from Workflow Hub. Skipped operators (not in this Texera build): ${skipped.join(", ")}`,
            creationTime: new Date().toISOString(),
            creatorName: "Workflow Hub",
            creatorID: 0,
          },
        ],
        commentBoxPosition: { x: xStart, y: y - 120 },
      });
    }

    return {
      operators,
      operatorPositions,
      links,
      commentBoxes,
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
