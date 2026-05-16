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
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";
import { ActivatedRoute, Router } from "@angular/router";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isDefined } from "../../../../../common/util/predicate";
import { NgIf, NgStyle, NgFor, DatePipe } from "@angular/common";
import { MarkdownComponent } from "ngx-markdown";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { RouterLink } from "@angular/router";
import { ProjectMembersComponent } from "../project-members/project-members.component";
import { NzModalService } from "ng-zorro-antd/modal";
import { forkJoin, Observable, of } from "rxjs";
import { mergeMap, map } from "rxjs/operators";
import {
  CreateProjectDialogComponent,
  CreateProjectDialogData,
  CreateProjectDialogResult,
} from "../create-project-dialog/create-project-dialog.component";
import { NgbdModalAddProjectDatasetComponent } from "./ngbd-modal-add-project-dataset/ngbd-modal-add-project-dataset.component";
import { NgbdModalAddProjectWorkflowComponent } from "./ngbd-modal-add-project-workflow/ngbd-modal-add-project-workflow.component";
import { getProjectIcon, setProjectIcon } from "../project-icon.util";
import { getProjectDatasetIds, removeDatasetFromProject } from "../project-dataset.util";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";
import { DashboardWorkflow } from "../../../../type/dashboard-workflow.interface";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowContent } from "../../../../../common/type/workflow";
import { GuiConfigService } from "../../../../../common/service/gui-config.service";
import { DASHBOARD_USER_PROJECT, DASHBOARD_USER_WORKSPACE } from "../../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-user-project-section",
  templateUrl: "./user-project-section.component.html",
  styleUrls: ["./user-project-section.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NgStyle,
    DatePipe,
    MarkdownComponent,
    NzTooltipDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTabsModule,
    NzPopconfirmDirective,
    RouterLink,
    ProjectMembersComponent,
  ],
})
export class UserProjectSectionComponent implements OnInit {
  public readonly DASHBOARD_USER_PROJECT = DASHBOARD_USER_PROJECT;
  public readonly DASHBOARD_USER_WORKSPACE = DASHBOARD_USER_WORKSPACE;

  // project metadata
  public pid?: number = undefined;
  public name: string = "";
  public description: string = "";
  public ownerID: number = 0;
  public creationTime: number = 0;
  public accessLevel: string = "READ";
  public color: string | null = null;
  public icon: string = "📁";
  public projectDataIsLoaded: boolean = false;

  public projectWorkflows: DashboardWorkflow[] = [];
  public workflowsLoading = false;

  public datasets: DashboardDataset[] = [];
  public datasetsLoading = false;

  constructor(
    private userProjectService: UserProjectService,
    private datasetService: DatasetService,
    private workflowPersistService: WorkflowPersistService,
    private activatedRoute: ActivatedRoute,
    private router: Router,
    private notificationService: NotificationService,
    private modalService: NzModalService,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    this.activatedRoute.url.pipe(untilDestroyed(this)).subscribe(url => {
      if (url.length == 2 && url[1].path) {
        this.pid = parseInt(url[1].path);
        this.loadProjectMetadata();
        this.refreshWorkflows();
        this.refreshDatasets();
      }
    });
  }

  get bgColorHex(): string {
    return this.color ?? "808080";
  }

  get canEdit(): boolean {
    return this.accessLevel === "WRITE";
  }

  private loadProjectMetadata(): void {
    if (!isDefined(this.pid)) return;
    const pid = this.pid;
    this.icon = getProjectIcon(pid);
    this.userProjectService
      .getProjectList()
      .pipe(untilDestroyed(this))
      .subscribe(userProjectList => {
        if (!userProjectList) return;
        const found = userProjectList.find(p => p.pid === pid);
        if (!found) return;
        this.name = found.name;
        this.description = found.description ?? "";
        this.ownerID = found.ownerId;
        this.creationTime = found.creationTime;
        this.accessLevel = found.accessLevel;
        this.color = found.color;
        this.projectDataIsLoaded = true;
      });
  }

  public openEditDialog(): void {
    if (!isDefined(this.pid)) return;
    const pid = this.pid;
    const data: CreateProjectDialogData = {
      mode: "edit",
      initialName: this.name,
      initialDescription: this.description,
      initialIcon: this.icon,
      initialColor: this.color ?? "808080",
    };
    const modalRef = this.modalService.create({
      nzContent: CreateProjectDialogComponent,
      nzData: data,
      nzFooter: null,
      nzTitle: "Edit project",
      nzCentered: true,
      nzWidth: 520,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe((result: CreateProjectDialogResult | null) => {
      if (!result) return;
      this.applyEdits(pid, result);
    });
  }

  private applyEdits(pid: number, edits: CreateProjectDialogResult): void {
    setProjectIcon(pid, edits.icon);
    this.icon = edits.icon;

    const updates: Observable<unknown>[] = [];
    if (edits.name !== this.name) {
      updates.push(this.userProjectService.updateProjectName(pid, edits.name));
    }
    if (edits.description !== this.description) {
      updates.push(this.userProjectService.updateProjectDescription(pid, edits.description));
    }
    if (edits.color !== (this.color ?? "")) {
      updates.push(this.userProjectService.updateProjectColor(pid, edits.color));
    }

    const after$: Observable<unknown> = updates.length > 0 ? forkJoin(updates) : of(null);
    after$.pipe(untilDestroyed(this)).subscribe({
      next: () => {
        this.name = edits.name;
        this.description = edits.description;
        this.color = edits.color;
      },
      error: (e: unknown) => this.notificationService.error(`Failed to save: ${(e as Error).message}`),
    });
  }

  public refreshWorkflows(): void {
    if (!isDefined(this.pid)) return;
    this.workflowsLoading = true;
    this.userProjectService
      .retrieveWorkflowsOfProject(this.pid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: workflows => {
          this.projectWorkflows = workflows;
          this.workflowsLoading = false;
        },
        error: () => {
          this.projectWorkflows = [];
          this.workflowsLoading = false;
        },
      });
  }

  public openAddWorkflowModal(): void {
    if (!isDefined(this.pid)) return;
    const modalRef = this.modalService.create({
      nzContent: NgbdModalAddProjectWorkflowComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Add Workflows To Project",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.refreshWorkflows());
  }

  public createNewWorkflow(): void {
    if (!isDefined(this.pid)) return;
    const pid = this.pid;
    const emptyContent: WorkflowContent = {
      operators: [],
      commentBoxes: [],
      links: [],
      operatorPositions: {},
      settings: {
        dataTransferBatchSize: this.config.env.defaultDataTransferBatchSize,
        executionMode: this.config.env.defaultExecutionMode,
      },
    };
    this.workflowPersistService
      .createWorkflow(emptyContent, DEFAULT_WORKFLOW_NAME)
      .pipe(
        mergeMap(created => {
          if (created.workflow?.wid === undefined) {
            throw new Error("Workflow creation returned no wid.");
          }
          return this.userProjectService
            .addWorkflowToProject(pid, created.workflow.wid)
            .pipe(map(() => created));
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: created => {
          this.router.navigate([DASHBOARD_USER_WORKSPACE, created.workflow.wid]);
        },
        error: (e: unknown) => this.notificationService.error(`Failed to create workflow: ${(e as Error).message}`),
      });
  }

  public removeWorkflowFromProject(wid: number | undefined): void {
    if (!isDefined(this.pid) || wid === undefined) return;
    this.userProjectService
      .removeWorkflowFromProject(this.pid, wid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success("Workflow removed from project.");
          this.refreshWorkflows();
        },
        error: (e: unknown) => this.notificationService.error(`Failed to remove: ${(e as Error).message}`),
      });
  }

  public trackWorkflow = (_: number, w: DashboardWorkflow) => w.workflow?.wid ?? -1;

  public refreshDatasets(): void {
    if (!isDefined(this.pid)) return;
    const pid = this.pid;
    const wantedIds = new Set(getProjectDatasetIds(pid));
    if (wantedIds.size === 0) {
      this.datasets = [];
      return;
    }
    this.datasetsLoading = true;
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: all => {
          this.datasets = all.filter(d => typeof d.dataset.did === "number" && wantedIds.has(d.dataset.did));
          this.datasetsLoading = false;
        },
        error: () => {
          this.datasets = [];
          this.datasetsLoading = false;
        },
      });
  }

  public openAddDatasetModal(): void {
    if (!isDefined(this.pid)) return;
    const modalRef = this.modalService.create({
      nzContent: NgbdModalAddProjectDatasetComponent,
      nzData: { projectId: this.pid },
      nzFooter: null,
      nzTitle: "Add Existing Dataset",
      nzCentered: true,
      nzWidth: 600,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.refreshDatasets());
  }

  public removeDataset(did: number): void {
    if (!isDefined(this.pid) || typeof did !== "number") return;
    removeDatasetFromProject(this.pid, did);
    this.refreshDatasets();
  }

  public formatBytes(bytes: number): string {
    if (!bytes || bytes < 0) return "—";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    let size = bytes;
    while (size >= 1024 && i < units.length - 1) {
      size /= 1024;
      i++;
    }
    return `${size.toFixed(size >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  public trackDataset = (_: number, d: DashboardDataset) => d.dataset.did ?? -1;

  public confirmDelete(): void {
    if (!isDefined(this.pid)) return;
    this.userProjectService
      .deleteProject(this.pid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Deleted project "${this.name}".`);
          this.router.navigate([DASHBOARD_USER_PROJECT]);
        },
        error: (e: unknown) => this.notificationService.error(`Failed to delete: ${(e as Error).message}`),
      });
  }
}
