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
import { ActivatedRoute, Router } from "@angular/router";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isDefined } from "../../../../../common/util/predicate";
import { NgIf, NgStyle } from "@angular/common";
import { MarkdownComponent } from "ngx-markdown";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { RouterLink } from "@angular/router";
import { UserWorkflowComponent } from "../../user-workflow/user-workflow.component";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { NzModalService } from "ng-zorro-antd/modal";
import { forkJoin, Observable, of } from "rxjs";
import {
  CreateProjectDialogComponent,
  CreateProjectDialogData,
  CreateProjectDialogResult,
} from "../create-project-dialog/create-project-dialog.component";
import { getProjectIcon, setProjectIcon } from "../project-icon.util";
import { DASHBOARD_USER_PROJECT } from "../../../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-user-project-section",
  templateUrl: "./user-project-section.component.html",
  styleUrls: ["./user-project-section.component.scss"],
  imports: [
    NgIf,
    NgStyle,
    MarkdownComponent,
    NzTooltipDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzTabsModule,
    NzPopconfirmDirective,
    RouterLink,
    UserWorkflowComponent,
    ShareAccessComponent,
  ],
})
export class UserProjectSectionComponent implements OnInit {
  public readonly DASHBOARD_USER_PROJECT = DASHBOARD_USER_PROJECT;

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

  constructor(
    private userProjectService: UserProjectService,
    private activatedRoute: ActivatedRoute,
    private router: Router,
    private notificationService: NotificationService,
    private modalService: NzModalService
  ) {}

  ngOnInit(): void {
    this.activatedRoute.url.pipe(untilDestroyed(this)).subscribe(url => {
      if (url.length == 2 && url[1].path) {
        this.pid = parseInt(url[1].path);
        this.loadProjectMetadata();
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
