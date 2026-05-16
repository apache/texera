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
import { UserProjectService } from "../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UserService } from "../../../../common/service/user/user.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { PublicProjectComponent } from "./public-project/public-project.component";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzDropdownADirective, NzDropdownDirective, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { NzMenuDirective, NzMenuItemComponent } from "ng-zorro-antd/menu";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NgIf, NgFor } from "@angular/common";
import { UserProjectListItemComponent } from "./user-project-list-item/user-project-list-item.component";
import {
  CreateProjectDialogComponent,
  CreateProjectDialogData,
  CreateProjectDialogResult,
} from "./create-project-dialog/create-project-dialog.component";
import { setProjectIcon } from "./project-icon.util";
import { forkJoin, Observable, of } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list",
  templateUrl: "./user-project.component.html",
  styleUrls: ["./user-project.component.scss"],
  imports: [
    NgIf,
    NgFor,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzDropdownADirective,
    NzDropdownDirective,
    NzDropdownMenuComponent,
    NzMenuDirective,
    NzMenuItemComponent,
    NzTooltipDirective,
    UserProjectListItemComponent,
  ],
})
export class UserProjectComponent implements OnInit {
  public userProjectEntries: DashboardProject[] = [];
  public uid: number | undefined;
  public loading = false;

  constructor(
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private userService: UserService,
    private modalService: NzModalService
  ) {
    this.uid = this.userService.getCurrentUser()?.uid;
  }

  ngOnInit(): void {
    this.loadProjects();
  }

  public loadProjects(): void {
    this.loading = true;
    this.userProjectService
      .getProjectList()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: projectEntries => {
          this.userProjectEntries = projectEntries;
          this.loading = false;
        },
        error: () => {
          this.loading = false;
        },
      });
  }

  public deleteProject(pid: number): void {
    this.userProjectService
      .deleteProject(pid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.loadProjects());
  }

  public openCreateDialog(): void {
    const data: CreateProjectDialogData = { mode: "create" };
    const modalRef = this.modalService.create({
      nzContent: CreateProjectDialogComponent,
      nzData: data,
      nzFooter: null,
      nzTitle: "Create new project",
      nzCentered: true,
      nzWidth: 520,
    });

    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe((result: CreateProjectDialogResult | null) => {
      if (!result) return;
      if (!this.isValidNewProjectName(result.name)) {
        this.notificationService.error(
          `Cannot create project named: "${result.name}". It must be a non-empty, unique name.`
        );
        return;
      }
      this.createProjectWithDetails(result);
    });
  }

  private createProjectWithDetails(input: CreateProjectDialogResult): void {
    this.userProjectService
      .createProject(input.name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: created => {
          setProjectIcon(created.pid, input.icon);
          const followups: Observable<Response>[] = [];
          if (input.description) {
            followups.push(this.userProjectService.updateProjectDescription(created.pid, input.description));
          }
          if (input.color) {
            followups.push(this.userProjectService.updateProjectColor(created.pid, input.color));
          }
          const after$: Observable<unknown> = followups.length > 0 ? forkJoin(followups) : of(null);
          after$
            .pipe(untilDestroyed(this))
            .subscribe({
              next: () => this.loadProjects(),
              error: (e: unknown) => {
                this.notificationService.error(`Project created but failed to apply some details: ${(e as Error).message}`);
                this.loadProjects();
              },
            });
        },
        error: (e: unknown) => {
          this.notificationService.error(`Failed to create project: ${(e as Error).message}`);
        },
      });
  }

  public trackByPid(_index: number, project: DashboardProject): number {
    return project.pid;
  }

  public sortByCreationTime(): void {
    this.userProjectEntries = [...this.userProjectEntries].sort((p1, p2) =>
      p1.creationTime !== undefined && p2.creationTime !== undefined ? p2.creationTime - p1.creationTime : 0
    );
  }

  public sortByNameAsc(): void {
    this.userProjectEntries = [...this.userProjectEntries].sort((p1, p2) =>
      p1.name.toLowerCase().localeCompare(p2.name.toLowerCase())
    );
  }

  public sortByNameDesc(): void {
    this.userProjectEntries = [...this.userProjectEntries].sort((p1, p2) =>
      p2.name.toLowerCase().localeCompare(p1.name.toLowerCase())
    );
  }

  private isValidNewProjectName(newName: string, oldProject?: DashboardProject): boolean {
    if (typeof oldProject === "undefined") {
      return newName.length !== 0 && this.userProjectEntries.filter(project => project.name === newName).length === 0;
    }
    return (
      newName.length !== 0 &&
      this.userProjectEntries.filter(project => project.pid !== oldProject.pid && project.name === newName).length === 0
    );
  }

  public openPublicProject(): void {
    const modalRef = this.modalService.create({
      nzContent: PublicProjectComponent,
      nzData: { disabledList: new Set(this.userProjectEntries.map(project => project.pid)) },
      nzFooter: null,
      nzTitle: "Add Public Projects",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.loadProjects());
  }
}
