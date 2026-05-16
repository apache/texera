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
import { forkJoin, Observable, of } from "rxjs";
import { concatMap } from "rxjs/operators";
import { WorkflowPersistService } from "../../../../../../common/service/workflow-persist/workflow-persist.service";
import { DashboardWorkflow } from "../../../../../type/dashboard-workflow.interface";
import { UserProjectService } from "../../../../../service/user/project/user-project.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { HttpErrorResponse } from "@angular/common/http";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NgFor, NgIf, DatePipe } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";

@UntilDestroy()
@Component({
  selector: "texera-add-project-workflow-modal",
  templateUrl: "./ngbd-modal-add-project-workflow.component.html",
  styleUrls: ["./ngbd-modal-add-project-workflow.component.scss"],
  imports: [NgFor, NgIf, FormsModule, DatePipe, NzButtonComponent, NzIconDirective],
})
export class NgbdModalAddProjectWorkflowComponent implements OnInit {
  readonly projectId: number = inject(NZ_MODAL_DATA).projectId;

  public unaddedWorkflows: DashboardWorkflow[] = [];
  public checked: boolean[] = [];
  public searchTerm = "";
  public loading = false;

  private addedWorkflowKeys: Set<number> = new Set<number>();

  constructor(
    private workflowPersistService: WorkflowPersistService,
    private userProjectService: UserProjectService,
    private notificationService: NotificationService,
    private modalRef: NzModalRef
  ) {}

  ngOnInit(): void {
    this.refresh();
  }

  get filteredIndices(): number[] {
    const q = this.searchTerm.trim().toLowerCase();
    if (!q) return this.unaddedWorkflows.map((_, i) => i);
    return this.unaddedWorkflows
      .map((w, i) => ({ w, i }))
      .filter(({ w }) => (w.workflow?.name ?? "").toLowerCase().includes(q))
      .map(({ i }) => i);
  }

  public selectedCount(): number {
    return this.checked.filter(Boolean).length;
  }

  public anyChecked(): boolean {
    return this.checked.some(Boolean);
  }

  public isAllChecked(): boolean {
    const indices = this.filteredIndices;
    return indices.length > 0 && indices.every(i => this.checked[i]);
  }

  public toggleAll(): void {
    const fill = !this.isAllChecked();
    this.filteredIndices.forEach(i => (this.checked[i] = fill));
  }

  public confirm(): void {
    const selected: number[] = [];
    const ops: Observable<Response>[] = [];
    for (let i = 0; i < this.checked.length; i++) {
      const wid = this.unaddedWorkflows[i].workflow?.wid;
      if (this.checked[i] && wid !== undefined) {
        selected.push(wid);
        ops.push(this.userProjectService.addWorkflowToProject(this.projectId, wid));
      }
    }
    if (ops.length === 0) {
      this.modalRef.close(null);
      return;
    }
    forkJoin(ops)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(
            `Added ${selected.length} workflow${selected.length === 1 ? "" : "s"} to project.`
          );
          // Pass the wids back so the parent can verify / force-refresh.
          this.modalRef.close({ addedWids: selected });
        },
        error: (e: unknown) => {
          const msg = e instanceof HttpErrorResponse ? e.error?.message ?? e.message : (e as Error).message;
          this.notificationService.error(`Failed to add workflows: ${msg ?? e}`);
          // Close anyway so the parent refreshes and partial adds appear.
          this.modalRef.close({ addedWids: selected, error: true });
        },
      });
  }

  public cancel(): void {
    this.modalRef.close(false);
  }

  private refresh(): void {
    this.loading = true;
    this.userProjectService
      .retrieveWorkflowsOfProject(this.projectId)
      .pipe(
        concatMap(existing => {
          existing.forEach(e => {
            if (e.workflow?.wid !== undefined) this.addedWorkflowKeys.add(e.workflow.wid);
          });
          return this.workflowPersistService.retrieveWorkflowsBySessionUser();
        }),
        untilDestroyed(this)
      )
      .subscribe({
        next: all => {
          this.unaddedWorkflows = all.filter(
            w => w.workflow?.wid !== undefined && !this.addedWorkflowKeys.has(w.workflow.wid)
          );
          this.checked = new Array(this.unaddedWorkflows.length).fill(false);
          this.loading = false;
        },
        error: () => {
          this.unaddedWorkflows = [];
          this.checked = [];
          this.loading = false;
        },
      });
  }
}
