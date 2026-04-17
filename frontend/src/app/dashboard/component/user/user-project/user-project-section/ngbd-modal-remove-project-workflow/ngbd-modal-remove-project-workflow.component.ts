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
import { forkJoin, Observable } from "rxjs";
import { UserProjectService } from "../../../../../service/user/project/user-project.service";
import { DashboardWorkflow } from "../../../../../type/dashboard-workflow.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

@UntilDestroy()
@Component({
  selector: "texera-remove-project-workflow-modal",
  templateUrl: "./ngbd-modal-remove-project-workflow.component.html",
  styleUrls: ["./ngbd-modal-remove-project-workflow.component.scss"],
})
export class NgbdModalRemoveProjectWorkflowComponent implements OnInit {
  readonly projectId: number = inject(NZ_MODAL_DATA).projectId;

  public checkedWorkflows: boolean[] = []; // used to implement check boxes
  public addedWorkflows: DashboardWorkflow[] = []; // for passing back to update the frontend cache, stores the new workflow list with selected ones removed

  constructor(private userProjectService: UserProjectService) {}

  ngOnInit(): void {
    this.refreshProjectWorkflowEntries();
  }

  public submitForm() {
    let observables: Observable<Response>[] = [];

    for (let index = this.checkedWorkflows.length - 1; index >= 0; --index) {
      if (this.checkedWorkflows[index]) {
        observables.push(
          this.userProjectService.removeWorkflowFromProject(this.projectId, this.addedWorkflows[index].workflow.wid!)
        );
        this.addedWorkflows.splice(index, 1); // for updating frontend cache
      }
    }

    forkJoin(observables).pipe(untilDestroyed(this)).subscribe();
  }

  public isAllChecked() {
    return this.checkedWorkflows.length > 0 && this.checkedWorkflows.every(isChecked => isChecked);
  }

  public changeAll() {
    if (this.isAllChecked()) {
      this.checkedWorkflows.fill(false);
    } else {
      this.checkedWorkflows.fill(true);
    }
  }

  private refreshProjectWorkflowEntries(): void {
    this.userProjectService
      .retrieveWorkflowsOfProject(this.projectId)
      .pipe(untilDestroyed(this))
      .subscribe(dashboardWorkflowEntries => {
        this.addedWorkflows = dashboardWorkflowEntries;
        this.checkedWorkflows = new Array(this.addedWorkflows.length).fill(false);
      });
  }
}
