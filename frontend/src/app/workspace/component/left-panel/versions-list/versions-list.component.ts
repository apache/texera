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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import { WorkflowExecutionsService } from "../../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowVersionCollapsableEntry } from "../../../../dashboard/type/workflow-version-entry";
import { ActivatedRoute, Router } from "@angular/router";
import { NgIf, NgFor, NgClass, DatePipe } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import {
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzCellAlignDirective,
  NzTbodyComponent,
  NzTdAddOnComponent,
} from "ng-zorro-antd/table";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";

@UntilDestroy()
@Component({
  selector: "texera-version-list",
  templateUrl: "versions-list.component.html",
  styleUrls: ["versions-list.component.scss"],
  imports: [
    NgIf,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NgFor,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzCellAlignDirective,
    NzTbodyComponent,
    NgClass,
    NzTdAddOnComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    DatePipe,
    FormsModule,
    NzCheckboxComponent,
  ],
})
export class VersionsListComponent implements OnInit {
  public versionsList: WorkflowVersionCollapsableEntry[] | undefined;
  public versionTableHeaders: string[] = ["", "Version#", "Timestamp"];
  public selectedRowIndex: number | null = null;
  public compareMode = false;
  public compareSelection = new Set<number>();
  public compareError: string | null = null;
  public compareLoading = false;

  constructor(
    private workflowActionService: WorkflowActionService,
    public workflowVersionService: WorkflowVersionService,
    private workflowExecutionsService: WorkflowExecutionsService,
    private router: Router,
    public route: ActivatedRoute
  ) {}

  public getDisplayedVersionId(index: number, count: number) {
    return count - index;
  }

  collapse(index: number, $event: boolean): void {
    if (this.versionsList == undefined) {
      return;
    }
    if (!$event) {
      while (++index < this.versionsList.length && !this.versionsList[index].importance) {
        this.versionsList[index].expand = false;
      }
    } else {
      while (++index < this.versionsList.length && !this.versionsList[index].importance) {
        this.versionsList[index].expand = true;
      }
    }
  }

  ngOnInit(): void {
    // unhighlight all the current highlighted operators/groups/links
    const elements = this.workflowActionService.getJointGraphWrapper().getCurrentHighlights();
    this.workflowActionService.getJointGraphWrapper().unhighlightElements(elements);
    // gets the versions result and updates the workflow versions table displayed on the form
    const wid = this.route.snapshot.params.id;
    if (wid === undefined) {
      return;
    }
    this.workflowVersionService
      .retrieveVersionsOfWorkflow(wid)
      .pipe(untilDestroyed(this))
      .subscribe(versionsList => {
        this.versionsList = versionsList.map(version => ({
          vId: version.vId,
          creationTime: version.creationTime,
          content: version.content,
          importance: version.importance,
          expand: false,
        }));
      });
  }

  getVersion(vid: number, displayedVersionId: number, index: number) {
    this.selectedRowIndex = index;

    this.workflowVersionService
      .retrieveWorkflowByVersion(<number>this.workflowActionService.getWorkflowMetadata()?.wid, vid)
      .pipe(untilDestroyed(this))
      .subscribe(workflow => {
        this.workflowVersionService.displayParticularVersion(workflow, vid, displayedVersionId);
      });
  }

  toggleCompareMode(): void {
    this.compareMode = !this.compareMode;
    this.compareSelection.clear();
    this.compareError = null;
  }

  isVersionSelectedForCompare(vid: number): boolean {
    return this.compareSelection.has(vid);
  }

  onCompareCheckChange(vid: number, checked: boolean): void {
    if (checked) {
      if (this.compareSelection.size >= 2) {
        // drop the earliest-added entry to keep at most 2
        const first = this.compareSelection.values().next().value;
        if (first !== undefined) this.compareSelection.delete(first);
      }
      this.compareSelection.add(vid);
    } else {
      this.compareSelection.delete(vid);
    }
  }

  canCompare(): boolean {
    return this.compareSelection.size === 2 && !this.compareLoading;
  }

  runCompare(): void {
    if (!this.canCompare()) return;
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (!wid) {
      this.compareError = "Save the workflow first to enable comparison";
      return;
    }
    const [vidA, vidB] = Array.from(this.compareSelection);
    this.compareLoading = true;
    this.compareError = null;
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(wid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: executions => {
          if (executions.length === 0) {
            this.compareLoading = false;
            this.compareError = "This workflow has no executions yet — run it at least twice to compare";
            return;
          }
          // Prefer the latest execution whose vid exactly matches; otherwise fall back to the
          // latest execution at or before the chosen version (closest prior run). Every save
          // creates a new vid, but most versions don't have a dedicated run, so without this
          // fallback comparing any "save without rerun" version always fails.
          const resolve = (vid: number) => {
            const exact = executions.filter(e => e.vId === vid);
            if (exact.length) return exact.reduce((latest, cur) => (cur.eId > latest.eId ? cur : latest));
            const prior = executions.filter(e => e.vId <= vid);
            if (prior.length) return prior.reduce((latest, cur) => (cur.eId > latest.eId ? cur : latest));
            return null;
          };
          const a = resolve(vidA);
          const b = resolve(vidB);
          this.compareLoading = false;
          if (!a || !b) {
            const missing = [!a ? vidA : null, !b ? vidB : null].filter(v => v !== null);
            this.compareError =
              `No execution exists at or before version(s): ${missing.join(", ")}. ` +
              `Run one of those versions first.`;
            return;
          }
          if (a.eId === b.eId) {
            this.compareError =
              "Both selected versions map to the same execution. " +
              "Pick versions that span at least one separate run.";
            return;
          }
          this.router.navigate(["/dashboard/user/workflow", wid, "compare", a.eId, b.eId]);
        },
        error: err => {
          this.compareLoading = false;
          this.compareError = err?.error?.message ?? err?.message ?? "Failed to load executions";
        },
      });
  }
}
