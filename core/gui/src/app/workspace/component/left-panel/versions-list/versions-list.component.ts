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
import { WorkflowVersionCollapsableEntry } from "../../../../dashboard/type/workflow-version-entry";
import { ActivatedRoute } from "@angular/router";

@UntilDestroy()
@Component({
  selector: "texera-version-list",
  templateUrl: "versions-list.component.html",
  styleUrls: ["versions-list.component.scss"],
})
export class VersionsListComponent implements OnInit {
  public versionsList: WorkflowVersionCollapsableEntry[] | undefined;
  public versionTableHeaders: string[] = ["Version#", "Timestamp"];
  public selectedRowIndex: number | null = null;

  constructor(
    private workflowActionService: WorkflowActionService,
    public workflowVersionService: WorkflowVersionService,
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
}
