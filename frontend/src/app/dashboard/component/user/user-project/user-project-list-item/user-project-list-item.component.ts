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

import { Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChanges } from "@angular/core";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { ShareAccessService } from "../../../../service/user/share-access/share-access.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DASHBOARD_USER_PROJECT } from "../../../../../app-routing.constant";
import { NgStyle, NgIf, DatePipe } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { RouterLink } from "@angular/router";
import { HighlightSearchTermsPipe } from "../../user-workflow/user-workflow-list-item/highlight-search-terms.pipe";
import { getProjectIcon } from "../project-icon.util";

const DEFAULT_BG_COLOR = "808080";

@UntilDestroy()
@Component({
  selector: "texera-user-project-list-item",
  templateUrl: "./user-project-list-item.component.html",
  styleUrls: ["./user-project-list-item.component.scss"],
  imports: [
    NgStyle,
    NgIf,
    RouterLink,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzWaveDirective,
    NzPopconfirmDirective,
    DatePipe,
    HighlightSearchTermsPipe,
  ],
})
export class UserProjectListItemComponent implements OnInit, OnChanges {
  public readonly ROUTER_USER_PROJECT_BASE_URL = DASHBOARD_USER_PROJECT;

  private _entry?: DashboardProject;
  @Input() public keywords: string[] = [];
  @Input() public editable = false;
  @Input() public uid: number | undefined;
  @Output() deleted = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();

  @Input()
  get entry(): DashboardProject {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserProjectListItemComponent.");
    }
    return this._entry;
  }
  set entry(value: DashboardProject) {
    this._entry = value;
  }

  workflowCount = 0;
  memberCount = 1;
  datasetCount = 0;
  lastUpdatedMs?: number;
  icon = "📁";

  constructor(
    private userProjectService: UserProjectService,
    private shareAccessService: ShareAccessService
  ) {}

  ngOnInit(): void {
    this.refreshStats();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"] && !changes["entry"].firstChange) {
      this.refreshStats();
    }
  }

  get bgColorHex(): string {
    return this.entry.color ?? DEFAULT_BG_COLOR;
  }

  get descriptionPreview(): string {
    return (this.entry.description ?? "").trim();
  }

  get relativeUpdatedLabel(): string {
    const ts = this.lastUpdatedMs ?? this.entry.creationTime;
    if (!ts) return "";
    return formatRelative(ts);
  }

  private refreshStats(): void {
    this.icon = getProjectIcon(this.entry.pid);

    this.userProjectService
      .retrieveWorkflowsOfProject(this.entry.pid)
      .pipe(untilDestroyed(this))
      .subscribe(workflows => {
        this.workflowCount = workflows.length;
        const times = workflows
          .map(w => w.workflow?.lastModifiedTime ?? w.workflow?.creationTime)
          .filter((t): t is number => typeof t === "number");
        if (times.length > 0) {
          this.lastUpdatedMs = Math.max(...times);
        }
      });

    this.shareAccessService
      .getAccessList("project", this.entry.pid)
      .pipe(untilDestroyed(this))
      .subscribe(access => {
        // owner + shared collaborators
        this.memberCount = access.length + 1;
      });
  }
}

function formatRelative(timestampMs: number): string {
  const diff = Date.now() - timestampMs;
  if (diff < 0) return "just now";
  const minutes = Math.floor(diff / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours === 1 ? "" : "s"} ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days} day${days === 1 ? "" : "s"} ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months} month${months === 1 ? "" : "s"} ago`;
  const years = Math.floor(months / 12);
  return `${years} year${years === 1 ? "" : "s"} ago`;
}
