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

import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from "@angular/core";
import { auditTime } from "rxjs/operators";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { StagedFileService } from "../../../service/user/file-resource/staged-file.service";
import {
  DATASET_FILE_RESOURCE_ENDPOINT,
  FileResourceEndpoint,
} from "../../../service/user/file-resource/file-resource-endpoint";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { formatTime } from "src/app/common/util/format.util";
import { NgIf } from "@angular/common";
import { NzListComponent, NzListItemComponent } from "ng-zorro-antd/list";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { CdkFixedSizeVirtualScroll, CdkVirtualForOf, CdkVirtualScrollViewport } from "@angular/cdk/scrolling";

@UntilDestroy()
@Component({
  selector: "texera-staged-objects-list",
  templateUrl: "./staged-objects-list.component.html",
  styleUrls: ["./staged-objects-list.component.scss"],
  imports: [
    NgIf,
    NzListComponent,
    NzListItemComponent,
    CdkVirtualScrollViewport,
    CdkFixedSizeVirtualScroll,
    CdkVirtualForOf,
    NzTagComponent,
    NzTooltipDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzEmptyComponent,
  ],
})
export class StagedObjectsListComponent implements OnInit {
  // Coalesces change events so a bulk upload refetches the diff at most once
  // per window, not once per finished file.
  static readonly REFRESH_AUDIT_TIME_MS = 1000;

  @Input() resourceId?: number;
  /** Which resource family `resourceId` belongs to. */
  @Input() endpoint: FileResourceEndpoint = DATASET_FILE_RESOURCE_ENDPOINT;
  @Input() set userMakeChangesEvent(event: EventEmitter<void>) {
    if (event) {
      event.pipe(auditTime(StagedObjectsListComponent.REFRESH_AUDIT_TIME_MS), untilDestroyed(this)).subscribe(() => {
        this.fetchStagedObjects();
      });
    }
  }
  @Input() uploadTimeMap?: Map<string, number>;

  @Output() stagedObjectsChanged = new EventEmitter<DatasetStagedObject[]>(); // Emits staged objects list

  stagedObjects: DatasetStagedObject[] = [];
  formatTime = formatTime;

  // Row height must match .staged-object-row in the SCSS.
  readonly STAGED_ROW_HEIGHT_PX = 40;
  readonly STAGED_LIST_MAX_HEIGHT_PX = 200;

  @ViewChild(CdkVirtualScrollViewport) private viewport?: CdkVirtualScrollViewport;

  get stagedListHeightPx(): number {
    return Math.min(this.stagedObjects.length * this.STAGED_ROW_HEIGHT_PX, this.STAGED_LIST_MAX_HEIGHT_PX);
  }

  // The viewport measures height 0 when created inside a hidden ancestor
  // (e.g. a collapsed panel); hosts call this once the list is visible.
  remeasureViewport(): void {
    setTimeout(() => this.viewport?.checkViewportSize());
  }

  constructor(
    private stagedFileService: StagedFileService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.fetchStagedObjects();
  }

  private fetchStagedObjects(): void {
    if (this.resourceId != undefined) {
      this.stagedFileService
        .getDiff(this.endpoint, this.resourceId)
        .pipe(untilDestroyed(this))
        .subscribe(diffs => {
          this.stagedObjects = diffs;
          // Emit the updated staged objects list
          this.stagedObjectsChanged.emit(this.stagedObjects);
        });
    }
  }

  onObjectReverted(objDiff: DatasetStagedObject) {
    if (this.resourceId) {
      this.stagedFileService
        .resetFileDiff(this.endpoint, this.resourceId, objDiff.path)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(`"${objDiff.diffType} ${objDiff.path}" is successfully reverted`);
            this.fetchStagedObjects();
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to delete the file");
          },
        });
    }
  }

  getFileUploadTime(filePath: string): number | null {
    if (!this.uploadTimeMap) return null;

    const filename = filePath.split("/").pop() || filePath;
    return this.uploadTimeMap.get(filename) || null;
  }

  trackByStagedObject(_: number, obj: DatasetStagedObject): string {
    return `${obj.diffType}:${obj.path}`;
  }
}
