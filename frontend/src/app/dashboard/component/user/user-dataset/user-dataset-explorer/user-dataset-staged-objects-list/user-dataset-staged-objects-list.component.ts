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

import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DatasetStagedObject } from "../../../../../../common/type/dataset-staged-object";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { formatTime } from "src/app/common/util/format.util";

@UntilDestroy()
@Component({
  selector: "texera-dataset-staged-objects-list",
  templateUrl: "./user-dataset-staged-objects-list.component.html",
  styleUrls: ["./user-dataset-staged-objects-list.component.scss"],
})
export class UserDatasetStagedObjectsListComponent implements OnInit {
  @Input() did?: number; // Dataset ID
  @Input() set userMakeChangesEvent(event: EventEmitter<void>) {
    if (event) {
      event.pipe(untilDestroyed(this)).subscribe(() => {
        this.fetchDatasetStagedObjects();
      });
    }
  }
  @Input() uploadTimeMap?: Map<string, number>;

  @Output() stagedObjectsChanged = new EventEmitter<DatasetStagedObject[]>(); // Emits staged objects list

  datasetStagedObjects: DatasetStagedObject[] = [];
  formatTime = formatTime;

  constructor(
    private datasetService: DatasetService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.fetchDatasetStagedObjects();
  }

  private fetchDatasetStagedObjects(): void {
    if (this.did != undefined) {
      this.datasetService
        .getDatasetDiff(this.did)
        .pipe(untilDestroyed(this))
        .subscribe(diffs => {
          this.datasetStagedObjects = diffs;
          // Emit the updated staged objects list
          this.stagedObjectsChanged.emit(this.datasetStagedObjects);
        });
    }
  }

  onObjectReverted(objDiff: DatasetStagedObject) {
    if (this.did) {
      this.datasetService
        .resetDatasetFileDiff(this.did, objDiff.path)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(`"${objDiff.diffType} ${objDiff.path}" is successfully reverted`);
            this.fetchDatasetStagedObjects();
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
}
