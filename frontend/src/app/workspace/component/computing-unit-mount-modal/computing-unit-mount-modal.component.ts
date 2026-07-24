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
import { NZ_MODAL_DATA, NzModalService } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgFor, NgIf } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { DatasetSelectionModalComponent } from "../dataset-selection-modal/dataset-selection-modal.component";
import {
  MountedDatasetInfo,
  WorkflowComputingUnitManagingService,
} from "../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { extractErrorMessage } from "../../../common/util/error";

/**
 * Modal for managing the datasets mounted on a single computing unit. Users add a dataset
 * (picked with the shared dataset-selection modal), see everything currently mounted, and
 * unmount individual datasets. Each action proxies through the computing-unit service to
 * that CU's node mounter; state lives on the mounter, so the list is always re-fetched.
 */
@UntilDestroy()
@Component({
  templateUrl: "computing-unit-mount-modal.component.html",
  styleUrls: ["computing-unit-mount-modal.component.scss"],
  imports: [
    NgFor,
    NgIf,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpinComponent,
    NzEmptyComponent,
    NzTooltipDirective,
  ],
})
export class ComputingUnitMountModalComponent implements OnInit {
  private readonly data = inject(NZ_MODAL_DATA) as { cuid: number; computingUnitName?: string };
  readonly cuid = this.data.cuid;

  mounts: MountedDatasetInfo[] = [];
  loading = false;
  mounting = false;
  unmountingPath: string | null = null;

  constructor(
    private modalService: NzModalService,
    private computingUnitService: WorkflowComputingUnitManagingService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.refresh();
  }

  refresh(): void {
    this.loading = true;
    this.computingUnitService
      .listMountedDatasets(this.cuid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: mounts => {
          this.mounts = mounts;
          this.loading = false;
        },
        error: (err: unknown) => {
          this.loading = false;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  onClickAddDataset(): void {
    const modal = this.modalService.create({
      nzContent: DatasetSelectionModalComponent,
      nzFooter: null,
      nzData: {
        fileMode: false,
        selectedPath: null,
      },
      nzBodyStyle: {
        resize: "both",
        overflow: "auto",
        minHeight: "200px",
        minWidth: "550px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });

    modal.afterClose.pipe(untilDestroyed(this)).subscribe((selectedPath: string | undefined) => {
      if (selectedPath) {
        this.mount(selectedPath);
      }
    });
  }

  private mount(datasetPath: string): void {
    if (this.mounts.some(mount => mount.datasetPath === datasetPath)) {
      this.notificationService.warning("That dataset is already mounted on this computing unit.");
      return;
    }
    this.mounting = true;
    this.computingUnitService
      .mountDataset(this.cuid, datasetPath)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.mounting = false;
          this.notificationService.success(`Mounted ${datasetPath}`);
          this.refresh();
        },
        error: (err: unknown) => {
          this.mounting = false;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  onClickUnmount(mount: MountedDatasetInfo): void {
    if (!mount.datasetPath || this.unmountingPath) {
      return;
    }
    this.unmountingPath = mount.datasetPath;
    this.computingUnitService
      .unmountDataset(this.cuid, mount.datasetPath)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.unmountingPath = null;
          this.refresh();
        },
        error: (err: unknown) => {
          this.unmountingPath = null;
          this.notificationService.error(extractErrorMessage(err));
        },
      });
  }

  displayName(mount: MountedDatasetInfo): string {
    return mount.datasetPath || `${mount.repositoryName}:${mount.commitHash}`;
  }
}
