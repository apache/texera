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
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../common/type/dataset";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  templateUrl: "dataset-selection-modal.component.html",
  styleUrls: ["dataset-selection-modal.component.scss"],
})
export class DatasetSelectionModalComponent implements OnInit {
  private readonly data = inject(NZ_MODAL_DATA) as {
    selectFile: boolean;
    selectedPath?: string | null;
  };

  readonly selectFile: boolean = this.data.selectFile;

  loading = true;

  datasets: ReadonlyArray<DashboardDataset> = [];
  datasetVersions: ReadonlyArray<DatasetVersion> = [];
  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;
  suggestedFileTreeNodes: DatasetFileNode[] = [];
  selectedFilePath?: string;

  constructor(
    private modalRef: NzModalRef,
    private datasetService: DatasetService
  ) {}

  ngOnInit() {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        this.datasets = datasets;
        const selectedPath = this.data.selectedPath;
        if (selectedPath) {
          const [ownerEmail, datasetName, versionName] = selectedPath.split("/").filter(part => part.length > 0);
          this.selectedDataset = this.datasets.find(
            dataset => dataset.ownerEmail === ownerEmail && dataset.dataset.name === datasetName
          );
          this.loadDatasetVersions(versionName);
        }
        this.loading = false;
      });
  }

  onDatasetChange() {
    this.selectedVersion = undefined;
    this.selectedFilePath = undefined;
    this.suggestedFileTreeNodes = [];
    this.loadDatasetVersions();
  }

  onVersionChange() {
    this.selectedFilePath = undefined;
    this.suggestedFileTreeNodes = [];

    if (!this.selectFile && this.selectedDataset && this.selectedVersion) {
      this.selectedFilePath =
        `/${this.selectedDataset.ownerEmail}/${this.selectedDataset.dataset.name}/${this.selectedVersion.name}`;
    }

    if (
      this.selectFile &&
      this.selectedDataset?.dataset.did !== undefined &&
      this.selectedVersion?.dvid !== undefined
    ) {
      this.datasetService
        .retrieveDatasetVersionFileTree(this.selectedDataset.dataset.did, this.selectedVersion.dvid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.suggestedFileTreeNodes = data.fileNodes;
        });
    }
  }

  onFileTreeNodeSelected(node: DatasetFileNode) {
    this.selectedFilePath = getFullPathFromDatasetFileNode(node)
  }

  onConfirmSelection(): void {
    this.modalRef.close(this.selectedFilePath);
  }

  private loadDatasetVersions(preferredVersionName?: string): void {
    if (this.selectedDataset?.dataset.did === undefined) {
      this.datasetVersions = [];
      return;
    }

    this.datasetService
      .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
      .pipe(untilDestroyed(this))
      .subscribe(versions => {
        this.datasetVersions = versions;
        this.selectedVersion =
          versions.find(version => version.name === preferredVersionName) ?? versions[0];
        this.onVersionChange();
      });
  }
}
