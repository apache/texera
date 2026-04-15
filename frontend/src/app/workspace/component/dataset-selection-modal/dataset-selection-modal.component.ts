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
    fileMode: boolean;
    selectedPath?: string | null;
  };

  datasets: ReadonlyArray<DashboardDataset> = [];
  datasetVersions: ReadonlyArray<DatasetVersion> = [];
  fileTree: DatasetFileNode[] = [];
  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;
  selectedPath?: string;

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
          this.onDatasetChange(versionName);
        }
      });
  }

  onDatasetChange(versionName?: string) {
    this.fileTree = [];
    if (this.selectedDataset?.dataset.did !== undefined) {
      this.datasetService
        .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.datasetVersions = versions;
          if (this.data.fileMode) {
            this.selectedVersion = versions.find(version => version.name === versionName) ?? versions[0];
            this.onVersionChange();
          }
        });
    }
  }

  onVersionChange() {
    if (this.selectedDataset?.dataset.did !== undefined && this.selectedVersion?.dvid !== undefined) {
      this.selectedPath = undefined;
      this.datasetService
        .retrieveDatasetVersionFileTree(this.selectedDataset.dataset.did, this.selectedVersion.dvid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.fileTree = data.fileNodes;
        });
      if (!this.data.fileMode) {
        this.selectedPath = `/${this.selectedDataset.ownerEmail}/${this.selectedDataset.dataset.name}/${this.selectedVersion.name}`;
      }
    }
  }

  onFileSelected(node: DatasetFileNode) {
    if (this.data.fileMode) {
      this.selectedPath = getFullPathFromDatasetFileNode(node);
    }
  }

  onConfirmSelection() {
    this.modalRef.close(this.selectedPath);
  }
}
