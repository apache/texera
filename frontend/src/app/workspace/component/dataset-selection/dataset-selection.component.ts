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

type DatasetSelectionMode = "file" | "version";

interface DatasetSelectionModalData {
  mode: DatasetSelectionMode;
  selectedPath?: string | null;
}

interface ParsedDatasetVersionPath {
  ownerEmail: string;
  datasetName: string;
  versionName: string;
}

@UntilDestroy()
@Component({
  selector: "texera-dataset-selection-modal",
  templateUrl: "dataset-selection.component.html",
  styleUrls: ["dataset-selection.component.scss"],
})
export class DatasetSelectionComponent implements OnInit {
  private readonly data: DatasetSelectionModalData = inject(NZ_MODAL_DATA);
  private _datasets: ReadonlyArray<DashboardDataset> = [];

  readonly mode: DatasetSelectionMode = this.data.mode;
  readonly selectedPath: string = this.data.selectedPath ?? "";

  isAccessibleDatasetsLoading = true;

  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;
  selectedFileNode?: DatasetFileNode;
  datasetVersions?: DatasetVersion[];
  suggestedFileTreeNodes: DatasetFileNode[] = [];
  isDatasetSelected = false;

  constructor(
    private modalRef: NzModalRef,
    private datasetService: DatasetService
  ) {}

  ngOnInit() {
    this.isAccessibleDatasetsLoading = true;

    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        this._datasets = datasets;
        this.isAccessibleDatasetsLoading = false;

        if (!this.selectedPath) {
          return;
        }

        const parsedPath = this.parseDatasetVersionPath(this.selectedPath);
        this.selectedDataset = this.datasets.find(
          dataset =>
            dataset.ownerEmail === parsedPath.ownerEmail &&
            dataset.dataset.name === parsedPath.datasetName
        );
        this.isDatasetSelected = !!this.selectedDataset;

        if (this.selectedDataset?.dataset.did !== undefined) {
          this.datasetService
            .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
            .pipe(untilDestroyed(this))
            .subscribe(versions => {
              this.datasetVersions = versions;
              this.selectedVersion =
                this.datasetVersions.find(version => version.name === parsedPath.versionName) ??
                this.datasetVersions[0];
              this.onVersionChange();
            });
        }
      });
  }

  onDatasetChange() {
    this.selectedVersion = undefined;
    this.selectedFileNode = undefined;
    this.suggestedFileTreeNodes = [];
    this.isDatasetSelected = !!this.selectedDataset;

    if (this.selectedDataset?.dataset.did !== undefined) {
      this.datasetService
        .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.datasetVersions = versions;
          if (this.datasetVersions.length > 0) {
            this.selectedVersion = this.datasetVersions[0];
            this.onVersionChange();
          }
        });
    }
  }

  onVersionChange() {
    this.selectedFileNode = undefined;
    this.suggestedFileTreeNodes = [];

    if (
      this.mode === "file" &&
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
    this.selectedFileNode = node.type === "file" ? node : undefined;
  }

  onConfirmSelection(): void {
    if (this.mode === "version") {
      if (this.selectedDataset && this.selectedVersion) {
        this.modalRef.close(
          `/${this.selectedDataset.ownerEmail}/${this.selectedDataset.dataset.name}/${this.selectedVersion.name}`
        );
      }
      return;
    }

    if (this.selectedFileNode) {
      this.modalRef.close(getFullPathFromDatasetFileNode(this.selectedFileNode));
    }
  }

  get datasets(): ReadonlyArray<DashboardDataset> {
    return this._datasets;
  }

  get confirmButtonText(): string {
    return this.mode === "version" ? "Select Dataset" : "Select File";
  }

  get isConfirmDisabled(): boolean {
    return this.mode === "version"
      ? !(this.selectedDataset && this.selectedVersion)
      : !this.selectedFileNode;
  }

  private parseDatasetVersionPath(path: string): ParsedDatasetVersionPath {
    const parts = path.split("/").filter(part => part.length > 0);

    if (parts.length < 3) {
      throw new Error("Invalid dataset version path format");
    }

    const [ownerEmail, datasetName, versionName] = parts;
    return { ownerEmail, datasetName, versionName };
  }
}
