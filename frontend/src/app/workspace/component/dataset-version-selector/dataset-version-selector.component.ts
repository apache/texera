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

import { ChangeDetectorRef, Component, OnInit } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetVersion } from "../../../common/type/dataset";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-dataset-version-selector-template",
  templateUrl: "./dataset-version-selector.component.html",
})
export class DatasetVersionSelectorComponent extends FieldType<FieldTypeConfig> implements OnInit {
  datasets: ReadonlyArray<DashboardDataset> = [];
  datasetVersions: ReadonlyArray<DatasetVersion> = [];
  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;

  constructor(
    private datasetService: DatasetService,
    private changeDetectorRef: ChangeDetectorRef
  ) {
    super();
  }

  ngOnInit(): void {
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe(datasets => {
        this.datasets = datasets;
        const path = this.formControl.value.split("/");
        if (path) {
          const [, ownerEmail, datasetName] = this.formControl.value.split("/");
          this.selectedDataset = this.datasets.find(
            dataset => dataset.ownerEmail === ownerEmail && dataset.dataset.name === datasetName
          );
          this.onDatasetChange();
        }
      });
  }

  onDatasetChange(): void {
    if (this.selectedDataset) {
      this.datasetService
        .retrieveDatasetVersionList(this.selectedDataset.dataset.did!)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.datasetVersions = versions;
          this.selectedVersion = versions[0];
          this.onVersionChange();
          this.changeDetectorRef.detectChanges();
        });
    } else {
      this.selectedVersion = undefined;
      this.onVersionChange();
    }
  }

  onVersionChange(): void {
    this.formControl.setValue(
      this.selectedDataset && this.selectedVersion
        ? `/${this.selectedDataset?.ownerEmail}/${this.selectedDataset?.dataset?.name}/${this.selectedVersion?.name}`
        : null
    );
  }
}
