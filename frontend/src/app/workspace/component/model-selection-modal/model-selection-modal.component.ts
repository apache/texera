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
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { ModelVersion } from "../../../common/type/model";
import { ResourceType } from "../../../common/type/resource-type";
import { DashboardModel } from "../../../dashboard/type/dashboard-model.interface";
import { ModelService } from "../../../dashboard/service/user/model/model.service";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { NzSelectComponent, NzOptionComponent, NzSelectItemInterface } from "ng-zorro-antd/select";
import { FormsModule } from "@angular/forms";
import { NgFor } from "@angular/common";
import { UserDatasetVersionFiletreeComponent } from "../../../dashboard/component/user/user-dataset/user-dataset-explorer/user-dataset-version-filetree/user-dataset-version-filetree.component";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { datasetMatchesQuery } from "../dataset-selection-modal/dataset-search.util";

/** Picks a model version; closes with its /model/ownerEmail/name/version path. */
@UntilDestroy()
@Component({
  templateUrl: "model-selection-modal.component.html",
  styleUrls: ["model-selection-modal.component.scss"],
  imports: [
    NzRowDirective,
    NzSelectComponent,
    NzColDirective,
    FormsModule,
    NgFor,
    NzOptionComponent,
    UserDatasetVersionFiletreeComponent,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
  ],
})
export class ModelSelectionModalComponent implements OnInit {
  private readonly data = inject(NZ_MODAL_DATA, { optional: true }) as { selectedPath?: string | null } | null;

  models: ReadonlyArray<DashboardModel> = [];
  modelVersions: ReadonlyArray<ModelVersion> = [];
  fileTree: DatasetFileNode[] = [];
  selectedModel?: DashboardModel;
  selectedVersion?: ModelVersion;
  selectedPath?: string;

  constructor(
    private modalRef: NzModalRef,
    private modelService: ModelService
  ) {}

  // Matches the typed text against the model's name and its `#<id>`, as the dataset browser does.
  modelFilterOption = (input: string, option: NzSelectItemInterface): boolean => {
    const model = option.nzValue as DashboardModel | undefined;
    return datasetMatchesQuery(model?.model?.name, model?.model?.mid, input ?? "");
  };

  ngOnInit(): void {
    this.modelService
      .retrieveAccessibleModels()
      .pipe(untilDestroyed(this))
      .subscribe(models => {
        this.models = models;
        const selectedPath = this.data?.selectedPath;
        if (!selectedPath) return;
        const [, ownerEmail, modelName, versionName] = selectedPath.split("/").filter(part => part.length > 0);
        this.selectedModel = models.find(model => model.ownerEmail === ownerEmail && model.model.name === modelName);
        this.onModelChange(versionName);
      });
  }

  onModelChange(versionName?: string): void {
    this.fileTree = [];
    this.selectedVersion = undefined;
    this.selectedPath = undefined;
    const mid = this.selectedModel?.model.mid;
    if (mid === undefined) {
      this.modelVersions = [];
      return;
    }
    this.modelService
      .retrieveModelVersionList(mid)
      .pipe(untilDestroyed(this))
      .subscribe(versions => {
        this.modelVersions = versions;
        const preselected = versions.find(version => version.name === versionName);
        if (preselected) {
          this.selectedVersion = preselected;
          this.onVersionChange();
        }
      });
  }

  onVersionChange(): void {
    const model = this.selectedModel;
    const version = this.selectedVersion;
    this.fileTree = [];
    this.selectedPath = undefined;
    if (model?.model.mid === undefined || version?.mvid === undefined) return;
    this.modelService
      .retrieveModelVersionFileTree(model.model.mid, version.mvid)
      .pipe(untilDestroyed(this))
      .subscribe(data => (this.fileTree = data.fileNodes));
    this.selectedPath = `/${ResourceType.Model}/${model.ownerEmail}/${model.model.name}/${version.name}`;
  }

  onConfirmSelection(): void {
    this.modalRef.close(this.selectedPath);
  }
}
