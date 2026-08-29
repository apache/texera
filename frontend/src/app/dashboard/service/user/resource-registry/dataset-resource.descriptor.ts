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

import { Injectable } from "@angular/core";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { ResourceDescriptor } from "../../../type/resource-descriptor";
import { EntityType } from "../../../../hub/service/hub.service";
import { DatasetService, DEFAULT_DATASET_NAME, validateDatasetName } from "../dataset/dataset.service";
import { HUB_DATASET_RESULT_DETAIL, USER_DATASET } from "../../../../app-routing.constant";
import { DownloadService } from "../download/download.service";

@Injectable({
  providedIn: "root",
})
export class DatasetResourceDescriptor implements ResourceDescriptor {
  readonly type = EntityType.Dataset;
  readonly iconType = "database";
  readonly privateRoute = USER_DATASET;
  readonly hubRoute = HUB_DATASET_RESULT_DETAIL;
  readonly hasSize = true;
  readonly defaultName = DEFAULT_DATASET_NAME;

  constructor(
    private datasetService: DatasetService,
    private downloadService: DownloadService
  ) {}

  isOwner = (entry: DashboardEntry): boolean => entry.dataset.isOwner;
  validateName = validateDatasetName;
  rename = (id: number, name: string) => this.datasetService.updateDatasetName(id, name);
  updateDescription = (id: number, description: string) =>
    this.datasetService.updateDatasetDescription(id, description);
  retrieveOwners = () => this.datasetService.retrieveOwners();
  download = (id: number, name: string) => this.downloadService.downloadDataset(id, name);
  retrieveSingleFile = (filePath: string, isLogin: boolean) =>
    this.datasetService.retrieveDatasetVersionSingleFile(filePath, isLogin);
  // No dataset-id endpoint exists, so `retrieveIds` stays absent and the id filter hides itself.
}
