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
import { DEFAULT_MODEL_NAME, ModelService, validateModelName } from "../model/model.service";
import { MODEL_ICON } from "../../../../common/icon/model-icon";
import { USER_MODEL } from "../../../../app-routing.constant";
import { DownloadService } from "../download/download.service";

@Injectable({
  providedIn: "root",
})
export class ModelResourceDescriptor implements ResourceDescriptor {
  readonly type = EntityType.Model;
  readonly iconType = MODEL_ICON;
  readonly privateRoute = USER_MODEL;
  // `hubRoute` is deliberately absent: models reach the hub with the rest of the hub UI.
  readonly hasSize = true;
  readonly defaultName = DEFAULT_MODEL_NAME;

  constructor(
    private modelService: ModelService,
    private downloadService: DownloadService
  ) {}

  isOwner = (entry: DashboardEntry): boolean => entry.model.isOwner;
  validateName = validateModelName;
  rename = (id: number, name: string) => this.modelService.updateModelName(id, name);
  updateDescription = (id: number, description: string) => this.modelService.updateModelDescription(id, description);
  download = (id: number, name: string) => this.downloadService.downloadModel(id, name);
  retrieveSingleFile = (filePath: string, isLogin: boolean) =>
    this.modelService.retrieveModelVersionSingleFile(filePath, isLogin);
  // `retrieveOwners` arrives with the share modal and the filters, which need it.
}
