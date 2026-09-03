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
import { ResourceAffordances, ResourceDescriptor } from "../../../type/resource-descriptor";
import { EntityType } from "../../../../hub/service/hub.service";
import { DEFAULT_MODEL_NAME, ModelService, validateModelName } from "../model/model.service";
import { MODEL_ICON } from "../../../../common/icon/model-icon";
import { HUB_MODEL_RESULT_DETAIL, USER_MODEL } from "../../../../app-routing.constant";
import { DownloadService } from "../download/download.service";
import { map } from "rxjs/operators";

@Injectable({
  providedIn: "root",
})
export class ModelResourceDescriptor implements ResourceDescriptor {
  readonly type = EntityType.Model;
  readonly iconType = MODEL_ICON;
  readonly privateRoute = USER_MODEL;
  readonly hubRoute = HUB_MODEL_RESULT_DETAIL;
  readonly hasSize = true;
  readonly defaultName = DEFAULT_MODEL_NAME;
  // Publishing a model grants read access only; there is no clone action for it.
  readonly affordances: ResourceAffordances = { clonable: false };

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
  retrieveOwners = () => this.modelService.retrieveOwners();
  isPublic = (id: number) => this.modelService.getModel(id).pipe(map(dashboard => dashboard.model.isPublic));
  // The endpoint toggles, so `next` is the caller's expectation rather than a payload.
  setPublished = (id: number) => this.modelService.updateModelPublicity(id);
  coverUrl = (id: number) => this.modelService.getModelCoverUrl(id).pipe(map(({ url }) => url));
  setCover = (id: number, path: string) => this.modelService.updateModelCoverImage(id, path);
}
