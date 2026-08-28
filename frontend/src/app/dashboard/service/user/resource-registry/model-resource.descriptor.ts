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

@Injectable({
  providedIn: "root",
})
export class ModelResourceDescriptor implements ResourceDescriptor {
  readonly type = EntityType.Model;
  readonly iconType = MODEL_ICON;
  // `privateRoute` is deliberately absent: /user/model/:mid has no component yet, so
  // entryLink returns [] and a model card does not navigate.
  readonly hasSize = true;
  readonly defaultName = DEFAULT_MODEL_NAME;

  constructor(private modelService: ModelService) {}

  isOwner = (entry: DashboardEntry): boolean => entry.model.isOwner;
  validateName = validateModelName;
  rename = (id: number, name: string) => this.modelService.updateModelName(id, name);
  updateDescription = (id: number, description: string) => this.modelService.updateModelDescription(id, description);
  // `retrieveOwners` arrives with the share modal and the filters, which need it.
}
