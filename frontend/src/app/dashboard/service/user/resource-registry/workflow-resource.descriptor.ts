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
import { EntityType, HubService } from "../../../../hub/service/hub.service";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "../../../../common/service/workflow-persist/workflow-persist.service";
import { HUB_WORKFLOW_RESULT_DETAIL, USER_WORKSPACE } from "../../../../app-routing.constant";
import { DownloadService } from "../download/download.service";
import { map } from "rxjs/operators";

@Injectable({
  providedIn: "root",
})
export class WorkflowResourceDescriptor implements ResourceDescriptor {
  readonly type = EntityType.Workflow;
  readonly iconType = "project";
  readonly privateRoute = USER_WORKSPACE;
  readonly hubRoute = HUB_WORKFLOW_RESULT_DETAIL;
  readonly hasSize = true;
  readonly defaultName = DEFAULT_WORKFLOW_NAME;
  readonly affordances: ResourceAffordances = { clonable: true };

  constructor(
    private workflowPersistService: WorkflowPersistService,
    private hubService: HubService,
    private downloadService: DownloadService
  ) {}

  // Bound lazily, never in the constructor: specs hand these descriptors partial service spies,
  // and reading an absent method up front would fail the whole TestBed.
  isOwner = (entry: DashboardEntry): boolean => entry.workflow.isOwner;
  rename = (id: number, name: string) => this.workflowPersistService.updateWorkflowName(id, name);
  updateDescription = (id: number, description: string) =>
    this.workflowPersistService.updateWorkflowDescription(id, description);
  retrieveOwners = () => this.workflowPersistService.retrieveOwners();
  retrievePublicOwners = () => this.hubService.getPublicOwners(EntityType.Workflow);
  retrieveIds = () => this.workflowPersistService.retrieveWorkflowIDs();
  download = (id: number, name: string) => this.downloadService.downloadWorkflow(id, name);
  isPublic = (id: number) =>
    this.workflowPersistService.getWorkflowIsPublished(id).pipe(map(published => published === "Public"));
  setPublished = (id: number, next: boolean) => this.workflowPersistService.updateWorkflowIsPublished(id, next);
  // No `coverUrl`/`setCover`: a workflow cover is a data URL carried on the entry itself, not a
  // committed file that has to be fetched.
}
