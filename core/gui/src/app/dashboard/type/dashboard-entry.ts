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

import { DashboardFile } from "./dashboard-file.interface";
import { DashboardWorkflow } from "./dashboard-workflow.interface";
import { DashboardProject } from "./dashboard-project.interface";
import { DashboardDataset } from "./dashboard-dataset.interface";
import { isDashboardDataset, isDashboardFile, isDashboardProject, isDashboardWorkflow } from "./type-predicates";
import { EntityType } from "../../hub/service/hub.service";

export interface UserInfo {
  userName: string;
  googleAvatar?: string;
}

export class DashboardEntry {
  checked = false;
  type: EntityType;
  name: string;
  creationTime: number | undefined;
  lastModifiedTime: number | undefined;
  id: number | undefined;
  description: string | undefined;
  accessLevel: string | undefined;
  ownerName: string | undefined;
  ownerEmail: string | undefined;
  ownerGoogleAvatar: string | undefined;
  ownerId: number | undefined;
  size: number | undefined;
  viewCount: number;
  cloneCount: number;
  likeCount: number;
  isLiked: boolean;
  accessibleUserIds: number[];

  constructor(public value: DashboardWorkflow | DashboardProject | DashboardFile | DashboardDataset) {
    if (isDashboardWorkflow(value)) {
      this.type = EntityType.Workflow;
      this.id = value.workflow.wid;
      this.name = value.workflow.name;
      this.description = value.workflow.description;
      this.creationTime = value.workflow.creationTime;
      this.lastModifiedTime = value.workflow.lastModifiedTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = value.ownerName;
      this.ownerEmail = "";
      this.ownerGoogleAvatar = "";
      this.ownerId = value.ownerId;
      this.size = 0;
      this.viewCount = 0;
      this.cloneCount = 0;
      this.likeCount = 0;
      this.isLiked = false;
      this.accessibleUserIds = [];
    } else if (isDashboardProject(value)) {
      this.type = EntityType.Project;
      this.id = value.pid;
      this.name = value.name;
      this.description = "";
      this.creationTime = value.creationTime;
      this.lastModifiedTime = value.creationTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = "";
      this.ownerEmail = "";
      this.ownerGoogleAvatar = "";
      this.ownerId = value.ownerId;
      this.size = 0;
      this.viewCount = 0;
      this.cloneCount = 0;
      this.likeCount = 0;
      this.isLiked = false;
      this.accessibleUserIds = [];
    } else if (isDashboardFile(value)) {
      this.type = EntityType.File;
      this.id = value.file.fid;
      this.name = value.file.name;
      this.description = value.file.description;
      this.creationTime = value.file.uploadTime;
      this.lastModifiedTime = value.file.uploadTime;
      this.accessLevel = value.accessLevel;
      this.ownerName = "";
      this.ownerEmail = value.ownerEmail;
      this.ownerGoogleAvatar = "";
      this.ownerId = value.file.ownerUid;
      this.size = value.file.size;
      this.viewCount = 0;
      this.cloneCount = 0;
      this.likeCount = 0;
      this.isLiked = false;
      this.accessibleUserIds = [];
    } else if (isDashboardDataset(value)) {
      this.type = EntityType.Dataset;
      this.id = value.dataset.did;
      this.name = value.dataset.name;
      this.description = value.dataset.description;
      this.creationTime = value.dataset.creationTime;
      this.lastModifiedTime = value.dataset.creationTime;
      this.accessLevel = value.accessPrivilege;
      this.ownerName = "";
      this.ownerEmail = value.ownerEmail;
      this.ownerGoogleAvatar = "";
      this.ownerId = value.dataset.ownerUid;
      this.size = value.size;
      this.viewCount = 0;
      this.cloneCount = 0;
      this.likeCount = 0;
      this.isLiked = false;
      this.accessibleUserIds = [];
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }

  setOwnerName(ownerName: string): void {
    this.ownerName = ownerName;
  }

  setOwnerGoogleAvatar(ownerGoogleAvatar: string): void {
    this.ownerGoogleAvatar = ownerGoogleAvatar;
  }

  setCount(viewCount: number, cloneCount: number, likeCount: number): void {
    this.viewCount = viewCount;
    this.cloneCount = cloneCount;
    this.likeCount = likeCount;
  }

  setIsLiked(isLiked: boolean): void {
    this.isLiked = isLiked;
  }

  setAccessUsers(accessUsers: number[]): void {
    this.accessibleUserIds = accessUsers;
  }

  setSize(size: number): void {
    this.size = size;
  }

  get project(): DashboardProject {
    if (!isDashboardProject(this.value)) {
      throw new Error("Value is not of type DashboardProject.");
    }
    return this.value;
  }

  get workflow(): DashboardWorkflow {
    if (!isDashboardWorkflow(this.value)) {
      throw new Error("Value is not of type DashboardWorkflow.");
    }
    return this.value;
  }

  get file(): DashboardFile {
    if (!isDashboardFile(this.value)) {
      throw new Error("Value is not of type DashboardFile.");
    }
    return this.value;
  }

  get dataset(): DashboardDataset {
    if (!isDashboardDataset(this.value)) {
      throw new Error("Value is not of type DashboardDataset");
    }
    return this.value;
  }
}
