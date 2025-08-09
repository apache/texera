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

import { Point } from "../../workspace/types/workflow-common.interface";

/**
 * This interface stores the information about the user account.
 * Such information is used to identify users and to save their data
 * Corresponds to `core/amber/src/main/scala/edu/uci/ics/texera/web/resource/auth/UserResource.scala`
 */

// Please check Role at \core\amber\src\main\scala\edu\uci\ics\texera\web\model\jooq\generated\enums\UserRole.java
export enum Role {
  INACTIVE = "INACTIVE",
  RESTRICTED = "RESTRICTED",
  REGULAR = "REGULAR",
  ADMIN = "ADMIN",
}

export interface User
  extends Readonly<{
    uid: number;
    name: string;
    email: string;
    googleId?: string;
    role: Role;
    color?: string;
    googleAvatar?: string;
    comment: string;
    lastLogin?: number;
  }> {}

export interface File
  extends Readonly<{
    userId: number;
    fileId: number;
    fileName: string;
    fileSize: number;
    uploadedTime: number;
    description: string;
  }> {}

export interface Workflow
  extends Readonly<{
    userId: number;
    workflowId: number;
    workflowName: string;
    creationTime: number;
    lastModifiedTime: number;
  }> {}

export interface WorkflowQuota {
  workflowId: number;
  workflowName: string;
  executions: ExecutionQuota[];
}

export interface ExecutionQuota
  extends Readonly<{
    eid: number;
    workflowId: number;
    workflowName: string;
    resultBytes: number;
    runTimeStatsBytes: number;
    logBytes: number;
  }> {}

/**
 * Coeditor extends User and adds clientId to differentiate local user and collaborative editor
 */
export interface Coeditor extends User {
  clientId: string;
}

/**
 * This interface is for user-presence information in shared-editing.
 */
export interface CoeditorState {
  user: Coeditor;
  isActive: boolean;
  userCursor: Point;
  highlighted?: readonly string[];
  unhighlighted?: readonly string[];
  currentlyEditing?: string;
  changed?: string;
  editingCode?: boolean;
}
