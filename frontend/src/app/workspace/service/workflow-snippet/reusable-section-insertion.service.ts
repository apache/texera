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
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowSnippet } from "../../types/workflow-snippet.interface";
import { WorkflowSnippetService } from "./workflow-snippet.service";

/** Shared UI insertion action for both the library and its preview dialog. */
@Injectable({ providedIn: "root" })
export class ReusableSectionInsertionService {
  constructor(
    private readonly actions: WorkflowActionService,
    private readonly snippets: WorkflowSnippetService,
    private readonly notifications: NotificationService
  ) {}

  insert(section: WorkflowSnippet): boolean {
    try {
      const translation = this.actions.getJointGraphWrapper().getMainJointPaper()?.translate();
      this.snippets.insert(section.id, { x: 400 - (translation?.tx ?? 0), y: 200 - (translation?.ty ?? 0) });
      this.notifications.success(`Reusable section box "${section.name}" inserted as an independent copy.`);
      return true;
    } catch (error) {
      this.notifications.error(error instanceof Error ? error.message : "Unable to insert the reusable section.");
      return false;
    }
  }
}
