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

import { USER_WORKSPACE } from "../../../../app-routing.constant";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { DefaultView } from "../../../type/workflow-metadata.interface";

/**
 * Where a dashboard entry lands when opened, and whether it is a form-default workflow. Shared by
 * the list row and the card so the two renderers of the same dashboard agree: with the Form View
 * flag on, a workflow whose default_view is FORM is marked as such and, when its link points into
 * the user's own workspace, deep-links straight into its form (the operator canvas stays one click
 * away from there). Every other link, hub links included, is left exactly as the descriptor built it.
 */
export function defaultsToFormView(entry: DashboardEntry, formViewEnabled: boolean): boolean {
  return formViewEnabled && entry.type === "workflow" && entry.workflow?.workflow?.defaultView === DefaultView.FORM;
}

export function landingLink(entry: DashboardEntry, descriptorLink: string[], formViewEnabled: boolean): string[] {
  if (!formViewEnabled || entry.type !== "workflow" || descriptorLink[0] !== USER_WORKSPACE) {
    return descriptorLink;
  }
  return defaultsToFormView(entry, formViewEnabled)
    ? [USER_WORKSPACE, String(entry.id), "form"]
    : [USER_WORKSPACE, String(entry.id)];
}
