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

// Type-only: this module is imported all over the app and must not pull the router in at runtime.
import type { Router } from "@angular/router";

export const HOME = "/home";
export const ABOUT = "/about";
export const LOGIN = "/login";

export const HUB = "/hub";
export const HUB_WORKFLOW = `${HUB}/workflow`;
export const HUB_WORKFLOW_RESULT = `${HUB_WORKFLOW}/result`;
export const HUB_WORKFLOW_RESULT_DETAIL = `${HUB_WORKFLOW_RESULT}/detail`;
export const HUB_DATASET = `${HUB}/dataset`;
export const HUB_DATASET_RESULT = `${HUB_DATASET}/result`;
export const HUB_DATASET_RESULT_DETAIL = `${HUB_DATASET_RESULT}/detail`;
export const HUB_MODEL = `${HUB}/model`;
export const HUB_MODEL_RESULT = `${HUB_MODEL}/result`;
export const HUB_MODEL_RESULT_DETAIL = `${HUB_MODEL_RESULT}/detail`;

export const USER = "/user";
export const USER_WORKSPACE = `${USER}/workflow`;
export const USER_WORKFLOW = `${USER}/workflow`;

/** One workflow is open in the workspace under two views: the operator canvas and the Form View. */
export const workspaceCanvasUrl = (wid: number): string => `${USER_WORKSPACE}/${wid}`;
export const workspaceFormUrl = (wid: number): string => `${workspaceCanvasUrl(wid)}/form`;

/**
 * Whether `url` is one of the two views workflow `wid` is open under. A falsy `wid` is no
 * workflow: `DEFAULT_WORKFLOW` carries 0 until the first save gives it an id.
 */
function isWorkspaceViewOf(url: string, wid: number | undefined): boolean {
  if (!wid) {
    return false;
  }
  const path = url.split(/[?#]/)[0];
  return path === workspaceCanvasUrl(wid) || path === workspaceFormUrl(wid);
}

/**
 * Whether the navigation now in flight is leaving workflow `wid`, rather than moving between the
 * two views it is open under.
 *
 * Both views ask this as they are destroyed, and both must answer it the same way: the session
 * below them -- the shared document and the co-editing room it holds, the computing unit
 * connection, the execution state -- belongs to the workflow, not to either view. Moving between
 * the views hands it over; leaving the pair drops it. No navigation in flight means the view is
 * being destroyed for some reason other than routing, and so has no successor to hand to, which
 * counts as leaving. Unloading the page is not one of those: since #8600 neither view tears down
 * on `beforeunload` at all, precisely so a document restored from the back/forward cache still
 * has the session it was left with.
 *
 * `wid` is the workflow the caller is actually holding open, not the one in its route: a workflow
 * created by the first autosave has no id in the route it was opened with.
 */
export function isLeavingWorkspace(router: Router, wid: number | undefined): boolean {
  const target = router.getCurrentNavigation()?.finalUrl;
  return !target || !isWorkspaceViewOf(router.serializeUrl(target), wid);
}

export const USER_DATASET = `${USER}/dataset`;
export const USER_DATASET_CREATE = `${USER_DATASET}/create`;
export const USER_MODEL = `${USER}/model`;
export const USER_COMPUTING_UNIT = `${USER}/compute`;
export const USER_WAREHOUSE = `${USER}/warehouse`;
export const USER_PYTHON_VENV = `${USER}/python-venv`;
export const USER_QUOTA = `${USER}/quota`;
export const USER_DISCUSSION = `${USER}/discussion`;
export const USER_FEEDBACK = `${USER}/feedback`;

export const ADMIN = "/admin";
export const ADMIN_USER = `${ADMIN}/user`;
export const ADMIN_GMAIL = `${ADMIN}/gmail`;
export const ADMIN_EXECUTION = `${ADMIN}/execution`;
export const ADMIN_SETTINGS = `${ADMIN}/settings`;
export const ADMIN_CU_IMAGE = `${ADMIN}/cu-image`;

export const SEARCH = "/search";
