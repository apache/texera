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
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import { HttpClient, HttpParams } from "@angular/common/http";
import { WorkflowExecutionsEntry } from "../../../type/workflow-executions-entry";
import { WorkflowRuntimeStatistics } from "../../../type/workflow-runtime-statistics";
import { ExecutionState } from "../../../../workspace/types/execute-workflow.interface";

export const WORKFLOW_EXECUTIONS_API_BASE_URL = `${AppSettings.getApiEndpoint()}/executions`;

@Injectable({
  providedIn: "root",
})
export class WorkflowExecutionsService {
  constructor(private http: HttpClient) {}

  /**
   * Retrieves the latest execution entry (latest VID, latest start-time)
   * for the given workflow ID.
   */
  retrieveLatestWorkflowExecution(wid: number): Observable<WorkflowExecutionsEntry> {
    return this.http.get<WorkflowExecutionsEntry>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/latest`);
  }

  /**
   * retrieves a list of executions for a particular workflow from the back-end
   * database.
   *
   * @param wid       workflow ID
   * @param statuses  optional list of status strings
   *                  (e.g. ["running", "completed"]).  If the array is empty or
   *                  omitted, no status filter is applied.
   */
  retrieveWorkflowExecutions(wid: number, statuses?: ExecutionState[]): Observable<WorkflowExecutionsEntry[]> {
    /* -------------------------------------------------------------------- */
    /* build query-string ?status=running,completed …                        */
    /* -------------------------------------------------------------------- */
    let params = new HttpParams();
    if (statuses && statuses.length > 0) {
      params = params.set("status", statuses.join(","));
    }

    return this.http.get<WorkflowExecutionsEntry[]>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}`, { params });
  }

  groupSetIsBookmarked(wid: number, eIds: number[], isBookmarked: boolean): Observable<Object> {
    return this.http.put(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/set_execution_bookmarks`, {
      wid,
      eIds,
      isBookmarked,
    });
  }

  groupDeleteWorkflowExecutions(wid: number, eIds: number[]): Observable<Object> {
    return this.http.put(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/delete_executions`, {
      wid,
      eIds,
    });
  }

  updateWorkflowExecutionsName(wid: number | undefined, eId: number, executionName: string): Observable<Response> {
    return this.http.post<Response>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/update_execution_name`, {
      wid,
      eId,
      executionName,
    });
  }

  retrieveWorkflowRuntimeStatistics(wid: number, eId: number, cuid: number): Observable<WorkflowRuntimeStatistics[]> {
    const params = new HttpParams().set("cuid", cuid.toString());
    return this.http.get<WorkflowRuntimeStatistics[]>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/stats/${eId}`, {
      params,
    });
  }
}
