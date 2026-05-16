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
export const SYNC_EXECUTION_API_BASE_URL = `${AppSettings.getApiEndpoint()}/execution`;

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

  /**
   * Side-by-side comparison summary for two executions of the same workflow.
   */
  compareTwoExecutions(wid: number, eidA: number, eidB: number): Observable<WorkflowExecutionCompareSummary> {
    return this.http.get<WorkflowExecutionCompareSummary>(
      `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/${eidA}/compare/${eidB}`
    );
  }

  /**
   * Fetch a paginated page of rows from a specific operator port's persisted result for a
   * past execution. Powers the per-operator result panel of the workflow-compare view.
   */
  retrieveExecutionResultPage(
    wid: number,
    eid: number,
    opId: string,
    portId: number,
    page: number,
    pageSize: number
  ): Observable<ExecutionOperatorResultPage> {
    const params = new HttpParams().set("page", page.toString()).set("pageSize", pageSize.toString());
    return this.http.get<ExecutionOperatorResultPage>(
      `${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/${eid}/result/${encodeURIComponent(opId)}/${portId}`,
      { params }
    );
  }

  /**
   * Run a historical workflow version end-to-end on the given computing unit and return
   * the new execution's eid. Synchronous: the call blocks server-side until the workflow
   * finishes (success or failure). Used by the compare-versions flow when a selected
   * version has no completed execution to compare against.
   */
  runWorkflowVersion(wid: number, cuid: number, vid: number): Observable<SyncRunVersionResult> {
    return this.http.post<SyncRunVersionResult>(
      `${SYNC_EXECUTION_API_BASE_URL}/${wid}/${cuid}/run-version/${vid}`,
      {}
    );
  }
}

export interface SyncRunVersionResult {
  readonly success: boolean;
  readonly state: string;
  readonly errors?: ReadonlyArray<string>;
  readonly eid: number;
}

export interface CompareAttributeMeta {
  readonly name: string;
  readonly typeName: string;
}

export interface OperatorPortCompareResult {
  readonly operatorId: string;
  readonly portId: number;
  readonly status: "shared" | "onlyInA" | "onlyInB";
  readonly rowCountA: number | null;
  readonly rowCountB: number | null;
  readonly schemaA: ReadonlyArray<CompareAttributeMeta>;
  readonly schemaB: ReadonlyArray<CompareAttributeMeta>;
  readonly schemaMatches: boolean;
}

export interface WorkflowExecutionCompareSummary {
  readonly wid: number;
  readonly eidA: number;
  readonly eidB: number;
  readonly vidA: number;
  readonly vidB: number;
  readonly operators: ReadonlyArray<OperatorPortCompareResult>;
}

export interface ExecutionOperatorResultPage {
  readonly schema: ReadonlyArray<CompareAttributeMeta>;
  readonly rows: ReadonlyArray<Record<string, unknown>>;
  readonly totalRowCount: number;
  readonly pageIndex: number;
  readonly pageSize: number;
}
