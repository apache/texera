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
import { EMPTY, Observable, of } from "rxjs";
import { catchError, map, switchMap } from "rxjs/operators";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { isNotInExecution } from "../../types/execute-workflow.interface";
import { loadPersistedHeatmapView } from "./heatmap-overlay-persistence";
import { toOperatorRuntimeStatusMap } from "./runtime-statistics-mapper";

/** Persisted execution status code for a completed run (EXECUTION_STATUS_CODE). */
const EXECUTION_COMPLETED_CODE = 3;

/**
 * Restores the last execution's per-operator statistics after a page refresh,
 * so the performance heat-map can render a finished run without re-executing.
 *
 * All gating lives here rather than in the caller: the persisted-overlay check
 * is a synchronous localStorage read, so a user who never enabled the overlay
 * incurs no fetch at all.
 */
@Injectable({
  providedIn: "root",
})
export class HeatmapStatsRestoreService {
  constructor(
    private workflowExecutionsService: WorkflowExecutionsService,
    private workflowActionService: WorkflowActionService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {}

  /**
   * Fetches the latest run's statistics and feeds them into
   * WorkflowStatusService. Cold: nothing happens until subscribed. Skips
   * silently (including on HTTP errors — restoring is best-effort) when:
   * - the overlay is not persisted on,
   * - the workflow has never been saved (no wid),
   * - an execution is in progress (the live stream wins),
   * - the workflow has no executions or the run left no statistics.
   */
  public restoreLatestRunStatistics(): Observable<void> {
    if (loadPersistedHeatmapView() === null) {
      return EMPTY;
    }
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (wid === undefined) {
      return EMPTY;
    }
    if (!isNotInExecution(this.executeWorkflowService.getExecutionState().state)) {
      return EMPTY;
    }

    return this.workflowExecutionsService.retrieveWorkflowExecutions(wid).pipe(
      switchMap(executions => {
        const run = this.pickLatestRun(executions);
        if (run === undefined) {
          return EMPTY;
        }
        return this.workflowExecutionsService.retrieveWorkflowRuntimeStatistics(wid, run.eId, run.cuId);
      }),
      map(rows => {
        const runtimeStatus = toOperatorRuntimeStatusMap(rows);
        if (Object.keys(runtimeStatus).length > 0) {
          this.workflowStatusService.setExternalStatus(runtimeStatus);
        }
      }),
      catchError(() => EMPTY)
    );
  }

  /**
   * The run to restore: the most recent completed execution, or — when no run
   * ever completed — the most recent one overall, so a partially executed
   * workflow still shows the statistics it produced.
   */
  private pickLatestRun(executions: ReadonlyArray<WorkflowExecutionsEntry>): WorkflowExecutionsEntry | undefined {
    const latestOf = (entries: ReadonlyArray<WorkflowExecutionsEntry>) =>
      entries.length === 0
        ? undefined
        : entries.reduce((latest, entry) => (entry.startingTime > latest.startingTime ? entry : latest));
    return latestOf(executions.filter(e => e.status === EXECUTION_COMPLETED_CODE)) ?? latestOf(executions);
  }
}
