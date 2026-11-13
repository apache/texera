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
import { EMPTY, Observable, defer } from "rxjs";
import { catchError, map, switchMap, takeUntil, tap } from "rxjs/operators";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { EXECUTION_STATUS_CODE, WorkflowExecutionsEntry } from "../../../dashboard/type/workflow-executions-entry";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, isNotInExecution } from "../../types/execute-workflow.interface";
import { loadPersistedHeatmapView } from "./heatmap-overlay-persistence";
import { toOperatorRuntimeStatusMap } from "./runtime-statistics-mapper";

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
   * - an execution is in progress, on entry or by the time the fetches return
   *   (the live stream wins),
   * - the workflow has no executions or the run left no statistics,
   * - another producer writes statistics first (a new run clears the canvas).
   */
  public restoreLatestRunStatistics(): Observable<void> {
    return defer(() => {
      if (loadPersistedHeatmapView() === null) {
        return EMPTY;
      }
      const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
      if (wid === undefined) {
        return EMPTY;
      }
      if (this.isExecuting()) {
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
        // Only the fetches are best-effort. Placed above the map so a mapping or ingestion
        // failure still surfaces instead of looking like a run with nothing to restore.
        catchError(() => EMPTY),
        tap(rows => {
          const runtimeStatus = toOperatorRuntimeStatusMap(rows);
          // Re-checked, not redundant: the entry guard ran two round trips ago, and the
          // websocket can connect and start streaming a live run inside that window.
          if (this.isExecuting() || Object.keys(runtimeStatus).length === 0) {
            return;
          }
          this.workflowStatusService.setExternalStatus(runtimeStatus);
        }),
        map(() => undefined),
        // Any other producer writing statistics means the canvas is no longer ours to restore:
        // pressing Run resets the execution state to Uninitialized, which isExecuting() cannot
        // see until the backend answers, but resetStatus() writes here first. Unsubscribing
        // tears the pending fetch down, so the tap above never runs. A plain Subject, so
        // subscribing does not itself emit. The restore's own write does fire it, from inside
        // the tap, which closes the stream only after that write has reached its subscribers.
        takeUntil(this.workflowStatusService.getStatisticsUpdateStream())
      );
    });
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
    const completed = executions.filter(e => EXECUTION_STATUS_CODE[e.status] === ExecutionState.Completed);
    return latestOf(completed) ?? latestOf(executions);
  }

  private isExecuting(): boolean {
    return !isNotInExecution(this.executeWorkflowService.getExecutionState().state);
  }
}
