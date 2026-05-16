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
import { Observable, defer, of, shareReplay, map, catchError } from "rxjs";
import {
  EXECUTION_STATUS_CODE,
  WorkflowExecutionsEntry,
} from "../../../dashboard/type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { BaselineReport } from "./profiler-delta";
import {
  WorkflowRuntimeStatsRow,
  convertStatsRowsToBaseline,
} from "./profiler-history";

/**
 * P6 (compare across runs): turns the persisted per-execution stats already
 * exposed by `/executions/{wid}` + `/executions/{wid}/stats/{eid}` into the
 * existing `BaselineReport` shape so the side-panel delta UI works
 * end-to-end with zero new rendering code.
 *
 * Caching policy: we memoize per (wid, eid) because the same baseline tends to
 * be re-selected as users compare against different historical runs. Cache
 * never invalidates within a session — historical stats are immutable once an
 * execution completes.
 */
@Injectable({ providedIn: "root" })
export class ProfilerHistoryService {
  private readonly baselineCache = new Map<string, Observable<BaselineReport | undefined>>();

  constructor(private readonly workflowExecutionsService: WorkflowExecutionsService) {}

  /**
   * List the historical executions of a workflow that have completed and
   * therefore have a baseline worth comparing against. Filters out in-flight
   * / failed runs since their stats are incomplete or absent.
   */
  public listCompletedExecutions(workflowId: number): Observable<WorkflowExecutionsEntry[]> {
    return this.workflowExecutionsService
      .retrieveWorkflowExecutions(workflowId, [ExecutionState.Completed])
      .pipe(
        map(rows =>
          rows.filter(r => EXECUTION_STATUS_CODE[r.status] === ExecutionState.Completed)
        ),
        catchError(() => of([] as WorkflowExecutionsEntry[]))
      );
  }

  /**
   * Fetch the runtime stats for the given execution and convert them into a
   * `BaselineReport` ready to feed into `ProfilerService.setBaseline`. Returns
   * `undefined` on a network failure or when the persisted stats yield zero
   * valid operators — callers should fall back to no-baseline state.
   *
   * `cuid` is forwarded to the backend's query string for API-shape parity but
   * the current server impl ignores it (the URI lookup is keyed solely on eId).
   */
  public loadBaselineForExecution(input: {
    workflowId: number;
    execution: WorkflowExecutionsEntry;
    workflowName: string;
  }): Observable<BaselineReport | undefined> {
    const key = `${input.workflowId}::${input.execution.eId}`;
    const cached = this.baselineCache.get(key);
    if (cached) return cached;

    const stream = defer(() =>
      this.workflowExecutionsService.retrieveWorkflowRuntimeStatistics(
        input.workflowId,
        input.execution.eId,
        input.execution.cuId
      )
    ).pipe(
      map(rows => {
        const rowsAsRecords = rows as unknown as WorkflowRuntimeStatsRow[];
        return convertStatsRowsToBaseline({
          rows: rowsAsRecords,
          workflowName: input.workflowName,
          executionName: input.execution.name || `Execution #${input.execution.eId}`,
          generatedAt: completionTimestampToIso(input.execution.completionTime),
        });
      }),
      catchError(() => of(undefined)),
      shareReplay(1)
    );
    this.baselineCache.set(key, stream);
    return stream;
  }

  /** Clears all cached baselines. Mainly for tests + workflow switches. */
  public clearCache(): void {
    this.baselineCache.clear();
  }
}

function completionTimestampToIso(ts: number | undefined): string {
  if (typeof ts !== "number" || !Number.isFinite(ts) || ts <= 0) {
    return new Date().toISOString();
  }
  return new Date(ts).toISOString();
}
