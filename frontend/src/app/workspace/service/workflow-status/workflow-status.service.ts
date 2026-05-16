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
import { Observable, Subject } from "rxjs";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { MacroService } from "../macro/macro.service";

// Macro inner-op IDs are fresh UUIDs (assigned by MacroExpander on the backend)
// — no longer derivable from the macro instance via prefix concat. The
// `MacroService.macroInstanceForRuntimeOp(runtimeOpId)` synchronous lookup
// consults the `/api/workflow/{wid}/macro-mapping` cache to find the
// instance any given runtime op belongs to. This function rolls inner-op
// stats up to the visible macro node so the canvas can show aggregated
// state / row counts during execution.

// State-priority for combining inner-op states into a single macro state.
// Worst-case wins (any failure surfaces; running beats ready; ready beats
// completed). Matches the user's mental model: "the macro is running if any
// inner op is still running."
const STATE_PRIORITY: Record<OperatorState, number> = {
  [OperatorState.Recovering]: 9,
  [OperatorState.Pausing]: 8,
  [OperatorState.Paused]: 7,
  [OperatorState.Resuming]: 6,
  [OperatorState.Running]: 5,
  [OperatorState.Initializing]: 4,
  [OperatorState.Ready]: 3,
  [OperatorState.Completed]: 2,
  [OperatorState.Uninitialized]: 1,
};

function combineStates(states: OperatorState[]): OperatorState {
  if (states.length === 0) return OperatorState.Uninitialized;
  return states.reduce((acc, s) => (STATE_PRIORITY[s] >= STATE_PRIORITY[acc] ? s : acc));
}

/**
 * Group raw per-op stats by macro instance and emit one aggregated entry per
 * macro under the visible instance ID. The original prefixed entries are
 * preserved so the drill-down view can find them.
 *
 * Aggregation rules:
 *  - state: worst-case state across inner ops (see STATE_PRIORITY)
 *  - input/output row counts: sum across inner ops (approximate but useful as
 *    an activity indicator; precise boundary-only counts would need body shape)
 *  - port metrics: not aggregated (macro's port-level metrics are not 1:1 with
 *    inner-op port metrics; leave as empty so the tooltip doesn't show stale)
 *  - numWorkers: sum across inner ops
 */
function withMacroAggregates(
  raw: Record<string, OperatorStatistics>,
  macroService: MacroService
): Record<string, OperatorStatistics> {
  // Each runtime op contributes to the aggregate of EVERY macro instance in
  // its chain (outermost → innermost). A runtime op with chain
  // [outer, inner] gets summed into BOTH `outer`'s aggregate AND `inner`'s
  // aggregate — so the nested macro op visible inside the outer's drill-down
  // view shows its own stats, in addition to the outer macro on the parent
  // canvas.
  const byMacro = new Map<string, OperatorStatistics[]>();
  for (const [runtimeOpId, stats] of Object.entries(raw)) {
    const chain = macroService.macroChainForRuntimeOp(runtimeOpId);
    if (!chain || chain.length === 0) continue;
    for (const macroInstanceId of chain) {
      const list = byMacro.get(macroInstanceId) ?? [];
      list.push(stats);
      byMacro.set(macroInstanceId, list);
    }
  }
  if (byMacro.size === 0) return raw;
  const out: Record<string, OperatorStatistics> = { ...raw };
  for (const [macroInstanceId, innerStats] of byMacro.entries()) {
    // Don't overwrite a real entry that the engine sent for this ID.
    if (out[macroInstanceId] !== undefined) continue;
    out[macroInstanceId] = {
      operatorState: combineStates(innerStats.map(s => s.operatorState)),
      aggregatedInputRowCount: innerStats.reduce((sum, s) => sum + s.aggregatedInputRowCount, 0),
      inputPortMetrics: {},
      aggregatedOutputRowCount: innerStats.reduce((sum, s) => sum + s.aggregatedOutputRowCount, 0),
      outputPortMetrics: {},
      numWorkers: innerStats.reduce((sum, s) => sum + (s.numWorkers ?? 0), 0),
    };
  }
  return out;
}

@Injectable({
  providedIn: "root",
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private statusSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatus: Record<string, OperatorStatistics> = {};

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private macroService: MacroService
  ) {
    this.getStatusUpdateStream().subscribe(event => (this.currentStatus = event));

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== "OperatorStatisticsUpdateEvent") {
        return;
      }
      this.statusSubject.next(withMacroAggregates(event.operatorStatistics, this.macroService));
    });
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.statusSubject.asObservable();
  }

  public getCurrentStatus(): Record<string, OperatorStatistics> {
    return this.currentStatus;
  }

  public resetStatus(): void {
    const initStatus: Record<string, OperatorStatistics> = Object.keys(this.currentStatus).reduce(
      (accumulator, operatorId) => {
        accumulator[operatorId] = {
          operatorState: OperatorState.Uninitialized,
          aggregatedInputRowCount: 0,
          inputPortMetrics: {},
          aggregatedOutputRowCount: 0,
          outputPortMetrics: {},
        };
        return accumulator;
      },
      {} as Record<string, OperatorStatistics>
    );
    this.statusSubject.next(initStatus);
  }

  public clearStatus(): void {
    this.currentStatus = {};
    this.statusSubject.next({});
  }
}
