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

// Macro inner-op IDs carry a "${macroInstanceId}--..." prefix after MacroExpander
// runs on the backend. The engine reports stats keyed by those expanded IDs, but
// the outer canvas only has the macro instance itself — so we synthesize one
// aggregated entry per macro under the visible instance ID so the macro node can
// show a state and tuple counts during execution. The original prefixed entries
// stay in the map for the drill-down view (it maps "${instance}--${innerId}"
// back to "${innerId}" when displaying the body).
const MACRO_INNER_SEPARATOR = "--";

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
  raw: Record<string, OperatorStatistics>
): Record<string, OperatorStatistics> {
  const byMacro = new Map<string, OperatorStatistics[]>();
  for (const [opId, stats] of Object.entries(raw)) {
    const sep = opId.indexOf(MACRO_INNER_SEPARATOR);
    if (sep < 0) continue;
    const macroId = opId.substring(0, sep);
    const list = byMacro.get(macroId) ?? [];
    list.push(stats);
    byMacro.set(macroId, list);
  }
  if (byMacro.size === 0) return raw;
  const out: Record<string, OperatorStatistics> = { ...raw };
  for (const [macroId, innerStats] of byMacro.entries()) {
    // Don't overwrite a real entry that the engine sent for this ID (defensive
    // — engine should never emit both, but if it does the real one wins).
    if (out[macroId] !== undefined) continue;
    out[macroId] = {
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

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.getStatusUpdateStream().subscribe(event => (this.currentStatus = event));

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== "OperatorStatisticsUpdateEvent") {
        return;
      }
      this.statusSubject.next(withMacroAggregates(event.operatorStatistics));
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
