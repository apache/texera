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
import { BehaviorSubject, Observable, Subject } from "rxjs";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { OperatorPerformanceMetrics, extractPerformanceMetrics } from "./performance-metrics";

@Injectable({
  providedIn: "root",
})
export class WorkflowStatusService {
  // The engine streams operator state and operator statistics bundled in one
  // wire object (OperatorRuntimeStatus); this service splits them into two
  // separate sub-concepts, each with its own stream and snapshot. Derived
  // performance metrics are the third, separate concept.
  private stateSubject = new Subject<Record<string, OperatorState>>();
  private currentState: Record<string, OperatorState> = {};

  private statisticsSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatistics: Record<string, OperatorStatistics> = {};

  // Derived, ground-truth performance metrics for the heat-map overlay. Backed by
  // a BehaviorSubject so a consumer that subscribes after a run already streamed
  // (e.g. the overlay toggled on mid/post-run) still receives the latest value.
  private performanceMetricsSubject = new BehaviorSubject<Record<string, OperatorPerformanceMetrics>>({});

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.getStateUpdateStream().subscribe(state => {
      this.currentState = state;
    });

    // Single derivation path: every statistics emission (websocket, reset, or
    // clear) recomputes the performance metrics.
    this.getStatisticsUpdateStream().subscribe(statistics => {
      this.currentStatistics = statistics;
      this.performanceMetricsSubject.next(this.buildPerformanceMetrics(statistics));
    });

    // Each wire event produces exactly one emission on each stream, state
    // first and statistics second (resetStatus/clearStatus follow the same
    // order), so a statistics subscriber may read getCurrentState() and see
    // the matching snapshot.
    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== "OperatorStatisticsUpdateEvent") {
        return;
      }
      const state: Record<string, OperatorState> = {};
      const statistics: Record<string, OperatorStatistics> = {};
      for (const [operatorId, update] of Object.entries(event.operatorStatistics)) {
        const { operatorState, ...statisticsOnly } = update;
        state[operatorId] = operatorState;
        statistics[operatorId] = statisticsOnly;
      }
      this.stateSubject.next(state);
      this.statisticsSubject.next(statistics);
    });
  }

  /** Stream of per-operator execution states, keyed by operator id. */
  public getStateUpdateStream(): Observable<Record<string, OperatorState>> {
    return this.stateSubject.asObservable();
  }

  /** Synchronous snapshot of the latest per-operator execution states. */
  public getCurrentState(): Record<string, OperatorState> {
    return this.currentState;
  }

  /** Stream of per-operator statistics (row counts, sizes, timing), keyed by operator id. */
  public getStatisticsUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.statisticsSubject.asObservable();
  }

  /** Synchronous snapshot of the latest per-operator statistics. */
  public getCurrentStatistics(): Record<string, OperatorStatistics> {
    return this.currentStatistics;
  }

  /** Stream of derived per-operator performance metrics, keyed by operator id. */
  public getPerformanceMetricsStream(): Observable<Record<string, OperatorPerformanceMetrics>> {
    return this.performanceMetricsSubject.asObservable();
  }

  /** Synchronous snapshot of the latest derived per-operator performance metrics. */
  public getCurrentPerformanceMetrics(): Record<string, OperatorPerformanceMetrics> {
    return this.performanceMetricsSubject.getValue();
  }

  private buildPerformanceMetrics(
    statistics: Record<string, OperatorStatistics>
  ): Record<string, OperatorPerformanceMetrics> {
    const metrics: Record<string, OperatorPerformanceMetrics> = {};
    for (const operatorId of Object.keys(statistics)) {
      metrics[operatorId] = extractPerformanceMetrics(statistics[operatorId]);
    }
    return metrics;
  }

  public resetStatus(): void {
    const initState: Record<string, OperatorState> = {};
    for (const operatorId of Object.keys(this.currentState)) {
      initState[operatorId] = OperatorState.Uninitialized;
    }
    const initStatistics: Record<string, OperatorStatistics> = {};
    for (const operatorId of Object.keys(this.currentStatistics)) {
      initStatistics[operatorId] = {
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        aggregatedOutputRowCount: 0,
        outputPortMetrics: {},
      };
    }
    this.stateSubject.next(initState);
    this.statisticsSubject.next(initStatistics);
  }

  public clearStatus(): void {
    this.stateSubject.next({});
    this.statisticsSubject.next({});
  }
}
