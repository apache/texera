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

import { TestBed } from "@angular/core/testing";
import { Subject } from "rxjs";
import { WorkflowStatusService } from "./workflow-status.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { OperatorPerformanceMetrics } from "./performance-metrics";
import { OperatorRuntimeStatus, OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { TexeraWebsocketEvent } from "../../types/workflow-websocket.interface";

const sampleStatistics: OperatorStatistics = {
  aggregatedInputRowCount: 1_000,
  aggregatedInputSize: 8_000,
  inputPortMetrics: { "0": 1_000 },
  aggregatedOutputRowCount: 250,
  aggregatedOutputSize: 2_000,
  outputPortMetrics: { "0": 250 },
  numWorkers: 2,
  aggregatedDataProcessingTime: 5_000_000,
  aggregatedControlProcessingTime: 1_000_000,
  aggregatedIdleTime: 700_000,
};

// The wire object the engine streams: state and statistics bundled together.
const sampleRuntimeStatus: OperatorRuntimeStatus = {
  operatorState: OperatorState.Running,
  ...sampleStatistics,
};

function statsEvent(operatorStatistics: Record<string, OperatorRuntimeStatus>): TexeraWebsocketEvent {
  return { type: "OperatorStatisticsUpdateEvent", operatorStatistics } as TexeraWebsocketEvent;
}

describe("WorkflowStatusService", () => {
  let service: WorkflowStatusService;
  let websocketEventSubject: Subject<TexeraWebsocketEvent>;

  beforeEach(() => {
    websocketEventSubject = new Subject<TexeraWebsocketEvent>();
    const websocketStub: Partial<WorkflowWebsocketService> = {
      websocketEvent: () => websocketEventSubject.asObservable(),
    };
    TestBed.configureTestingModule({
      providers: [WorkflowStatusService, { provide: WorkflowWebsocketService, useValue: websocketStub }],
    });
    service = TestBed.inject(WorkflowStatusService);
  });

  it("splits an OperatorStatisticsUpdateEvent into the state and statistics streams", () => {
    const stateEmissions: Record<string, OperatorState>[] = [];
    const statisticsEmissions: Record<string, OperatorStatistics>[] = [];
    service.getStateUpdateStream().subscribe(s => stateEmissions.push(s));
    service.getStatisticsUpdateStream().subscribe(s => statisticsEmissions.push(s));

    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));

    expect(stateEmissions).toHaveLength(1);
    expect(stateEmissions[0]).toEqual({ op1: OperatorState.Running });
    expect(statisticsEmissions).toHaveLength(1);
    expect(statisticsEmissions[0]).toEqual({ op1: sampleStatistics });
    expect(service.getCurrentState()).toEqual({ op1: OperatorState.Running });
    expect(service.getCurrentStatistics()).toEqual({ op1: sampleStatistics });
  });

  it("does not leak the operator state into the statistics concept", () => {
    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));
    expect(service.getCurrentStatistics()["op1"]).not.toHaveProperty("operatorState");
  });

  it("exposes state and statistics as independent streams", () => {
    // A consumer subscribed to only one of the two sub-concepts sees exactly
    // one emission per update, unaffected by the other stream.
    let stateCount = 0;
    let statisticsCount = 0;
    service.getStateUpdateStream().subscribe(() => stateCount++);
    service.getStatisticsUpdateStream().subscribe(() => statisticsCount++);

    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));
    websocketEventSubject.next(statsEvent({ op1: { ...sampleRuntimeStatus, operatorState: OperatorState.Paused } }));

    expect(stateCount).toBe(2);
    expect(statisticsCount).toBe(2);
    expect(service.getCurrentState()).toEqual({ op1: OperatorState.Paused });
  });

  it("emits state before statistics, so a statistics subscriber sees the matching state snapshot", () => {
    const order: string[] = [];
    // Captured in the subscriber, asserted after next() returns: rxjs re-throws
    // a subscriber's error asynchronously, so an expect() inside the callback
    // could not fail this test.
    let stateSeenByStatisticsSubscriber: Record<string, OperatorState> | undefined;
    service.getStateUpdateStream().subscribe(() => order.push("state"));
    service.getStatisticsUpdateStream().subscribe(() => {
      order.push("statistics");
      stateSeenByStatisticsSubscriber = service.getCurrentState();
    });

    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));

    expect(order).toEqual(["state", "statistics"]);
    // The licensed read: by the time statistics arrive, the state snapshot
    // already reflects the same wire event. The converse does not hold.
    expect(stateSeenByStatisticsSubscriber).toEqual({ op1: OperatorState.Running });
  });

  it("ignores websocket events of other types", () => {
    const stateEmissions: Record<string, OperatorState>[] = [];
    const statisticsEmissions: Record<string, OperatorStatistics>[] = [];
    service.getStateUpdateStream().subscribe(s => stateEmissions.push(s));
    service.getStatisticsUpdateStream().subscribe(s => statisticsEmissions.push(s));

    websocketEventSubject.next({ type: "WorkflowErrorEvent" } as unknown as TexeraWebsocketEvent);

    expect(stateEmissions).toHaveLength(0);
    expect(statisticsEmissions).toHaveLength(0);
    expect(service.getCurrentState()).toEqual({});
    expect(service.getCurrentStatistics()).toEqual({});
  });

  it("derives performance metrics from a statistics update", () => {
    const emissions: Record<string, OperatorPerformanceMetrics>[] = [];
    service.getPerformanceMetricsStream().subscribe(m => emissions.push(m));

    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));

    // BehaviorSubject seeds {} then emits the derived map.
    const latest = emissions[emissions.length - 1];
    expect(latest["op1"]).toEqual({
      dataProcessingTimeNs: 5_000_000,
      controlProcessingTimeNs: 1_000_000,
      idleTimeNs: 700_000,
      inputRows: 1_000,
      outputRows: 250,
      inputSize: 8_000,
      outputSize: 2_000,
      numWorkers: 2,
    });
    expect(service.getCurrentPerformanceMetrics()).toEqual(latest);
  });

  it("keys the derived metrics by operator id, including unicode ids", () => {
    const id = "算子-✓-1";
    websocketEventSubject.next(statsEvent({ [id]: sampleRuntimeStatus }));
    expect(Object.keys(service.getCurrentPerformanceMetrics())).toEqual([id]);
  });

  it("seeds the performance-metrics stream with an empty map for late subscribers", () => {
    expect(service.getCurrentPerformanceMetrics()).toEqual({});
    let seeded: Record<string, OperatorPerformanceMetrics> | undefined;
    service.getPerformanceMetricsStream().subscribe(m => (seeded = m));
    expect(seeded).toEqual({});
  });

  it("defaults missing optional fields to 0 when deriving metrics", () => {
    const partial: OperatorRuntimeStatus = {
      operatorState: OperatorState.Uninitialized,
      aggregatedInputRowCount: 0,
      inputPortMetrics: {},
      aggregatedOutputRowCount: 0,
      outputPortMetrics: {},
    };
    websocketEventSubject.next(statsEvent({ op1: partial }));

    const m = service.getCurrentPerformanceMetrics()["op1"];
    expect(m.dataProcessingTimeNs).toBe(0);
    expect(m.controlProcessingTimeNs).toBe(0);
    expect(m.idleTimeNs).toBe(0);
    expect(m.inputSize).toBe(0);
    // an operator always runs on at least one worker
    expect(m.numWorkers).toBe(1);
  });

  it("resetStatus resets both concepts for known operators", () => {
    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));
    service.resetStatus();

    expect(service.getCurrentState()).toEqual({ op1: OperatorState.Uninitialized });
    expect(service.getCurrentStatistics()).toEqual({
      op1: {
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        aggregatedOutputRowCount: 0,
        outputPortMetrics: {},
      },
    });
    expect(service.getCurrentPerformanceMetrics()["op1"]).toEqual({
      dataProcessingTimeNs: 0,
      controlProcessingTimeNs: 0,
      idleTimeNs: 0,
      inputRows: 0,
      outputRows: 0,
      inputSize: 0,
      outputSize: 0,
      numWorkers: 1,
    });
  });

  it("clearStatus empties the state, statistics, and performance-metrics snapshots", () => {
    websocketEventSubject.next(statsEvent({ op1: sampleRuntimeStatus }));
    service.clearStatus();

    expect(service.getCurrentState()).toEqual({});
    expect(service.getCurrentStatistics()).toEqual({});
    expect(service.getCurrentPerformanceMetrics()).toEqual({});
  });
});
