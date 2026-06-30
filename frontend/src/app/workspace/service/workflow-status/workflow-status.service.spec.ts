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
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { TexeraWebsocketEvent } from "../../types/workflow-websocket.interface";

const sampleStats: OperatorStatistics = {
  operatorState: OperatorState.Running,
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

function statsEvent(operatorStatistics: Record<string, OperatorStatistics>): TexeraWebsocketEvent {
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

  it("forwards an OperatorStatisticsUpdateEvent to the status stream", () => {
    const received: Record<string, OperatorStatistics>[] = [];
    service.getStatusUpdateStream().subscribe(s => received.push(s));

    websocketEventSubject.next(statsEvent({ op1: sampleStats }));

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({ op1: sampleStats });
    expect(service.getCurrentStatus()).toEqual({ op1: sampleStats });
  });

  it("ignores websocket events of other types", () => {
    const received: Record<string, OperatorStatistics>[] = [];
    service.getStatusUpdateStream().subscribe(s => received.push(s));

    websocketEventSubject.next({ type: "WorkflowErrorEvent" } as unknown as TexeraWebsocketEvent);

    expect(received).toHaveLength(0);
    expect(service.getCurrentStatus()).toEqual({});
  });

  it("derives performance metrics from a status update", () => {
    const emissions: Record<string, OperatorPerformanceMetrics>[] = [];
    service.getPerformanceMetricsStream().subscribe(m => emissions.push(m));

    websocketEventSubject.next(statsEvent({ op1: sampleStats }));

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
    websocketEventSubject.next(statsEvent({ [id]: sampleStats }));
    expect(Object.keys(service.getCurrentPerformanceMetrics())).toEqual([id]);
  });

  it("seeds the performance-metrics stream with an empty map for late subscribers", () => {
    expect(service.getCurrentPerformanceMetrics()).toEqual({});
    let seeded: Record<string, OperatorPerformanceMetrics> | undefined;
    service.getPerformanceMetricsStream().subscribe(m => (seeded = m));
    expect(seeded).toEqual({});
  });

  it("defaults missing optional fields to 0 when deriving metrics", () => {
    const partial: OperatorStatistics = {
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

  it("resetStatus zeros the metrics for known operators", () => {
    websocketEventSubject.next(statsEvent({ op1: sampleStats }));
    service.resetStatus();

    const m = service.getCurrentPerformanceMetrics()["op1"];
    expect(m).toEqual({
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

  it("clearStatus empties both the status and performance-metrics snapshots", () => {
    websocketEventSubject.next(statsEvent({ op1: sampleStats }));
    service.clearStatus();

    expect(service.getCurrentStatus()).toEqual({});
    expect(service.getCurrentPerformanceMetrics()).toEqual({});
  });
});
