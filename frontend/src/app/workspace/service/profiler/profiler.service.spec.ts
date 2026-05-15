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

import "zone.js/testing";

import { fakeAsync, tick } from "@angular/core/testing";
import { BehaviorSubject, Subject } from "rxjs";

import { ProfilerService, ProfilerState } from "./profiler.service";
import { ProfilerConfig, profilerConfigEquals } from "./profiler-config";
import {
  ExecutionState,
  ExecutionStateInfo,
  OperatorState,
  OperatorStatistics,
} from "../../types/execute-workflow.interface";

class StubWorkflowStatusService {
  public statusSubject = new Subject<Record<string, OperatorStatistics>>();
  public currentStatus: Record<string, OperatorStatistics> = {};
  public getStatusUpdateStream() {
    return this.statusSubject.asObservable();
  }
  public getCurrentStatus() {
    return this.currentStatus;
  }
}

class StubExecuteWorkflowService {
  public executionStateStream = new Subject<{ previous: ExecutionStateInfo; current: ExecutionStateInfo }>();
  public getExecutionStateStream() {
    return this.executionStateStream.asObservable();
  }
}

class StubWorkflowActionService {
  public profilerConfig: ProfilerConfig | undefined = undefined;
  public profilerConfigSubject = new BehaviorSubject<ProfilerConfig | undefined>(undefined);
  public writeCalls: (ProfilerConfig | undefined)[] = [];
  public getProfilerConfigStream() {
    return this.profilerConfigSubject.asObservable();
  }
  public setProfilerConfig(cfg: ProfilerConfig | undefined): void {
    this.writeCalls.push(cfg);
    if (profilerConfigEquals(this.profilerConfig, cfg)) return;
    this.profilerConfig = cfg;
    this.profilerConfigSubject.next(cfg);
  }
}

function stat(partial: Partial<OperatorStatistics>): OperatorStatistics {
  return {
    operatorState: OperatorState.Completed,
    aggregatedInputRowCount: 0,
    inputPortMetrics: {},
    aggregatedOutputRowCount: 0,
    outputPortMetrics: {},
    ...partial,
  };
}

function installLocalStorageMock(): void {
  const store: Record<string, string> = {};
  const mock: Storage = {
    get length() {
      return Object.keys(store).length;
    },
    clear: () => {
      for (const k of Object.keys(store)) delete store[k];
    },
    getItem: (k: string) => (k in store ? store[k] : null),
    key: (i: number) => Object.keys(store)[i] ?? null,
    removeItem: (k: string) => {
      delete store[k];
    },
    setItem: (k: string, v: string) => {
      store[k] = String(v);
    },
  };
  Object.defineProperty(globalThis, "localStorage", {
    value: mock,
    configurable: true,
    writable: true,
  });
}

describe("ProfilerService", () => {
  let workflowStatus: StubWorkflowStatusService;
  let executeWorkflow: StubExecuteWorkflowService;
  let workflowAction: StubWorkflowActionService;
  let profiler: ProfilerService;

  beforeEach(() => {
    installLocalStorageMock();
    workflowStatus = new StubWorkflowStatusService();
    executeWorkflow = new StubExecuteWorkflowService();
    workflowAction = new StubWorkflowActionService();
    profiler = new ProfilerService(workflowStatus as any, executeWorkflow as any, workflowAction as any);
  });

  describe("computeScores (pure)", () => {
    it("returns empty object for empty stats map", () => {
      expect(profiler.computeScores({}, "runtime")).toEqual({});
    });

    it("assigns score 1.0 to the only operator when it has cost", () => {
      const scores = profiler.computeScores(
        { op1: stat({ aggregatedDataProcessingTime: 1000 }) },
        "runtime"
      );
      expect(scores["op1"].score).toBe(1);
    });

    it("normalizes runtime: hottest op gets 1.0, others scaled proportionally", () => {
      const scores = profiler.computeScores(
        {
          scan: stat({ aggregatedDataProcessingTime: 200 }),
          filter: stat({ aggregatedDataProcessingTime: 80 }),
          join: stat({ aggregatedDataProcessingTime: 2000 }),
          viz: stat({ aggregatedDataProcessingTime: 150 }),
        },
        "runtime"
      );
      expect(scores["join"].score).toBe(1);
      expect(scores["scan"].score).toBeCloseTo(0.1, 5);
      expect(scores["filter"].score).toBeCloseTo(0.04, 5);
      expect(scores["viz"].score).toBeCloseTo(0.075, 5);
    });

    it("returns score 0 for all when all runtimes are zero", () => {
      const scores = profiler.computeScores(
        {
          a: stat({ aggregatedDataProcessingTime: 0 }),
          b: stat({ aggregatedDataProcessingTime: 0 }),
        },
        "runtime"
      );
      expect(scores["a"].score).toBe(0);
      expect(scores["b"].score).toBe(0);
    });

    it("guards against NaN / undefined runtime fields", () => {
      const scores = profiler.computeScores(
        {
          a: stat({}), // no runtime field at all
          b: stat({ aggregatedDataProcessingTime: 1000 }),
        },
        "runtime"
      );
      expect(scores["a"].score).toBe(0);
      expect(scores["b"].score).toBe(1);
      expect(Number.isFinite(scores["a"].score)).toBe(true);
    });

    it("io-imbalance: a filter that drops most input is hot", () => {
      const scores = profiler.computeScores(
        {
          scan: stat({ aggregatedInputRowCount: 0, aggregatedOutputRowCount: 1000 }),
          filter: stat({ aggregatedInputRowCount: 1000, aggregatedOutputRowCount: 10 }),
          pass: stat({ aggregatedInputRowCount: 10, aggregatedOutputRowCount: 10 }),
        },
        "io-imbalance"
      );
      expect(scores["filter"].score).toBe(1);
      expect(scores["pass"].score).toBe(0);
      expect(scores["scan"].score).toBe(0);
    });

    it("preserves the operator state on each entry", () => {
      const scores = profiler.computeScores(
        { op1: stat({ operatorState: OperatorState.Running, aggregatedDataProcessingTime: 1 }) },
        "runtime"
      );
      expect(scores["op1"].state).toBe(OperatorState.Running);
    });
  });

  describe("enabled toggle", () => {
    it("starts disabled and emits empty scores when stats arrive", fakeAsync(() => {
      const emissions: ProfilerState[] = [];
      profiler.getStateStream().subscribe(s => emissions.push(s));
      workflowStatus.statusSubject.next({ op1: stat({ aggregatedDataProcessingTime: 100 }) });
      tick(600);
      const last = emissions[emissions.length - 1];
      expect(last.enabled).toBe(false);
      expect(last.scores).toEqual({});
    }));

    it("computes scores immediately when toggled on with current stats", () => {
      workflowStatus.currentStatus = { op1: stat({ aggregatedDataProcessingTime: 500 }) };
      profiler.setEnabled(true);
      const state = profiler.getState();
      expect(state.enabled).toBe(true);
      expect(state.scores["op1"].score).toBe(1);
    });

    it("clears scores immediately when toggled off (does not wait for next stats event)", () => {
      workflowStatus.currentStatus = { op1: stat({ aggregatedDataProcessingTime: 500 }) };
      profiler.setEnabled(true);
      expect(Object.keys(profiler.getState().scores).length).toBe(1);
      profiler.setEnabled(false);
      // Synchronous: no stats event needed to clear.
      expect(profiler.getState().scores).toEqual({});
      expect(profiler.getState().enabled).toBe(false);
    });
  });

  describe("run lifecycle reset", () => {
    it("clears scores on transition to Initializing", () => {
      workflowStatus.currentStatus = { op1: stat({ aggregatedDataProcessingTime: 100 }) };
      profiler.setEnabled(true);
      expect(Object.keys(profiler.getState().scores).length).toBe(1);
      executeWorkflow.executionStateStream.next({
        previous: { state: ExecutionState.Completed },
        current: { state: ExecutionState.Initializing },
      });
      expect(profiler.getState().scores).toEqual({});
    });
  });

  describe("config persistence", () => {
    it("persists view selection to localStorage", () => {
      profiler.setView("throughput");
      const raw = localStorage.getItem("texera.profiler.state");
      expect(raw).toBeTruthy();
      expect(JSON.parse(raw!).view).toBe("throughput");
    });

    it("clamps hot-threshold percentile to [0,100]", () => {
      profiler.setHotThresholdPercentile(150);
      expect(profiler.getState().hotThresholdPercentile).toBe(100);
      profiler.setHotThresholdPercentile(-5);
      expect(profiler.getState().hotThresholdPercentile).toBe(0);
    });

    it("ignores corrupt persisted view value and falls back to default", () => {
      localStorage.setItem(
        "texera.profiler.state",
        JSON.stringify({ enabled: true, view: "totally-bogus", hotThresholdPercentile: 50 })
      );
      const fresh = new ProfilerService(workflowStatus as any, executeWorkflow as any, workflowAction as any);
      expect(fresh.getState().view).toBe("runtime");
      expect(fresh.getState().enabled).toBe(true);
      expect(fresh.getState().hotThresholdPercentile).toBe(50);
    });

    it("clamps out-of-range persisted percentile on restore", () => {
      localStorage.setItem(
        "texera.profiler.state",
        JSON.stringify({ enabled: false, view: "runtime", hotThresholdPercentile: 9999 })
      );
      const fresh = new ProfilerService(workflowStatus as any, executeWorkflow as any, workflowAction as any);
      expect(fresh.getState().hotThresholdPercentile).toBe(100);
    });
  });

  describe("throttling", () => {
    it("emits at most twice per 500ms window of bursts (leading + trailing)", fakeAsync(() => {
      profiler.setEnabled(true);
      let emissions = 0;
      profiler.getStateStream().subscribe(() => emissions++);
      const initialCount = emissions;
      for (let i = 0; i < 20; i++) {
        workflowStatus.statusSubject.next({
          op1: stat({ aggregatedDataProcessingTime: 100 + i }),
        });
        tick(10);
      }
      tick(600);
      // Leading + trailing inside 500ms throttle ≈ at most 2 score-driven emissions for the burst.
      const burstEmissions = emissions - initialCount;
      expect(burstEmissions).toBeLessThanOrEqual(3);
    }));
  });

  describe("per-workflow profiler config sync", () => {
    it("hydrates state when a workflow with profilerConfig is loaded", () => {
      // Initial state: defaults (view=runtime, enabled=false, threshold=80)
      expect(profiler.getState().view).toBe("runtime");

      // Simulate workflow load with a saved profiler config.
      workflowAction.setProfilerConfig({
        enabled: true,
        view: "throughput",
        hotThresholdPercentile: 95,
      });

      expect(profiler.getState().enabled).toBe(true);
      expect(profiler.getState().view).toBe("throughput");
      expect(profiler.getState().hotThresholdPercentile).toBe(95);
    });

    it("leaves state untouched when the loaded workflow has no profilerConfig (undefined)", () => {
      profiler.setView("io-imbalance");
      const before = { ...profiler.getState() };

      // Simulate workflow load with no override.
      workflowAction.profilerConfigSubject.next(undefined);

      expect(profiler.getState().view).toBe(before.view);
      expect(profiler.getState().enabled).toBe(before.enabled);
    });

    it("writes back to the workflow when setEnabled is called", () => {
      profiler.setEnabled(true);
      expect(workflowAction.profilerConfig).toEqual({
        enabled: true,
        view: "runtime",
        hotThresholdPercentile: 80,
      });
    });

    it("writes back to the workflow when setView is called", () => {
      profiler.setView("io-imbalance");
      expect(workflowAction.profilerConfig?.view).toBe("io-imbalance");
    });

    it("writes back to the workflow when setHotThresholdPercentile is called", () => {
      profiler.setHotThresholdPercentile(42);
      expect(workflowAction.profilerConfig?.hotThresholdPercentile).toBe(42);
    });

    it("does not loop: hydrating from a config does not write back the same value", () => {
      // Prime the workflow with a config equal to one we'd derive from current state.
      // After hydration completes, the stub should have received writeCalls only from
      // the explicit setProfilerConfig the test made — none from ProfilerService.
      const initialCalls = workflowAction.writeCalls.length;
      workflowAction.setProfilerConfig({
        enabled: true,
        view: "runtime",
        hotThresholdPercentile: 80,
      });
      // The setProfilerConfig() call above counts as one write call from us.
      // ProfilerService should NOT have written anything back as a side effect.
      expect(workflowAction.writeCalls.length).toBe(initialCalls + 1);
    });

    it("clears scores when the workflow config turns profiling off", () => {
      // Enable profiling first and seed some scores via a stats event.
      profiler.setEnabled(true);
      workflowStatus.currentStatus = { op1: stat({ aggregatedDataProcessingTime: 100 }) };
      // The setEnabled(true) call internally recomputes from currentStatus.
      profiler.setEnabled(true);
      expect(Object.keys(profiler.getState().scores).length).toBe(1);

      // Workflow loads with profiling explicitly off.
      workflowAction.setProfilerConfig({
        enabled: false,
        view: "runtime",
        hotThresholdPercentile: 80,
      });
      expect(profiler.getState().enabled).toBe(false);
      expect(profiler.getState().scores).toEqual({});
    });

    it("baseline: setBaseline stores the report and getBaseline returns it", () => {
      const baseline = {
        header: {
          workflowName: "Prev Run",
          executionName: null,
          generatedAt: "2026-05-14T12:00:00Z",
          view: "runtime",
          hotThresholdPercentile: 80,
          operatorCount: 1,
        },
        operators: [
          {
            operatorId: "op-1",
            displayName: "Python UDF",
            operatorType: "PythonUDFV2",
            score: 0.5,
            runtimeMs: 1000,
            throughputRowsPerSec: 500,
            inputRows: 100,
            outputRows: 100,
            inputSize: null,
            outputSize: null,
            workers: 1,
            idleRatio: null,
          },
        ],
      };
      profiler.setBaseline(baseline);
      expect(profiler.getBaseline()).toBe(baseline);
      expect(profiler.getState().baseline).toBe(baseline);
    });

    it("baseline: clearBaseline removes a previously loaded baseline", () => {
      profiler.setBaseline({
        header: {
          workflowName: "Prev",
          executionName: null,
          generatedAt: "",
          view: "runtime",
          hotThresholdPercentile: 80,
          operatorCount: 0,
        },
        operators: [],
      });
      profiler.clearBaseline();
      expect(profiler.getBaseline()).toBeUndefined();
    });

    it("baseline: clearBaseline is a no-op when nothing is loaded (no extra emission)", () => {
      let emissions = 0;
      profiler.getStateStream().subscribe(() => emissions++);
      const initial = emissions;
      profiler.clearBaseline();
      expect(emissions).toBe(initial);
    });

    it("recomputes scores when workflow config flips profiling on", () => {
      workflowStatus.currentStatus = { op1: stat({ aggregatedDataProcessingTime: 1000 }) };
      // Initially disabled; no scores.
      expect(profiler.getState().enabled).toBe(false);

      workflowAction.setProfilerConfig({
        enabled: true,
        view: "runtime",
        hotThresholdPercentile: 80,
      });
      expect(profiler.getState().enabled).toBe(true);
      expect(profiler.getState().scores["op1"]?.score).toBe(1);
    });
  });
});
