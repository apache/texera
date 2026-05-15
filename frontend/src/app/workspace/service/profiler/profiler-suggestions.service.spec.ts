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
import { BehaviorSubject, Subject } from "rxjs";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { ProfilerEntry, ProfilerState } from "./profiler.service";
import { ProfilerSuggestionsService } from "./profiler-suggestions.service";
import { Suggestion, edgeSuggestionId } from "./profiler-suggestions";

function stat(partial: Partial<OperatorStatistics> = {}): OperatorStatistics {
  return {
    operatorState: OperatorState.Completed,
    aggregatedInputRowCount: 0,
    inputPortMetrics: {},
    aggregatedOutputRowCount: 0,
    outputPortMetrics: {},
    ...partial,
  };
}

interface FakeOperator {
  operatorID: string;
  operatorType: string;
  customDisplayName?: string;
}
interface FakeLink {
  source: { operatorID: string };
  target: { operatorID: string };
}

/**
 * Minimal fakes that mirror the parts of ProfilerService / WorkflowActionService
 * that ProfilerSuggestionsService touches.
 */
class StubProfilerService {
  public state$: BehaviorSubject<ProfilerState> = new BehaviorSubject<ProfilerState>({
    enabled: true,
    view: "runtime",
    hotThresholdPercentile: 80,
    scores: {},
  });
  public getState() { return this.state$.value; }
  public getStateStream() { return this.state$.asObservable(); }
}

class StubGraph {
  public operators: Record<string, FakeOperator> = {};
  public links: FakeLink[] = [];

  public getOperator(id: string): FakeOperator | undefined {
    return this.operators[id];
  }
  public getInputLinksByOperatorId(id: string): FakeLink[] {
    return this.links.filter(l => l.target.operatorID === id);
  }
  public getOutputLinksByOperatorId(id: string): FakeLink[] {
    return this.links.filter(l => l.source.operatorID === id);
  }
}
class StubWorkflowActionService {
  public graph = new StubGraph();
  public metadata: { wid: number | undefined } = { wid: undefined };
  public metadataChange$ = new Subject<{ wid: number | undefined }>();
  public getTexeraGraph() { return this.graph; }
  public getWorkflowMetadata() { return this.metadata; }
  public workflowMetaDataChanged() { return this.metadataChange$.asObservable(); }

  /** Test helper to simulate workflow load / switch. */
  public emitWorkflow(wid: number | undefined): void {
    this.metadata = { wid };
    this.metadataChange$.next(this.metadata);
  }
}

/**
 * Install an in-memory localStorage mock so the service's persistence path is
 * exercisable under Vitest (which ships only a partial Storage implementation).
 */
function installLocalStorageMock(): void {
  const store: Record<string, string> = {};
  const mock: Storage = {
    get length() { return Object.keys(store).length; },
    clear: () => { for (const k of Object.keys(store)) delete store[k]; },
    getItem: (k: string) => (k in store ? store[k] : null),
    key: (i: number) => Object.keys(store)[i] ?? null,
    removeItem: (k: string) => { delete store[k]; },
    setItem: (k: string, v: string) => { store[k] = String(v); },
  };
  Object.defineProperty(globalThis, "localStorage", {
    value: mock,
    configurable: true,
    writable: true,
  });
}

describe("ProfilerSuggestionsService", () => {
  let profiler: StubProfilerService;
  let action: StubWorkflowActionService;
  let service: ProfilerSuggestionsService;

  beforeEach(() => {
    installLocalStorageMock();
    profiler = new StubProfilerService();
    action = new StubWorkflowActionService();
    service = new ProfilerSuggestionsService(profiler as any, action as any);
  });

  function setScoresAndGraph(
    stats: Record<string, OperatorStatistics>,
    ops: Record<string, string>,
    links: FakeLink[] = []
  ): void {
    const scores: Record<string, ProfilerEntry> = {};
    for (const id of Object.keys(stats)) {
      scores[id] = { score: 0, state: stats[id].operatorState, stats: stats[id] };
    }
    profiler.state$.next({
      ...profiler.state$.value,
      scores,
    });
    action.graph.operators = {};
    for (const id of Object.keys(ops)) {
      action.graph.operators[id] = { operatorID: id, operatorType: ops[id] };
    }
    action.graph.links = links;
  }

  function collect(): Suggestion[][] {
    const emissions: Suggestion[][] = [];
    service.getSuggestionsStream().subscribe(s => emissions.push([...s]));
    return emissions;
  }

  it("emits no suggestions when profiling is disabled", () => {
    profiler.state$.next({ ...profiler.state$.value, enabled: false });
    setScoresAndGraph(
      { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
      { scan: "CSVScan", agg: "Aggregate" },
      [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
    );
    const emissions = collect();
    expect(emissions[emissions.length - 1]).toHaveLength(0);
  });

  it("emits an INSERT_FILTER suggestion for an unfiltered large scan when enabled", () => {
    setScoresAndGraph(
      { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
      { scan: "CSVScan", agg: "Aggregate" },
      [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
    );
    const emissions = collect();
    const last = emissions[emissions.length - 1];
    expect(last).toHaveLength(1);
    const first = last[0];
    expect(first.type).toBe("INSERT_FILTER");
    if (first.type === "INSERT_FILTER") {
      expect(first.upstreamOpId).toBe("scan");
      expect(first.downstreamOpId).toBe("agg");
      expect(first.reasonRuleId).toBe("SCAN_FULL_TABLE_NO_FILTER");
      expect(first.id).toBe(edgeSuggestionId("scan", "agg"));
    }
  });

  it("filters out dismissed suggestions", () => {
    setScoresAndGraph(
      { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
      { scan: "CSVScan", agg: "Aggregate" },
      [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
    );
    const emissions = collect();
    // First (post-subscribe + initial graph) emission: 1 suggestion present.
    expect(emissions[emissions.length - 1]).toHaveLength(1);

    service.dismiss(edgeSuggestionId("scan", "agg"));
    expect(emissions[emissions.length - 1]).toHaveLength(0);
  });

  it("clearDismissed re-emits previously dismissed suggestions", () => {
    setScoresAndGraph(
      { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
      { scan: "CSVScan", agg: "Aggregate" },
      [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
    );
    service.dismiss(edgeSuggestionId("scan", "agg"));
    const emissions = collect();
    expect(emissions[emissions.length - 1]).toHaveLength(0);

    service.clearDismissed();
    expect(emissions[emissions.length - 1]).toHaveLength(1);
  });

  it("requestWorkflowRun publishes on the run-request stream", () => {
    let received = 0;
    service.getWorkflowRunRequestStream().subscribe(() => received++);
    service.requestWorkflowRun();
    service.requestWorkflowRun();
    expect(received).toBe(2);
  });

  it("requestMaterialize publishes on the materialize-request stream", () => {
    setScoresAndGraph(
      { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
      { scan: "CSVScan", agg: "Aggregate" },
      [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
    );
    const received: Suggestion[] = [];
    service.getMaterializeRequestStream().subscribe(s => received.push(s));

    // Construct a synthetic INSERT_FILTER suggestion (same shape pure engine would produce).
    const synthetic: Suggestion = {
      id: edgeSuggestionId("scan", "agg"),
      type: "INSERT_FILTER",
      upstreamOpId: "scan",
      downstreamOpId: "agg",
      reasonRuleId: "SCAN_FULL_TABLE_NO_FILTER",
      reasonMessage: "x",
    };
    service.requestMaterialize(synthetic);
    expect(received).toHaveLength(1);
    expect(received[0].id).toBe(edgeSuggestionId("scan", "agg"));
  });

  describe("per-workflow persistence", () => {
    it("persists dismissed ids to localStorage under the current workflow id", () => {
      action.emitWorkflow(42);
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      service.dismiss(edgeSuggestionId("scan", "agg"));
      const raw = localStorage.getItem("texera.profiler.dismissedSuggestions.42");
      expect(raw).toBeTruthy();
      expect(JSON.parse(raw!)).toEqual([edgeSuggestionId("scan", "agg")]);
    });

    it("hydrates dismissed ids from localStorage when a workflow is loaded", () => {
      localStorage.setItem(
        "texera.profiler.dismissedSuggestions.7",
        JSON.stringify([edgeSuggestionId("scan", "agg")])
      );
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      const emissions = collect();
      // Pre-workflow-load: dismissals are empty, so suggestion shows up.
      expect(emissions[emissions.length - 1]).toHaveLength(1);

      action.emitWorkflow(7);
      // After hydrating workflow 7's dismissals: suggestion is filtered out.
      expect(emissions[emissions.length - 1]).toHaveLength(0);
    });

    it("swaps dismissed-set when the workflow id changes", () => {
      localStorage.setItem(
        "texera.profiler.dismissedSuggestions.1",
        JSON.stringify([edgeSuggestionId("scan", "agg")])
      );
      // Workflow 2 has no stored dismissals.
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      const emissions = collect();

      action.emitWorkflow(1);
      expect(emissions[emissions.length - 1]).toHaveLength(0); // dismissed in wf 1

      action.emitWorkflow(2);
      expect(emissions[emissions.length - 1]).toHaveLength(1); // visible in wf 2
    });

    it("does NOT persist when no workflow id is loaded (session-only)", () => {
      // No emitWorkflow — wid stays undefined.
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      service.dismiss(edgeSuggestionId("scan", "agg"));
      // No keys should have been written.
      let foundProfilerKey = false;
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i);
        if (k?.startsWith("texera.profiler.dismissedSuggestions.")) {
          foundProfilerKey = true;
          break;
        }
      }
      expect(foundProfilerKey).toBe(false);
    });

    it("recovers gracefully from corrupt persisted JSON", () => {
      localStorage.setItem("texera.profiler.dismissedSuggestions.9", "not valid json {");
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      const emissions = collect();
      action.emitWorkflow(9);
      // Bad JSON → fall back to empty set → suggestion is visible.
      expect(emissions[emissions.length - 1]).toHaveLength(1);
    });

    it("clearDismissed wipes the persisted entry too", () => {
      action.emitWorkflow(11);
      setScoresAndGraph(
        { scan: stat({ aggregatedOutputRowCount: 5_000_000 }) },
        { scan: "CSVScan", agg: "Aggregate" },
        [{ source: { operatorID: "scan" }, target: { operatorID: "agg" } }]
      );
      service.dismiss(edgeSuggestionId("scan", "agg"));
      expect(JSON.parse(localStorage.getItem("texera.profiler.dismissedSuggestions.11")!)).toHaveLength(1);

      service.clearDismissed();
      expect(JSON.parse(localStorage.getItem("texera.profiler.dismissedSuggestions.11")!)).toEqual([]);
    });
  });
});
