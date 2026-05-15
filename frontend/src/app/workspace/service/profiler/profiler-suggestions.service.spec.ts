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
import { BehaviorSubject } from "rxjs";
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
  public getTexeraGraph() { return this.graph; }
}

describe("ProfilerSuggestionsService", () => {
  let profiler: StubProfilerService;
  let action: StubWorkflowActionService;
  let service: ProfilerSuggestionsService;

  beforeEach(() => {
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
    expect(last[0].upstreamOpId).toBe("scan");
    expect(last[0].downstreamOpId).toBe("agg");
    expect(last[0].reasonRuleId).toBe("SCAN_FULL_TABLE_NO_FILTER");
    expect(last[0].id).toBe(edgeSuggestionId("scan", "agg"));
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
});
