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
import { BehaviorSubject, combineLatest, Observable } from "rxjs";
import { distinctUntilChanged, map } from "rxjs/operators";

import { ProfilerService } from "./profiler.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { HintContext } from "./profiler-hints";
import { statsToComparable } from "./profiler-delta";
import {
  computeSuggestions,
  Suggestion,
  SuggestionId,
} from "./profiler-suggestions";

/**
 * Holds the dismissed-set + exposes a live stream of ghost suggestions derived from
 * ProfilerService state + the workflow graph. Thin: all logic lives in the pure
 * `profiler-suggestions.ts` module; this class just wires Angular streams together.
 */
@Injectable({
  providedIn: "root",
})
export class ProfilerSuggestionsService {
  private readonly dismissed = new BehaviorSubject<ReadonlySet<SuggestionId>>(new Set());

  private readonly suggestions$: Observable<readonly Suggestion[]>;

  constructor(
    private profilerService: ProfilerService,
    private workflowActionService: WorkflowActionService
  ) {
    this.suggestions$ = combineLatest([
      this.profilerService.getStateStream(),
      this.dismissed.asObservable(),
    ]).pipe(
      map(([state, dismissed]) => {
        if (!state.enabled) return [] as readonly Suggestion[];
        return computeSuggestions(this.buildHintContext(), dismissed);
      }),
      // Cheap structural equality so subscribers don't re-render on identical lists.
      distinctUntilChanged((a, b) => sameSuggestions(a, b))
    );

    // Note: dismissed-set is session-scoped (lives for the lifetime of this root service).
    // We deliberately do not auto-clear on workflow reload because WorkflowActionService
    // does not expose a public reset stream; the risk of stale dismissals across workflows
    // is low (operator ids are workflow-specific) and clearing the page resets everything.
  }

  public getSuggestionsStream(): Observable<readonly Suggestion[]> {
    return this.suggestions$;
  }

  public dismiss(id: SuggestionId): void {
    const next = new Set(this.dismissed.value);
    next.add(id);
    this.dismissed.next(next);
  }

  public clearDismissed(): void {
    if (this.dismissed.value.size === 0) return;
    this.dismissed.next(new Set());
  }

  /**
   * Builds the same HintContext shape that `operator-property-edit-frame` builds for
   * its hint computation. Pulled into this service so both the side panel and the
   * canvas ghosts see identical inputs and produce consistent recommendations.
   */
  private buildHintContext(): HintContext {
    const state = this.profilerService.getState();
    const graph = this.workflowActionService.getTexeraGraph();
    const stats: Record<string, ReturnType<typeof statsToComparable> extends infer R ? any : never> = {};
    const scoreMap: Record<string, number> = {};
    for (const id of Object.keys(state.scores)) {
      stats[id] = state.scores[id].stats;
      scoreMap[id] = state.scores[id].score;
    }
    return {
      stats,
      scores: scoreMap,
      hotThreshold: state.hotThresholdPercentile / 100,
      operatorType: id => {
        try {
          return graph.getOperator(id)?.operatorType;
        } catch {
          return undefined;
        }
      },
      displayName: id => {
        try {
          const op = graph.getOperator(id);
          return op?.customDisplayName?.trim() || op?.operatorType || id;
        } catch {
          return id;
        }
      },
      upstreamOps: id => {
        try {
          return graph.getInputLinksByOperatorId(id).map(l => l.source.operatorID);
        } catch {
          return [];
        }
      },
      downstreamOps: id => {
        try {
          return graph.getOutputLinksByOperatorId(id).map(l => l.target.operatorID);
        } catch {
          return [];
        }
      },
    };
  }
}

function sameSuggestions(a: readonly Suggestion[], b: readonly Suggestion[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i].id !== b[i].id) return false;
  }
  return true;
}
