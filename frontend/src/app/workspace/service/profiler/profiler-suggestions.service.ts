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
import { BehaviorSubject, combineLatest, Observable, Subject } from "rxjs";
import { distinctUntilChanged, map } from "rxjs/operators";

const DISMISSED_STORAGE_PREFIX = "texera.profiler.dismissedSuggestions.";

/** Build the localStorage key for a workflow's dismissed-suggestion set. */
function dismissedStorageKey(wid: number): string {
  return `${DISMISSED_STORAGE_PREFIX}${wid}`;
}

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

  /**
   * Materialization is fired through this subject so multiple consumers can trigger it
   * without depending on each other. The workflow-editor component owns the actual
   * mutation (it needs JointJS paper coords); the menu component's popover-list "Apply"
   * buttons just publish requests here.
   */
  private readonly materializeRequest$ = new Subject<Suggestion>();

  /**
   * Fired after a suggestion is materialized when the user clicks the "Run now" prompt
   * that appears above the canvas. The menu component (which owns the actual run-button
   * orchestration: execution name, computing-unit selection, etc.) subscribes and routes
   * through the same handler used by clicking the Run button manually.
   */
  private readonly workflowRunRequest$ = new Subject<void>();

  /** Workflow id we last hydrated the dismissed set for. Used to avoid duplicate hydration. */
  private currentWid: number | undefined;

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

    // Hydrate the dismissed set per workflow. localStorage is keyed by the workflow id
    // so dismissals (a) survive page reload, (b) don't bleed across workflows. Same
    // pattern as P4's `WorkflowProfilerConfig` but stored client-side (since dismissals
    // are per-user, not a property of the workflow itself).
    this.currentWid = this.workflowActionService.getWorkflowMetadata()?.wid;
    this.hydrateFromStorage();
    this.workflowActionService
      .workflowMetaDataChanged()
      .pipe(distinctUntilChanged((a, b) => a?.wid === b?.wid))
      .subscribe(meta => {
        const newWid = meta?.wid;
        if (newWid === this.currentWid) return;
        this.currentWid = newWid;
        this.hydrateFromStorage();
      });
  }

  public getSuggestionsStream(): Observable<readonly Suggestion[]> {
    return this.suggestions$;
  }

  public dismiss(id: SuggestionId): void {
    const next = new Set(this.dismissed.value);
    next.add(id);
    this.dismissed.next(next);
    this.persistToStorage();
  }

  public clearDismissed(): void {
    if (this.dismissed.value.size === 0) return;
    this.dismissed.next(new Set());
    this.persistToStorage();
  }

  /**
   * Hydrate the dismissed set from localStorage for the currently-loaded workflow,
   * or fall back to an empty set when no workflow is loaded / no record exists.
   * Defensive: handles missing localStorage, corrupt JSON, and unexpected shapes.
   */
  private hydrateFromStorage(): void {
    if (this.currentWid === undefined) {
      if (this.dismissed.value.size > 0) this.dismissed.next(new Set());
      return;
    }
    try {
      const raw = localStorage.getItem(dismissedStorageKey(this.currentWid));
      if (!raw) {
        if (this.dismissed.value.size > 0) this.dismissed.next(new Set());
        return;
      }
      const parsed: unknown = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        const next = new Set<SuggestionId>(
          parsed.filter((x): x is string => typeof x === "string")
        );
        this.dismissed.next(next);
      } else {
        // Bogus shape — start fresh.
        this.dismissed.next(new Set());
      }
    } catch {
      // localStorage unavailable or JSON corrupt — silently fall back to empty.
      this.dismissed.next(new Set());
    }
  }

  /**
   * Persist the current dismissed set to localStorage under the current workflow id.
   * No-op when there's no current wid (e.g. a brand-new unsaved workflow); the set
   * remains in-memory for the session.
   */
  private persistToStorage(): void {
    if (this.currentWid === undefined) return;
    try {
      const arr = Array.from(this.dismissed.value);
      localStorage.setItem(dismissedStorageKey(this.currentWid), JSON.stringify(arr));
    } catch {
      // Storage full / disabled / private mode — ignore. Behavior degrades to session-only.
    }
  }

  /**
   * Request that the editor materialize the given suggestion. Fires on
   * `materializeRequest$`; the workflow-editor component subscribes and performs
   * the actual canvas mutation (with access to JointJS paper coordinates).
   */
  public requestMaterialize(suggestion: Suggestion): void {
    this.materializeRequest$.next(suggestion);
  }

  public getMaterializeRequestStream(): Observable<Suggestion> {
    return this.materializeRequest$.asObservable();
  }

  /**
   * Request that the workflow be re-run. Fires on `workflowRunRequest$`; the menu
   * component subscribes and triggers the standard Run-button handler.
   */
  public requestWorkflowRun(): void {
    this.workflowRunRequest$.next();
  }

  public getWorkflowRunRequestStream(): Observable<void> {
    return this.workflowRunRequest$.asObservable();
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
