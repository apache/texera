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
import { BehaviorSubject, Observable } from "rxjs";
import { throttleTime } from "rxjs/operators";
import {
  ExecutionState,
  OperatorState,
  OperatorStatistics,
} from "../../types/execute-workflow.interface";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { ProfilerConfig, profilerConfigEquals, serializeProfilerConfig } from "./profiler-config";
import { BaselineReport } from "./profiler-delta";

export type ProfilerView = "runtime" | "throughput" | "io-imbalance" | "delta";

export interface ProfilerEntry {
  readonly score: number;
  readonly state: OperatorState;
  readonly stats: OperatorStatistics;
}

export interface ProfilerState {
  readonly enabled: boolean;
  readonly view: ProfilerView;
  readonly hotThresholdPercentile: number;
  readonly scores: Readonly<Record<string, ProfilerEntry>>;
  /**
   * Optional baseline snapshot loaded by the user (uploaded P3 JSON report).
   * When present, the side panel surfaces per-operator deltas against it.
   * Not persisted (kept in memory only) — reports can be large and ephemeral.
   */
  readonly baseline?: BaselineReport;
}

const STORAGE_KEY = "texera.profiler.state";
const THROTTLE_MS = 500;

const DEFAULT_STATE: ProfilerState = {
  enabled: false,
  view: "runtime",
  hotThresholdPercentile: 80,
  scores: {},
};

@Injectable({
  providedIn: "root",
})
export class ProfilerService {
  private readonly state$ = new BehaviorSubject<ProfilerState>(DEFAULT_STATE);

  constructor(
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService
  ) {
    this.restoreConfig();

    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(throttleTime(THROTTLE_MS, undefined, { leading: true, trailing: true }))
      .subscribe(stats => this.recomputeScores(stats));

    this.executeWorkflowService.getExecutionStateStream().subscribe(({ previous, current }) => {
      const isRestart =
        current.state === ExecutionState.Initializing ||
        (current.state === ExecutionState.Running && previous.state === ExecutionState.Uninitialized);
      if (isRestart) {
        this.emit({ scores: {} });
      }
    });

    // Per-workflow override: when a workflow with a saved profilerConfig is loaded,
    // its values win over the user's localStorage defaults. `undefined` means the
    // workflow has no override — keep the current state unchanged.
    this.workflowActionService.getProfilerConfigStream().subscribe(cfg => {
      if (!cfg) return;
      this.hydrateFromConfig(cfg);
    });
  }

  public getState(): ProfilerState {
    return this.state$.value;
  }

  public getStateStream(): Observable<ProfilerState> {
    return this.state$.asObservable();
  }

  public setEnabled(enabled: boolean): void {
    if (enabled) {
      this.emit({ enabled });
      this.recomputeScores(this.workflowStatusService.getCurrentStatus());
    } else {
      // Clear cached scores when disabling so consumers (side panel, etc.)
      // do not show stale per-operator heat values.
      this.emit({ enabled, scores: {} });
    }
    this.persistConfig();
    this.persistToWorkflow();
  }

  public setView(view: ProfilerView): void {
    this.emit({ view });
    this.persistConfig();
    this.persistToWorkflow();
    this.recomputeScores(this.workflowStatusService.getCurrentStatus());
  }

  public setHotThresholdPercentile(percentile: number): void {
    const clamped = Math.max(0, Math.min(100, percentile));
    this.emit({ hotThresholdPercentile: clamped });
    this.persistConfig();
    this.persistToWorkflow();
  }

  /**
   * Loads a baseline snapshot (a parsed P3 JSON report) for comparison against
   * the live run. Replaces any previously loaded baseline.
   */
  public setBaseline(baseline: BaselineReport): void {
    this.emit({ baseline });
  }

  /** Clears the loaded baseline; the side-panel comparison section will disappear. */
  public clearBaseline(): void {
    if (this.state$.value.baseline === undefined) return;
    this.emit({ baseline: undefined });
  }

  /** Returns the currently-loaded baseline, or `undefined` if none. */
  public getBaseline(): BaselineReport | undefined {
    return this.state$.value.baseline;
  }

  /**
   * Pure score computation. Exposed for unit tests.
   */
  public computeScores(
    stats: Record<string, OperatorStatistics>,
    view: ProfilerView
  ): Record<string, ProfilerEntry> {
    const opIds = Object.keys(stats);
    if (opIds.length === 0) return {};

    const rawCost: Record<string, number> = {};
    for (const opId of opIds) {
      rawCost[opId] = this.rawCostFor(stats[opId], view);
    }

    const maxCost = Math.max(0, ...Object.values(rawCost));
    const result: Record<string, ProfilerEntry> = {};
    for (const opId of opIds) {
      const s = stats[opId];
      const score = maxCost > 0 ? clamp(rawCost[opId] / maxCost, 0, 1) : 0;
      result[opId] = {
        score: Number.isFinite(score) ? score : 0,
        state: s.operatorState,
        stats: s,
      };
    }
    return result;
  }

  private recomputeScores(stats: Record<string, OperatorStatistics>): void {
    if (!this.state$.value.enabled) {
      if (Object.keys(this.state$.value.scores).length > 0) {
        this.emit({ scores: {} });
      }
      return;
    }
    const scores = this.computeScores(stats, this.state$.value.view);
    this.emit({ scores });
  }

  private rawCostFor(s: OperatorStatistics, view: ProfilerView): number {
    switch (view) {
      case "runtime": {
        const t = s.aggregatedDataProcessingTime ?? 0;
        return Number.isFinite(t) && t > 0 ? t : 0;
      }
      case "throughput": {
        // Slow producers are "hot": invert output so small output -> high cost.
        const out = s.aggregatedOutputRowCount ?? 0;
        return out > 0 ? 1 / out : 0;
      }
      case "io-imbalance": {
        const inp = s.aggregatedInputRowCount ?? 0;
        const out = s.aggregatedOutputRowCount ?? 0;
        if (inp <= 0) return 0;
        return clamp(1 - out / inp, 0, 1);
      }
      case "delta": {
        // Delta view paints the canvas using current-vs-baseline deltas (handled
        // in the heatmap handler, not here). The side-panel "Heat score" still
        // uses runtime so the number stays meaningful when this view is selected.
        const t = s.aggregatedDataProcessingTime ?? 0;
        return Number.isFinite(t) && t > 0 ? t : 0;
      }
    }
  }

  private emit(patch: Partial<ProfilerState>): void {
    this.state$.next({ ...this.state$.value, ...patch });
  }

  private persistConfig(): void {
    try {
      const { enabled, view, hotThresholdPercentile } = this.state$.value;
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ enabled, view, hotThresholdPercentile }));
    } catch {
      // localStorage unavailable; ignore.
    }
  }

  /**
   * Writes the current profiler config back into the active workflow content so it
   * survives save/load round-trips. WorkflowActionService deep-equal-guards the write
   * so this is a no-op when the value hasn't actually changed (avoiding a feedback
   * loop with our own getProfilerConfigStream subscription).
   */
  private persistToWorkflow(): void {
    const { enabled, view, hotThresholdPercentile } = this.state$.value;
    const cfg: ProfilerConfig = serializeProfilerConfig({ enabled, view, hotThresholdPercentile });
    this.workflowActionService.setProfilerConfig(cfg);
  }

  /**
   * Applies a workflow-supplied profiler config to in-memory state. Guards against
   * the persistToWorkflow → getProfilerConfigStream feedback loop by early-returning
   * when state already matches the incoming config.
   */
  private hydrateFromConfig(cfg: ProfilerConfig): void {
    const current = serializeProfilerConfig(this.state$.value);
    if (profilerConfigEquals(current, cfg)) return;
    const wasEnabled = this.state$.value.enabled;
    this.emit({
      enabled: cfg.enabled,
      view: cfg.view,
      hotThresholdPercentile: cfg.hotThresholdPercentile,
    });
    // Recompute scores if the workflow's config turned profiling on, or if it
    // changed the view while profiling stays on (different formula → different scores).
    if (cfg.enabled) {
      this.recomputeScores(this.workflowStatusService.getCurrentStatus());
    } else if (wasEnabled) {
      // Workflow disabled profiling — clear stale scores in one synchronous emit.
      this.emit({ scores: {} });
    }
  }

  private restoreConfig(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as Partial<ProfilerState>;
      const view: ProfilerView = isValidView(parsed.view) ? parsed.view : DEFAULT_STATE.view;
      const rawPct = parsed.hotThresholdPercentile;
      const hotThresholdPercentile =
        typeof rawPct === "number" && Number.isFinite(rawPct)
          ? Math.max(0, Math.min(100, rawPct))
          : DEFAULT_STATE.hotThresholdPercentile;
      this.emit({
        enabled: typeof parsed.enabled === "boolean" ? parsed.enabled : DEFAULT_STATE.enabled,
        view,
        hotThresholdPercentile,
      });
    } catch {
      // ignore corrupt config
    }
  }
}

function isValidView(v: unknown): v is ProfilerView {
  return v === "runtime" || v === "throughput" || v === "io-imbalance";
}

function clamp(x: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, x));
}
