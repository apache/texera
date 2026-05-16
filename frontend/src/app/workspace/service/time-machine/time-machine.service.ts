/*
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

import { Injectable, OnDestroy } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { BehaviorSubject, Observable, Subject, Subscription, interval, of } from "rxjs";
import { catchError } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { AppSettings } from "../../../common/app-setting";

export type SnapshotChangeType =
  | "operator_added"
  | "operator_removed"
  | "operator_modified"
  | "link_added"
  | "link_removed"
  | "config_changed"
  | "agent_generated"
  | "manual_save"
  | "auto_save"
  | "run"
  | "revert";

export type SnapshotSource = "user" | "agent";

export interface TimeMachineSnapshotEntry {
  sid: number;
  wid: number;
  version: number;
  changeType: SnapshotChangeType;
  changeSummary: string;
  changedOperators: string[];
  source: SnapshotSource;
  uid?: number | null;
  creationTime: string;
}

export interface TimeMachineSnapshotFull extends TimeMachineSnapshotEntry {
  content: string;
}

export interface TimeMachineOperatorDiffEntry {
  operatorID: string;
  operatorType: string;
  customDisplayName: string;
  // Server-side pre-resolved label: customDisplayName || operatorType || operatorID.
  // Always non-empty.
  displayName: string;
}

export interface TimeMachineDiff {
  v1: number;
  v2: number;
  operatorsAdded: TimeMachineOperatorDiffEntry[];
  operatorsRemoved: TimeMachineOperatorDiffEntry[];
  operatorsModified: TimeMachineOperatorDiffEntry[];
  linksAdded: number;
  linksRemoved: number;
}

interface CreateSnapshotPayload {
  content: string;
  changeType: SnapshotChangeType;
  changeSummary: string;
  changedOperators: string[];
  source: SnapshotSource;
}

const TIME_MACHINE_BASE = "time-machine";
const AUTO_SNAPSHOT_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * TimeMachineService captures workflow snapshots at important moments only —
 * not on every keystroke or canvas tweak. Triggers:
 *
 *   - **Auto every 5 min**: if any change has happened since the last snapshot.
 *   - **Run**: when the user starts an execution.
 *   - **Agent**: when an agent-driven change is explicitly flagged
 *     (frontend integrations call `snapshotAgentChange()`, or the agent calls
 *     the `workflow_history` tool with `action: "snapshot"`).
 *   - **Manual**: user clicks "Save Snapshot" in the panel.
 *
 * Discrete graph events (add/remove operator, link, property change) still
 * subscribe — but only to **mark the workflow dirty**, so the 5-min timer
 * knows there's something worth saving. They no longer POST snapshots.
 */
@Injectable({ providedIn: "root" })
export class TimeMachineService implements OnDestroy {
  private readonly captureEnabledSubject = new BehaviorSubject<boolean>(true);
  private readonly snapshotsRefreshSubject = new Subject<void>();
  private sourceOverride: SnapshotSource | null = null;
  private dirty = false;
  private subscriptions: Subscription[] = [];

  constructor(
    private http: HttpClient,
    private workflowActionService: WorkflowActionService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    this.attachDirtyTracking();
    this.attachAutoSnapshotTimer();
    this.attachExecutionHook();
  }

  ngOnDestroy(): void {
    this.subscriptions.forEach(s => s.unsubscribe());
    this.subscriptions = [];
  }

  /** Pause capture (e.g. while reverting). */
  public setCaptureEnabled(enabled: boolean): void {
    this.captureEnabledSubject.next(enabled);
  }

  /**
   * Mark the next captured snapshot as agent-sourced. Resets after one use.
   * Frontend integrations that proxy agent edits should call this just before
   * the edit, then trigger a save (manual or by letting the auto-timer fire).
   */
  public flagNextChangeAsAgent(): void {
    this.sourceOverride = "agent";
  }

  public onSnapshotsChanged(): Observable<void> {
    return this.snapshotsRefreshSubject.asObservable();
  }

  public listSnapshots(wid: number): Observable<TimeMachineSnapshotEntry[]> {
    return this.http.get<TimeMachineSnapshotEntry[]>(
      `${AppSettings.getApiEndpoint()}/${TIME_MACHINE_BASE}/${wid}/snapshots`
    );
  }

  public getSnapshot(wid: number, sid: number): Observable<TimeMachineSnapshotFull> {
    return this.http.get<TimeMachineSnapshotFull>(
      `${AppSettings.getApiEndpoint()}/${TIME_MACHINE_BASE}/${wid}/snapshots/${sid}`
    );
  }

  public revertToSnapshot(wid: number, sid: number): Observable<TimeMachineSnapshotFull> {
    return this.http.post<TimeMachineSnapshotFull>(
      `${AppSettings.getApiEndpoint()}/${TIME_MACHINE_BASE}/${wid}/snapshots/${sid}/revert`,
      {}
    );
  }

  public diff(wid: number, v1: number, v2: number): Observable<TimeMachineDiff> {
    return this.http.get<TimeMachineDiff>(
      `${AppSettings.getApiEndpoint()}/${TIME_MACHINE_BASE}/${wid}/diff?v1=${v1}&v2=${v2}`
    );
  }

  /** Manual user-triggered checkpoint. Wired to the panel's "Save Snapshot" button. */
  public manualSnapshot(summary: string = "Manual snapshot"): void {
    this.capture("manual_save", summary, []);
  }

  /** Explicit agent-driven snapshot. */
  public snapshotAgentChange(summary: string, changedOperators: string[] = []): void {
    this.sourceOverride = "agent";
    this.capture("agent_generated", summary, changedOperators);
  }

  private attachDirtyTracking(): void {
    const graph = this.workflowActionService.getTexeraGraph();
    // We listen to the same events as before, but only to mark dirty. No HTTP.
    const dirtyStreams: Observable<unknown>[] = [
      graph.getOperatorAddStream(),
      graph.getOperatorDeleteStream(),
      graph.getLinkAddStream(),
      graph.getLinkDeleteStream(),
      graph.getOperatorPropertyChangeStream(),
    ];
    dirtyStreams.forEach(stream => {
      this.subscriptions.push(stream.subscribe(() => (this.dirty = true)));
    });
  }

  private attachAutoSnapshotTimer(): void {
    this.subscriptions.push(
      interval(AUTO_SNAPSHOT_INTERVAL_MS).subscribe(() => {
        if (!this.captureEnabledSubject.value) return;
        if (!this.dirty) return;
        this.capture("auto_save", "Auto-saved checkpoint", []);
      })
    );
  }

  private attachExecutionHook(): void {
    this.subscriptions.push(
      this.executeWorkflowService.getExecutionStateStream().subscribe(({ previous, current }) => {
        // Snapshot on the transition INTO Initializing — i.e. the moment the
        // user clicks Run. Only the leading edge, not every subsequent emission.
        if (current.state === ExecutionState.Initializing && previous.state !== ExecutionState.Initializing) {
          this.capture("run", "Workflow run started", []);
        }
      })
    );
  }

  private capture(changeType: SnapshotChangeType, summary: string, changedOperators: string[]): void {
    if (!this.captureEnabledSubject.value) return;
    const wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (!wid) return;
    const content = JSON.stringify(this.workflowActionService.getWorkflowContent());
    const source = this.sourceOverride ?? "user";
    this.sourceOverride = null;
    const payload: CreateSnapshotPayload = {
      content,
      changeType,
      changeSummary: summary,
      changedOperators,
      source,
    };
    this.http
      .post<TimeMachineSnapshotEntry>(
        `${AppSettings.getApiEndpoint()}/${TIME_MACHINE_BASE}/${wid}/snapshots`,
        payload
      )
      .pipe(
        catchError(err => {
          console.warn("[TimeMachine] snapshot save failed", err);
          return of(null);
        })
      )
      .subscribe(saved => {
        if (saved) {
          this.dirty = false;
          this.snapshotsRefreshSubject.next();
        }
      });
  }
}
