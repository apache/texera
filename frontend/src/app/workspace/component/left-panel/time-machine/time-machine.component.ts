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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { CommonModule, DatePipe, NgClass, NgFor, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Subject } from "rxjs";
import { takeUntil } from "rxjs/operators";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import {
  TimeMachineDiff,
  TimeMachineOperatorDiffEntry,
  TimeMachineService,
  TimeMachineSnapshotEntry,
} from "../../../service/time-machine/time-machine.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { UndoRedoService } from "../../../service/undo-redo/undo-redo.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { Workflow, WorkflowContent } from "../../../../common/type/workflow";

@UntilDestroy()
@Component({
  selector: "texera-time-machine",
  templateUrl: "time-machine.component.html",
  styleUrls: ["time-machine.component.scss"],
  imports: [
    CommonModule,
    NgIf,
    NgFor,
    NgClass,
    DatePipe,
    NzButtonModule,
    NzTagModule,
    NzEmptyModule,
    NzTooltipModule,
  ],
})
export class TimeMachineComponent implements OnInit, OnDestroy {
  public snapshots: TimeMachineSnapshotEntry[] = [];
  public selectedSid: number | null = null;
  public previewSummary: string | null = null;
  public compareSids: number[] = [];
  public diffResult: TimeMachineDiff | null = null;
  public loading = false;
  public error: string | null = null;
  private destroy$ = new Subject<void>();

  constructor(
    private timeMachineService: TimeMachineService,
    private workflowActionService: WorkflowActionService,
    private undoRedoService: UndoRedoService,
    private operatorMetadataService: OperatorMetadataService
  ) {}

  /**
   * Map an operator diff entry to a human-readable label. The backend already
   * resolves `displayName` to customDisplayName || operatorType || operatorID,
   * so it's always non-empty. We still try the schema's userFriendlyName when
   * we only have an operatorType so the label reads nicely (e.g. "CSV File
   * Scan" instead of "CSVFileScan").
   */
  public operatorLabel(entry: TimeMachineOperatorDiffEntry): string {
    const custom = entry.customDisplayName?.trim();
    if (custom) return custom;
    if (entry.operatorType) {
      try {
        const schema = this.operatorMetadataService.getOperatorSchema(entry.operatorType);
        const friendly = schema?.additionalMetadata?.userFriendlyName;
        if (friendly) return friendly;
      } catch {
        // schema not loaded yet or unknown type — fall through to displayName
      }
    }
    return entry.displayName || entry.operatorID;
  }

  /** Comma-joined friendly labels for a diff section. */
  public formatOperatorList(entries: TimeMachineOperatorDiffEntry[]): string {
    if (!entries || entries.length === 0) return "(none)";
    return entries.map(e => this.operatorLabel(e)).join(", ");
  }

  ngOnInit(): void {
    this.refresh();
    // refresh whenever a snapshot is captured
    this.timeMachineService
      .onSnapshotsChanged()
      .pipe(takeUntil(this.destroy$), untilDestroyed(this))
      .subscribe(() => this.refresh());
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  public get currentWid(): number {
    return this.workflowActionService.getWorkflowMetadata()?.wid ?? 0;
  }

  public refresh(): void {
    const wid = this.currentWid;
    if (!wid) {
      this.snapshots = [];
      this.error = "Save the workflow first to start capturing snapshots.";
      return;
    }
    this.loading = true;
    this.error = null;
    this.timeMachineService
      .listSnapshots(wid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: list => {
          this.snapshots = list ?? [];
          this.loading = false;
        },
        error: err => {
          this.loading = false;
          this.error = err?.message || "Failed to load history";
        },
      });
  }

  public sourceLabel(entry: TimeMachineSnapshotEntry): string {
    return entry.source === "agent" ? "🤖 agent" : "👤 user";
  }

  public scopeBadge(entry: TimeMachineSnapshotEntry): string {
    const n = entry.changedOperators?.length ?? 0;
    switch (entry.changeType) {
      case "operator_added":
        return `+${n} operator${n === 1 ? "" : "s"}`;
      case "operator_removed":
        return `-${n} operator${n === 1 ? "" : "s"}`;
      case "link_added":
        return "+1 link";
      case "link_removed":
        return "-1 link";
      case "config_changed":
        return n > 0 ? `~${n} config` : "config";
      case "revert":
        return "revert";
      case "agent_generated":
        return n > 0 ? `~${n} ops` : "agent";
      case "manual_save":
        return "checkpoint";
      case "auto_save":
        return "auto-save";
      case "run":
        return "run";
      default:
        return entry.changeType;
    }
  }

  public saveSnapshot(): void {
    if (!this.currentWid) {
      this.error = "Save the workflow first to capture a snapshot.";
      return;
    }
    this.timeMachineService.manualSnapshot("Manual snapshot");
  }

  public selectSnapshot(entry: TimeMachineSnapshotEntry): void {
    this.selectedSid = entry.sid;
    this.previewSummary = `v${entry.version} — ${entry.changeSummary}`;
  }

  public toggleCompare(entry: TimeMachineSnapshotEntry, event: MouseEvent): void {
    event.stopPropagation();
    const idx = this.compareSids.indexOf(entry.sid);
    if (idx >= 0) {
      this.compareSids.splice(idx, 1);
    } else {
      this.compareSids.push(entry.sid);
      if (this.compareSids.length > 2) this.compareSids.shift();
    }
    this.diffResult = null;
    if (this.compareSids.length === 2) {
      const [a, b] = this.compareSids;
      this.timeMachineService
        .diff(this.currentWid, a, b)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: d => (this.diffResult = d),
          error: err => (this.error = err?.message || "Failed to compute diff"),
        });
    }
  }

  public isInCompare(entry: TimeMachineSnapshotEntry): boolean {
    return this.compareSids.includes(entry.sid);
  }

  /** 1 or 2 for the order this snapshot was added to the compare queue; null if not queued. */
  public compareOrdinal(entry: TimeMachineSnapshotEntry): number | null {
    const idx = this.compareSids.indexOf(entry.sid);
    return idx >= 0 ? idx + 1 : null;
  }

  public clearCompare(): void {
    this.compareSids = [];
    this.diffResult = null;
  }

  /**
   * Revert: backend writes the snapshot's content back into the workflow row,
   * then we reload the canvas with that content. Capture is paused during the
   * reload so we don't snapshot the cascade of "delete all + re-add" events.
   */
  public revertTo(entry: TimeMachineSnapshotEntry): void {
    if (!confirm(`Revert workflow to version ${entry.version}? Current state will become a new snapshot.`)) {
      return;
    }
    const wid = this.currentWid;
    if (!wid) return;
    this.timeMachineService.setCaptureEnabled(false);
    this.timeMachineService
      .revertToSnapshot(wid, entry.sid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: full => {
          try {
            const content = JSON.parse(full.content) as WorkflowContent;
            const metadata = this.workflowActionService.getWorkflowMetadata();
            const workflow: Workflow = { ...metadata, content };
            this.workflowActionService.reloadWorkflow(workflow);
            this.undoRedoService.clearUndoStack();
            this.undoRedoService.clearRedoStack();
          } catch (e) {
            console.error("Failed to apply reverted workflow", e);
            this.error = "Revert saved but failed to apply on canvas. Reload the page.";
          } finally {
            // small delay before re-enabling so the reload's cascade of mutation
            // events flushes without producing extra snapshots
            setTimeout(() => this.timeMachineService.setCaptureEnabled(true), 1500);
            this.refresh();
          }
        },
        error: err => {
          this.timeMachineService.setCaptureEnabled(true);
          this.error = err?.message || "Revert failed";
        },
      });
  }
}
