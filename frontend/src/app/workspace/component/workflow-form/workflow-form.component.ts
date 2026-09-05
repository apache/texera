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

import { ChangeDetectorRef, Component, ElementRef, HostListener, OnDestroy, OnInit } from "@angular/core";
import { CommonModule, DatePipe } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { forkJoin } from "rxjs";
import { debounceTime } from "rxjs/operators";

import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { Workflow, WorkflowContent } from "../../../common/type/workflow";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { Point } from "../../types/workflow-common.interface";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { SAVE_DEBOUNCE_TIME_IN_MS } from "../workspace.component";

/**
 * The Form View: a second way to use a workflow. Building on the page shell, this PR adds the
 * title bar -- the workflow name (renamable, exactly as on the operator canvas), its "Saved
 * at ..." state, and the debounced save that both views share so an edit in one view is not
 * lost in the other. The read-only preview, the inputs, running and results are added by later
 * PRs. A view, not a new object: it opens the same workflow the canvas does.
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-form",
  templateUrl: "./workflow-form.component.html",
  styleUrls: ["./workflow-form.component.scss"],
  imports: [CommonModule, FormsModule, NzAvatarModule, UserIconComponent, CoeditorUserIconComponent],
})
export class WorkflowFormComponent implements OnInit, OnDestroy {
  public wid?: number;
  public workflowName = "";
  public loading = true;
  /** "Saved at …", worded and formatted exactly as on the operator canvas. */
  public autoSaveState = "";

  /** Set on teardown so deferred callbacks stop touching a view that is gone. */
  private destroyed = false;

  /**
   * Operator positions as loaded, kept only as a fallback: a save writes the live positions
   * from the shared model (a co-editor's drags included), and falls back to this snapshot, then
   * origin, if the live map is ever missing an operator -- so a save never drops an operator's
   * position, and never overwrites a co-editor's move with a stale one.
   */
  private storedPositions: { [operatorID: string]: Point } = {};

  constructor(
    // Public for the template: shows the same live collaborator avatars as the canvas.
    public coeditorPresenceService: CoeditorPresenceService,
    private route: ActivatedRoute,
    private router: Router,
    private workflowActionService: WorkflowActionService,
    private workflowPersistService: WorkflowPersistService,
    private operatorMetadataService: OperatorMetadataService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService,
    private userService: UserService,
    private cdr: ChangeDetectorRef,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private host: ElementRef<HTMLElement>,
    private datePipe: DatePipe,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    const wid = Number(this.route.snapshot.params.id);
    if (!Number.isFinite(wid)) {
      void this.router.navigate([USER_WORKFLOW]);
      return;
    }
    this.wid = wid;
    this.load(wid);
  }

  private load(wid: number): void {
    // With the feature off the form does not exist: hand straight to the operator canvas
    // without loading anything, so a request that then fails cannot strand the visitor on
    // an error instead of the page they would have gotten.
    if (!this.config.env.formViewEnabled) {
      void this.router.navigate([USER_WORKSPACE, String(wid)], { replaceUrl: true });
      return;
    }
    this.workflowActionService.resetAsNewWorkflow();
    forkJoin({
      metadata: this.operatorMetadataService.getOperatorMetadata(),
      workflow: this.workflowPersistService.retrieveWorkflow(wid),
    })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ workflow }) => {
          // With the flag on, the form renders for any workflow: default_view only decides
          // which view a workflow lands on by default, not whether the form is reachable
          // (settled on #8011). Gating the form on default_view here would quietly reintroduce
          // a per-workflow switch -- and bounce a later PR's canvas-to-form switch straight
          // back for any canvas-default workflow.
          this.workflowName = workflow.name;
          this.storedPositions = { ...(workflow.content?.operatorPositions ?? {}) };
          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          this.workflowActionService.reloadWorkflow(workflow);
          // The workflow is shown, not edited, from here: dragging operators around or
          // deleting them belongs to the operator canvas.
          this.applyEditability();
          this.refreshSavedState();
          this.later(() => this.adjustWorkflowNameWidth(), 0);
          this.registerMetadataRefresh();
          this.registerAutoPersist();
          this.loading = false;
          this.cdr.detectChanges();
        },
        // The load can fail for many reasons (no access, a network or server error, the
        // metadata call): a neutral message covers them without claiming it was permissions.
        error: () => {
          this.notificationService.error("Unable to open this workflow.");
          void this.router.navigate([USER_WORKFLOW]);
        },
      });
  }

  /**
   * Show the workflow rather than edit it: the graph shape and its properties are read-only
   * on this page. A later PR's authoring mode makes properties editable with write access.
   */
  private applyEditability(): void {
    this.workflowActionService.disableWorkflowModification();
  }

  /**
   * Size the name field to its text, the way the operator canvas does, so what follows
   * it starts at the same place in both views instead of after a fixed-width box.
   */
  private adjustWorkflowNameWidth(): void {
    const input = this.host.nativeElement.querySelector<HTMLInputElement>("input.wf-name");
    if (!input) {
      return;
    }
    /* v8 ignore start -- font-metrics DOM measuring; jsdom has no layout */
    const probe = document.createElement("span");
    probe.style.visibility = "hidden";
    probe.style.position = "absolute";
    probe.style.whiteSpace = "pre";
    probe.style.font = getComputedStyle(input).font;
    probe.textContent = input.value || input.placeholder;
    document.body.appendChild(probe);
    input.style.width = `${Math.min(probe.offsetWidth + 20, 800)}px`;
    document.body.removeChild(probe);
    /* v8 ignore stop */
  }

  private refreshSavedState(): void {
    const lastModified = this.workflowActionService.getWorkflowMetadata()?.lastModifiedTime;
    this.autoSaveState =
      lastModified === undefined
        ? ""
        : "Saved at " +
          (this.datePipe.transform(
            lastModified,
            "MM/dd/yyyy HH:mm:ss",
            Intl.DateTimeFormat().resolvedOptions().timeZone,
            "en"
          ) ?? "");
  }

  /**
   * Renaming here is the same edit as renaming on the operator canvas: commit the name and
   * save. The title bar itself -- the read-back (normalised) name and its width -- is
   * refreshed from the metadata subscription below, the single place this page's own rename
   * and a co-editor's both flow through.
   */
  public onRenameWorkflow(): void {
    this.workflowActionService.setWorkflowName(this.workflowName);
    this.save();
  }

  /**
   * Keep the title bar in step with the workflow's metadata, exactly as the operator canvas
   * does: a rename or a save -- this page's own or a co-editor's -- refreshes the name, its
   * width, and the "Saved at ..." state from one place, so the two views never drift apart.
   */
  private registerMetadataRefresh(): void {
    this.workflowActionService
      .workflowMetaDataChanged()
      // The same 100ms the operator canvas debounces its title-bar refresh by.
      .pipe(debounceTime(100), untilDestroyed(this))
      .subscribe(() => {
        this.workflowName = this.workflowActionService.getWorkflowMetadata()?.name ?? "";
        this.later(() => this.adjustWorkflowNameWidth(), 0);
        this.refreshSavedState();
      });
  }

  /**
   * Switch to the operator canvas with a full page load, not a route. The two views share
   * root-level singletons (the graph, the Yjs shared model, the CU connection); handing
   * over in-process left the old state attached -- undraggable operators, a ghost coeditor
   * of yourself, broken runs. A fresh document is the reliable handover.
   */
  public openRegularCanvas(): void {
    this.save();
    /* v8 ignore start -- full-document navigation; jsdom cannot navigate */
    window.location.href = `${USER_WORKSPACE}/${this.wid}`;
    /* v8 ignore stop */
  }

  /**
   * Save the same way the operator canvas does. Both views edit one workflow, so the
   * form has to write through the same debounced persist -- otherwise an author's
   * setup, or a value someone filled in, would be gone on the next visit.
   */
  private registerAutoPersist(): void {
    this.workflowActionService
      .workflowChanged()
      .pipe(debounceTime(SAVE_DEBOUNCE_TIME_IN_MS), untilDestroyed(this))
      .subscribe(() => this.save());
  }

  /**
   * Save the workflow this page opened, and only that one. The persist endpoint creates a
   * workflow when the payload has no id, so saving whatever the graph holds would spawn
   * stray "Untitled workflow" rows when the page is left before its workflow loaded.
   */
  private save(): void {
    if (!this.userService.isLogin() || !this.workflowPersistService.isWorkflowPersistEnabled()) {
      return;
    }
    const workflow = this.workflowActionService.getWorkflow();
    if (workflow.wid === undefined || workflow.wid !== this.wid) {
      return;
    }
    const preserved: Workflow = {
      ...workflow,
      content: { ...workflow.content, operatorPositions: this.positionsToSave(workflow.content) },
    };
    // On the way out the subscription must NOT be tied to this component: ngOnDestroy
    // calls save(), and untilDestroyed would tear the subscription down as part of the
    // very same destroy sequence, aborting the request that was the point of the call.
    const persist = this.workflowPersistService.persistWorkflow(preserved);
    // The `destroyed` branch deliberately omits untilDestroyed (see above); the persist call
    // is a one-shot HTTP request that completes on its own, so it needs no teardown operator.
    // eslint-disable-next-line rxjs-angular/prefer-takeuntil
    (this.destroyed ? persist : persist.pipe(untilDestroyed(this))).subscribe({
      // Feed the saved workflow back, exactly as the operator canvas does: this advances
      // lastModifiedTime (and the normalised name), and the metadata subscription then repaints
      // the title bar -- so "Saved at ..." moves past the moment the page opened.
      next: updatedWorkflow => this.workflowActionService.setWorkflowMetadata(updatedWorkflow),
      // A save that fails silently is the worst thing this page can do: the author walks
      // away believing the form they just built is stored.
      error: () => this.notificationService.error("Could not save. Your latest changes are not stored yet."),
    });
  }

  /**
   * A position for every operator: the live one from the shared model (what a co-editor's drag
   * has set), else the load-time snapshot, else origin. Preferring live saves what is current
   * rather than overwriting moves with a stale copy; the fallbacks keep the guarantee that every
   * operator has a position, since loading throws on one that does not.
   */
  private positionsToSave(content: WorkflowContent): { [operatorID: string]: Point } {
    const positions: { [operatorID: string]: Point } = {};
    for (const operator of content.operators) {
      positions[operator.operatorID] = content.operatorPositions?.[operator.operatorID] ??
        this.storedPositions[operator.operatorID] ?? { x: 0, y: 0 };
    }
    return positions;
  }

  /**
   * Run after a short delay, unless the page is gone by then: the callback touches the view,
   * and detectChanges on a destroyed view throws -- reachable by navigating away while the
   * name field is still waiting to be measured.
   */
  private later(fn: () => void, delayMs = 0): void {
    setTimeout(() => {
      if (!this.destroyed) {
        fn();
      }
    }, delayMs);
  }

  /**
   * Tear down exactly what the operator canvas tears down: both views drive the same
   * singleton services, so anything left bound here follows the user to the next page
   * (the symptom was a frozen canvas after a visit -- the old shared model still attached).
   * On the way out, save once more so a last edit is not lost.
   */
  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    this.destroyed = true;
    this.save();
    this.workflowActionService.clearWorkflow();
    this.computingUnitStatusService.disconnect();
    this.executeWorkflowService.resetExecutionAndWorkers();
    this.workflowConsoleService.clearConsoleMessages();
    this.workflowResultService.clearResults();
  }
}
