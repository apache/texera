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
import { AbstractControl, FormArray, FormGroup, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { ActivatedRoute, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { UserIconComponent } from "../../../dashboard/component/user/user-icon/user-icon.component";
import { cloneDeep } from "lodash-es";
import { MarkdownService } from "ngx-markdown";
import { asapScheduler, EMPTY, forkJoin, merge, Observable, Subject, timer } from "rxjs";
import { catchError, concatMap, debounceTime, finalize, observeOn, switchMap, takeUntil, tap } from "rxjs/operators";

import { CdkDragDrop, DragDropModule } from "@angular/cdk/drag-drop";
import { USER_WORKFLOW, USER_WORKSPACE } from "../../../app-routing.constant";
import { EditableLabelWrapperComponent } from "../../../common/formly/editable-label-wrapper/editable-label-wrapper.component";
import { FormFieldBinding, Workflow, WorkflowContent } from "../../../common/type/workflow";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { ComputingUnitState } from "../../../common/type/computing-unit-connection.interface";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UserService } from "../../../common/service/user/user.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { customFormlyFieldType, CANVAS_ONLY_FORMLY_TYPES } from "../../util/custom-formly-type";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { ExecuteWorkflowService, FORM_DEBOUNCE_TIME_MS } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { FormBindingService, ResolvedField } from "../../service/form-binding/form-binding.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { ComputingUnitSelectionComponent } from "../power-button/computing-unit-selection.component";
import { PropertyEditorComponent } from "../property-editor/property-editor.component";
import { ResultTableFrameComponent } from "../result-panel/result-table-frame/result-table-frame.component";
import { VisualizationFrameContentComponent } from "../visualization-panel-content/visualization-frame-content.component";
import { WorkflowEditorComponent } from "../workflow-editor/workflow-editor.component";
import { MiniMapComponent } from "../workflow-editor/mini-map/mini-map.component";
import { CoeditorUserIconComponent } from "../menu/coeditor-user-icon/coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { SAVE_DEBOUNCE_TIME_IN_MS } from "../workspace.component";

/**
 * Input types that take a click, not text. Focusing one is not "typing", so a rebuild that arrives
 * while one has the focus loses nothing and must not be held back (see isTypingInTheForm).
 */
const NON_TEXT_INPUT_TYPES = new Set([
  "checkbox",
  "radio",
  "button",
  "submit",
  "reset",
  "range",
  "color",
  "file",
  "image",
]);

/**
 * One rendered input: the resolved binding plus the operator's own formly field for that property.
 * Building the field from the operator's JSON schema (not guessing from the value) is what gives a
 * file its picker and an attribute its column dropdown.
 */
interface RenderedField {
  resolved: ResolvedField;
  fields: FormlyFieldConfig[];
  form: FormGroup;
  model: Record<string, unknown>;
}

/** One row of the "which results to show" picker: a candidate step and whether it is currently shown. */
interface ResultChoice {
  operatorID: string;
  label: string;
  shown: boolean;
}

/**
 * The Form View: a second way to use a workflow. On top of the title-bar frame and the collapsible
 * read-only workflow preview, this PR renders the inputs an author exposed -- each as its
 * operator's own formly field, so a file property gets the real picker and an attribute a column
 * dropdown -- and writes a filled-in value straight back to its operator, the same edit the canvas
 * makes, with each sub-field of a nested or repeated property renamed and hidden as the author set
 * it up. It also shows the author's instruction above the inputs, and runs the workflow: a Run
 * button (a reader's simplified Run/Stop, sharing the canvas's disable conditions), the
 * computing-unit selector, a run clock and plain-language failure messages. It then shows results
 * underneath -- the final step's output plus the author's chosen view-result steps, each a table, a
 * visualisation, or a compact "no result yet" -- reading the canvas's view-result set and never
 * writing it. A reader can also click a step on the embedded preview to open its property panel
 * read-only: the panel writes nothing to the shared workflow and its content is inert. With write
 * access, an Edit toggle turns the page into in-place authoring: rename an input or its sub-fields,
 * hide a sub-field, reorder inputs by drag, expose a new one by clicking a step (the panel goes
 * live and its writes turn on), remove one, write the instruction, and pick which extra results to
 * feature. A view, not a new object: every graph edit goes through the same shared graph the
 * operator canvas edits, and the form-binding config is local until #8351 shares it.
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-form",
  templateUrl: "./workflow-form.component.html",
  styleUrls: ["./workflow-form.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    FormlyModule,
    DragDropModule,
    NzAvatarModule,
    NzIconModule,
    NzButtonModule,
    NzTooltipModule,
    UserIconComponent,
    ComputingUnitSelectionComponent,
    PropertyEditorComponent,
    ResultTableFrameComponent,
    VisualizationFrameContentComponent,
    WorkflowEditorComponent,
    MiniMapComponent,
    CoeditorUserIconComponent,
  ],
})
export class WorkflowFormComponent implements OnInit, OnDestroy {
  public wid?: number;
  public workflowName = "";
  public loading = true;
  /** "Saved at …", worded and formatted exactly as on the operator canvas. */
  public autoSaveState = "";
  /** Write access: only then does a filled-in value write back, and only then does the page save. */
  public canEdit = false;
  /** Edit mode: a writer authoring the form in place (rename/hide/reorder/expose/remove inputs,
   *  edit the instruction, pick results). Off, the page is the read-only form a reader sees. */
  public authoring = false;
  /** While authoring, the instruction is edited as raw markdown ("write") or shown rendered
   *  ("preview"); a reader always sees it rendered. */
  public instructionMode: "write" | "preview" = "write";
  /** The "which results to show" picker: every candidate step with its shown flag. Everyone sees it:
   *  in edit mode a toggle sets the default for all readers (saved in the config); otherwise it is
   *  the viewer's own choice for this page (viewerResultIds), never written anywhere. */
  public resultChoices: ResultChoice[] = [];
  /** A viewer's own pick of results for this page, once they have toggled anything; undefined means
   *  "the author's default". Kept for the page's lifetime only, and cleared on entering edit mode so
   *  the author edits the real default rather than their own view of it. */
  private viewerResultIds?: Set<string>;

  /** The exposed inputs, resolved against the live graph, and the formly field built for each. */
  private parameters: ResolvedField[] = [];
  public rendered: RenderedField[] = [];
  /** Torn down and replaced whenever the form is rebuilt, so an old field's write-back stops. */
  private formsRebuilt = new Subject<void>();

  /** The author's instruction, rendered above the inputs; the reader always sees it as markdown. */
  public instructionOpen = true;
  public instructionTitle = "";
  public instructionBody = "";
  public instructionPreviewHtml = "";

  /**
   * Milliseconds the current run has been going, counted from the same engine event the operator
   * canvas counts: the engine reports the real elapsed time, and a local 1s timer fills in between
   * reports so the display ticks instead of jumping.
   */
  public executionDuration = 0;
  public executionState: ExecutionState = ExecutionState.Uninitialized;
  public runError = "";
  /** The picked unit's connection state, mirrored from the same stream the operator canvas reads,
   *  so "Connecting" here means exactly what it means there. */
  public computingUnitStatus: ComputingUnitState = ComputingUnitState.NoComputingUnit;
  /** The picked unit itself, kept so Run can be gated on write access to it -- the same gate the
   *  operator canvas applies (a READ/NONE-shared unit can be viewed but not executed on). */
  private selectedUnit: DashboardWorkflowComputingUnit | null = null;
  /** Workflow validity, read from the same validation stream the operator canvas uses, so Run is
   *  disabled ("Invalid" / "Empty") in the same cases. */
  public isWorkflowValid = true;
  public isWorkflowEmpty = false;

  /** The step whose property panel is open for read-only inspection, if any. The panel shows the
   *  operator's own title, so this id is all the page needs to track. */
  public selectedOperatorId?: string;

  /**
   * Which steps' results to show: the author's saved list (`shownResultIds` in the config) or, until
   * the author has chosen, the terminal (final) steps, whose results the engine always materializes;
   * either way kept to steps that still have a result on the canvas. The form NEVER writes the
   * canvas's view-result flags -- it only decides what it itself shows, so a canvas user's
   * result-viewing is unaffected.
   */
  public shownResultIds: string[] = [];
  /** Chart height per result (0 compact / 1 default / 2 tall). Per operator so one does not resize
   *  the others, and in memory only -- a viewing preference, not part of the workflow. */
  private zoomByResult = new Map<string, number>();
  /** Bumped when a result changes, used as the chart's *ngFor identity so the frame is rebuilt, not
   *  reused: the chart reads its content once at creation, so a stale frame showed "undefined". */
  private resultVersion = new Map<string, number>();

  /** The collapsible workflow preview: closed until the reader opens it. */
  public workflowOpen = false;
  /** The embedded canvas is built the first time the strip opens, never while collapsed. */
  public workflowEverOpened = false;

  /** Set on teardown so deferred callbacks stop touching a view that is gone. */
  private destroyed = false;
  /** Save requests, each the workflow as it was when the save was asked for. Drained one at a time and
   *  in order (see registerAutoPersist), so two saves can never race each other to the backend; the
   *  last one enqueued is the latest content. */
  private persistQueue = new Subject<Workflow>();
  /** Saves enqueued and not yet finished; the queue is drained when this returns to zero. */
  private queuedSaves = 0;
  /** What to do once the queue has drained (the Canvas switch navigates then): every save enqueued up
   *  to that point, and any enqueued while waiting, has to have completed first. A failed save drops
   *  them, so nobody navigates away from changes that were never stored. */
  private afterDrain: Array<() => void> = [];
  /** An edit has happened since the last snapshot was enqueued. The autosave behind workflowChanged
   *  is debounced, so at the moment the queue drains such an edit may not be queued yet -- and the
   *  hand-over waiting on the drain would lose it to the full-page load. Set the moment an edit is
   *  reported (before the debounce), cleared when a snapshot is enqueued (it carries everything up
   *  to then), and checked by the drain, which flushes one more save instead of handing over. */
  private dirtySinceLastEnqueue = false;
  /** A rebuild of the inputs that arrived while the reader was typing, held until the typing ends. */
  private rebuildDeferred = false;
  /** Set while this page makes a presentation write, so the write's own announcement does not rebuild
   *  the form under the control that took it (see reflectLocally). */
  private reflectingLocally = false;
  /** The later rows of a repeated section, per input and sub-field path (followerKey): they show the
   *  path's name and hidden state statically and follow the first row's controls, so the rows agree
   *  without a rebuild. Rebuilt with the form (applyFieldOverrides). */
  private followersByPath = new Map<string, FormlyFieldConfig[]>();

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
    private formBindingService: FormBindingService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private notificationService: NotificationService,
    private userService: UserService,
    private markdownService: MarkdownService,
    private formlyJsonschema: FormlyJsonschema,
    private cdr: ChangeDetectorRef,
    // Injected for its side effect: it fills its map from the operator-add stream, so it has to
    // exist before the workflow loads or every operator arrives unregistered and anything asking
    // for a schema later throws. It also carries the per-instance schema (upstream column names)
    // that turns an attribute box into a dropdown.
    private dynamicSchemaService: DynamicSchemaService,
    // Injected for its side effect: it compiles on graph changes and writes column names into each
    // operator's dynamic schema. Nothing else on this page injects it, so without this line it
    // never runs and an attribute input stays a plain text box.
    private workflowCompilingService: WorkflowCompilingService,
    private computingUnitStatusService: ComputingUnitStatusService,
    private workflowConsoleService: WorkflowConsoleService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private host: ElementRef<HTMLElement>,
    private datePipe: DatePipe,
    // The result table sizes its rows-per-page from this shared panel height. On the operator
    // canvas the docked panel drives it; this page has no such panel, so left at the tiny default
    // every table showed a single row per page. Given a realistic height in ngOnInit instead.
    private panelResizeService: PanelResizeService,
    // Same source the operator canvas reads its "Invalid" / "Empty" states from, so Run is
    // disabled here exactly when it is disabled there.
    private validationWorkflowService: ValidationWorkflowService,
    private config: GuiConfigService
  ) {}

  ngOnInit(): void {
    const wid = Number(this.route.snapshot.params.id);
    if (!Number.isFinite(wid)) {
      void this.router.navigate([USER_WORKFLOW]);
      return;
    }
    this.wid = wid;
    // Give the result tables a realistic height to page against, so they show a screenful of rows
    // instead of one. (~7 rows; the card scrolls for the rest.)
    this.panelResizeService.changePanelSize(900, 560);
    // Highlighting is off by default; turning it on is what makes a click on a step select it,
    // which is how a reader opens that step's panel to inspect it (and, later, an author to expose).
    this.workflowActionService.setHighlightingEnabled(true);
    this.load(wid);

    // Selecting a step on the embedded (read-only) canvas opens its property panel read-only. The
    // canvas is not editable, but highlighting still works, so reuse it rather than teach the editor
    // a second click mode.
    this.workflowActionService
      .getJointGraphWrapper()
      .getJointOperatorHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.syncSelectionFromHighlight();
        // The panel is mounted with [actsAsEditor]="false", so opening a step here never
        // announces "currently editing this operator" on the shared co-editor channel -- a reader
        // inspecting a step is not editing the graph, and broadcasting would print the reader's own
        // name in colour over that operator on everyone else's canvas. Suppressed at the frame (the
        // only place that writes it), not here, so it cannot be re-set after this handler runs.
      });

    // Un-highlighting moves the selection just as highlighting does: clicking empty canvas drops it
    // to none, and dropping one of two shift-selected steps leaves exactly one -- which has to OPEN
    // the panel, since the highlight stream stays silent (nothing was newly highlighted). So both
    // streams run the same rule rather than each testing for its own special case.
    this.workflowActionService
      .getJointGraphWrapper()
      .getJointOperatorUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.syncSelectionFromHighlight());

    // A result changing bumps that operator's version (so its chart frame is rebuilt, not reused),
    // re-limits what the form shows to the currently-viewed set, and re-fits the visualisations.
    this.workflowResultService
      .getResultUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(update => {
        for (const operatorID of Object.keys(update ?? {})) {
          this.resultVersion.set(operatorID, (this.resultVersion.get(operatorID) ?? 0) + 1);
        }
        this.refreshShownResults();
        // markForCheck, not detectChanges: this fires often during a run, and a synchronous pass
        // can be thrown out of by an unrelated component's NG0100, killing the subscription.
        this.cdr.markForCheck();
        this.later(() => this.fitVisualisations(), 300);
      });

    // What has a result, and what the picker offers, also follows the graph's shape and names: a
    // step deleted, disabled, given a downstream link, or renamed (a co-editor's edit included)
    // changes what shows or what its pill says. Those edits arrive on their own streams; compilation
    // re-reads the config after the structural ones too, but debounced and held while the reader is
    // typing, and a rename reaches no other stream at all, so refresh the two derived collections
    // from them directly, as for the eye below. Only these collections: nothing here rebuilds the inputs.
    const graph = this.workflowActionService.getTexeraGraph();
    merge(
      graph.getOperatorAddStream(),
      graph.getOperatorDeleteStream(),
      graph.getLinkAddStream(),
      graph.getLinkDeleteStream(),
      graph.getDisabledOperatorsChangedStream(),
      graph.getOperatorDisplayNameChangedStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.refreshShownResults();
        this.rebuildResultChoices();
        this.cdr.markForCheck();
      });

    // Turning a step's view-result OFF on the canvas emits no result-update event, so the filter
    // above would miss it and leave a stale card. React to the view-result set changing directly
    // (a co-editor's toggle included), so a de-viewed step drops out here at once.
    graph
      .getViewResultOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // An eye toggled on the canvas changes both what shows (a newly-viewed step) and what the
        // author can pick, so rebuild the picker here too -- otherwise a just-eyed step would not
        // appear as an option (and an un-eyed one would linger) until the next full re-read.
        this.refreshShownResults();
        this.rebuildResultChoices();
        this.cdr.markForCheck();
      });

    // The run clock, reusing the operator canvas's source outright rather than timing anything
    // here: the engine is the only thing that knows when the run really began, so a stopwatch
    // started at the click would drift and would be wrong after a reload.
    this.workflowWebsocketService
      .subscribeToEvent("ExecutionDurationUpdateEvent")
      .pipe(
        tap(event => (this.executionDuration = event.duration)),
        switchMap(event => (event.isRunning ? timer(1000, 1000) : EMPTY)),
        untilDestroyed(this)
      )
      .subscribe(() => {
        this.executionDuration += 1000;
        this.cdr.markForCheck();
      });

    // The run button's state is read from getters, so a change in unit/connection/validity has to
    // repaint the view. markForCheck, not detectChanges: a synchronous pass can be thrown out of by
    // an unrelated component's NG0100, killing the subscription.
    this.computingUnitStatusService
      .getSelectedComputingUnit()
      .pipe(untilDestroyed(this))
      .subscribe(unit => {
        this.selectedUnit = unit;
        this.cdr.markForCheck();
      });
    this.computingUnitStatusService
      .getStatus()
      .pipe(untilDestroyed(this))
      .subscribe(status => {
        this.computingUnitStatus = status;
        this.cdr.markForCheck();
      });
    this.workflowWebsocketService
      .getConnectionStatusStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.cdr.markForCheck());
    // Validity from the canvas's own stream, so a broken graph disables Run ("Invalid") here
    // exactly as it does there.
    this.validationWorkflowService
      .getWorkflowValidationErrorStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => {
        this.isWorkflowEmpty = value.workflowEmpty;
        this.isWorkflowValid = Object.keys(value.errors).length === 0;
        this.cdr.markForCheck();
      });

    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ current }) => {
        const wasRunning = this.isRunning;
        this.executionState = current.state;
        // Reconcile the lock with the new state. The execute service flips the lock BEFORE it emits
        // the state (updateWorkflowActionLock runs first in updateExecutionState), so when a run ends
        // the clamp below still saw "running" and locked the graph again; this is where edit mode
        // gets it back. Every other state is a no-op re-application of the same rule.
        this.applyEditability();
        // Clear a stale failure banner the moment any new run starts -- this session's or a
        // co-editor's. onRun() clears it for a run started here, but a co-editor's run moves the
        // shared execution stream to an in-flight state without going through onRun(), so without
        // this the previous failure would linger over their running run.
        if (!wasRunning && this.isRunning) {
          this.runError = "";
        }
        // Surface a failed run. Without this the spinner just stops and the form gives zero
        // feedback -- the opposite of what a reader needs.
        if (current.state === ExecutionState.Failed) {
          // A required input left empty is by far the commonest reason a run fails here, and the
          // engine reports it as an opaque "... is not contained in the schema". Answer with the
          // same word the field itself already shows ("required"), so the two messages are
          // consistent -- and it covers every operator, not just this one.
          this.runError = this.hasEmptyRequiredInputs()
            ? "Run failed: please fill in the required fields."
            : this.friendlyRunError(current.errorMessages?.[0]?.message?.trim() ?? "");
        }
        // Fit the charts to their cards once a run has results. Deliberately not on run START: the
        // run repaints operators and a re-fit then zoomed the whole preview down. Deliberately does
        // not open the workflow either -- someone using the form came for the inputs and results.
        if (this.hasResults) {
          this.later(() => this.fitVisualisations(), 400);
        }
        // markForCheck, not detectChanges: this is the one subscription the page cannot afford to
        // lose. A synchronous detectChanges can be thrown out of by an unrelated component's NG0100,
        // which would kill this stream and freeze the Run button on a stale state with no error
        // shown; marking dirty and letting the next pass render avoids that.
        this.cdr.markForCheck();
      });

    // Attribute boxes become dropdowns only after compilation writes the column enums into each
    // operator's dynamic schema -- which lands after these cards were built. Rebuild on the
    // compilation-state stream, a ReplaySubject(1) so a late subscriber (this page reloads fresh
    // on every Canvas<->Form switch) gets the current state at once. Held, not dropped, while
    // someone is typing (see rebuildFormOrDefer), so it neither throws away a half-entered value
    // under the cursor nor goes missing.
    this.workflowCompilingService
      .getCompilationStateInfoChangedStream()
      .pipe(debounceTime(FORM_DEBOUNCE_TIME_MS), untilDestroyed(this))
      .subscribe(() => this.rebuildFormOrDefer(false));

    // Exposing or un-exposing a property in the panel changes the definition; the inputs above have
    // to follow at once, which is the whole point of editing them side by side. Today this fires for
    // this client's own edits; once #8351 moves formBinding into the shared model it also fires for
    // a co-editor's -- so, like the compilation path, the rebuild is held while the reader is typing
    // (a remote change would otherwise throw away a half-entered value under the cursor) and runs
    // the moment the typing ends.
    this.workflowActionService.formBindingChanged$.pipe(untilDestroyed(this)).subscribe(() => {
      // A presentation write this page just made (see reflectLocally) is already shown by the
      // control that took it; rebuilding on its own announcement would replace that control
      // mid-click and drop the focus. The announcement still reaches the autosave.
      if (this.reflectingLocally) {
        return;
      }
      this.rebuildFormOrDefer(true);
    });

    // A step renamed (in the live panel, or by a co-editor) changes the "From ..." attribution on
    // every card that belongs to it, and nothing else emits for a rename: no compilation, no config
    // change. Rebuild the inputs from it too, held while the reader is typing like the other two.
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.rebuildFormOrDefer(true));
  }

  /**
   * Rebuild the inputs from the config now or, while the reader is typing, hold the rebuild until
   * the focus leaves the text control (onFocusOut). Held rather than dropped: the change that asked
   * for it (a property exposed in the panel, a schema compiled) still has to reach the page, only
   * not under the cursor. Dropping it left an exposed property's card missing until something else
   * happened to rebuild, which read as the tick box doing nothing.
   */
  private rebuildFormOrDefer(detect: boolean): void {
    if (this.isTypingInTheForm()) {
      this.rebuildDeferred = true;
      return;
    }
    this.rebuildDeferred = false;
    this.readConfig();
    if (detect) {
      this.cdr.detectChanges();
    }
  }

  /**
   * focusout fires before the next element takes the focus, so the held rebuild is decided after
   * the current tick: a reader who merely tabbed to another text field keeps it held, anyone else
   * gets it now. The hold is re-checked when that tick fires: the very click that took the focus
   * can be a control whose own change rebuilds at once (the expose tick box), clearing the hold in
   * between, and a stale callback that rebuilt regardless would only rebuild the same cards twice.
   */
  @HostListener("focusout")
  public onFocusOut(): void {
    if (this.rebuildDeferred) {
      this.later(() => {
        if (this.rebuildDeferred) {
          this.rebuildFormOrDefer(true);
        }
      }, 0);
    }
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
          this.canEdit = !workflow.readonly;
          this.workflowActionService.setNewSharedModel(wid, this.userService.getCurrentUser());
          this.workflowActionService.reloadWorkflow(workflow);
          // The workflow is shown, not edited, from here: dragging operators around or
          // deleting them belongs to the operator canvas. Lock now, and keep it locked against
          // anything else that unlocks the graph (clampEditability).
          this.applyEditability();
          this.clampEditability();
          this.refreshSavedState();
          this.later(() => this.adjustWorkflowNameWidth(), 0);
          this.readConfig();
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
   * Whether this page may hold the graph unlocked: a writer in edit mode, and no run in flight. The
   * run rule is the canvas's own (a run locks the graph until it ends), kept here too so that entering
   * edit mode mid-run cannot undo it. Every other state is locked, so a reader, or a writer merely
   * viewing, cannot change the workflow through this page.
   */
  private mayUnlock(): boolean {
    return this.authoring && this.canEdit && !this.isRunning;
  }

  /**
   * Whether the step panel is live (acting as an editor, tick boxes on, not inert): the same rule as
   * the lock, not edit mode alone. The property frame does not consult the lock before its own
   * writes -- it syncs the operator version on mount, writes the schema defaults ajv fills in, and
   * publishes the "currently editing" marker whenever it acts as an editor -- so a step selected
   * while a run is in flight must mount read-only even in edit mode, and turns live when the run ends.
   */
  public get panelLive(): boolean {
    return this.mayUnlock();
  }

  /** Lock or unlock editing from the current state (see mayUnlock); the one reducer for both directions. */
  private applyEditability(): void {
    if (this.mayUnlock()) {
      this.workflowActionService.enableWorkflowModification();
    } else {
      this.workflowActionService.disableWorkflowModification();
    }
  }

  /**
   * The lock is a root-level flag with writers that know nothing of this page: the execute service
   * unlocks it whenever a run ends (completed, failed, killed, reset), the computing-unit selector
   * unlocks it when it finds no run on the chosen unit, and any future caller may too. Rather than
   * chase each one, clamp at the one place they all report to: whenever the flag turns on while this
   * page must stay locked, turn it off again. In edit mode the canvas rule then stands unchanged:
   * locked while a run is in flight, unlocked once it ends -- the execute service flips the lock
   * before it emits the new state, so that unlock is clamped here (this page still sees "running")
   * and given back by the execution-state handler, which re-applies the rule with the new state.
   *
   * The clamp runs a microtask later (observeOn), never inside the unlocking call: enableWorkflow-
   * Modification emits the flag and only then enables undo/redo, and the stream still has to reach
   * its other subscribers. A disable nested inside that emission would leave undo/redo enabled and
   * later subscribers ending on the stale "true"; run after the call has finished, the disable is the
   * last word and every consumer sees the same locked state. Nothing a user does fits in that gap.
   */
  private clampEditability(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(observeOn(asapScheduler), untilDestroyed(this))
      .subscribe(enabled => {
        if (enabled && !this.mayUnlock()) {
          this.workflowActionService.disableWorkflowModification();
        }
      });
  }

  // ---------------------------------------------------------------------------
  // Inputs: the exposed properties, rendered as their operators' own fields
  // ---------------------------------------------------------------------------

  /**
   * Whether the reader is mid-way through typing somewhere on this page: the caret is in a control
   * that holds text (a text-like input, a textarea, a select, a content-editable). A tick box, radio
   * or button also takes the focus when clicked but holds no half-entered value, so it is not typing
   * -- a tick box (the step panel's expose boxes, once that panel is live for authoring) is precisely
   * the click that has to rebuild the cards at once, and counting it as typing held that rebuild back.
   */
  private isTypingInTheForm(): boolean {
    const active = document.activeElement as HTMLElement | null;
    if (!active || !this.host.nativeElement.contains(active)) {
      return false;
    }
    if (active.tagName === "INPUT") {
      return !NON_TEXT_INPUT_TYPES.has((active as HTMLInputElement).type);
    }
    return ["TEXTAREA", "SELECT"].includes(active.tagName) || active.isContentEditable;
  }

  /**
   * The steps a result can come from. The engine compiles only the enabled operators, so a disabled
   * step produces nothing however it is flagged: not offered in the picker, not shown as a result.
   */
  private enabledOperators(): OperatorPredicate[] {
    return this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .filter(op => !(op.isDisabled ?? false));
  }

  /**
   * The picker's candidates: every final step (the engine always materialises it) and every
   * intermediate step with view-result ("the eye") on the canvas. In edit mode a step already on the
   * author's saved list is kept as well, eye or no eye, so the author can take a stale choice off the
   * list; a reader is not offered it, since with no result materialised its pill could never turn on.
   * Reuse the one terminal rule (terminalOperatorIds) rather than a second copy. Disabled steps are
   * not offered at all, listed or not: they are left out of the run, so choosing one could never show
   * anyone anything. The shown flag mirrors shownResultIds, so it reflects the viewer's own choice when
   * they have made one and the author's default otherwise.
   */
  private rebuildResultChoices(): void {
    const config = this.formBindingService.getConfig();
    const viewed = this.workflowActionService.getTexeraGraph().getOperatorsToViewResult();
    const listed = new Set(this.authoring && this.canEdit ? config.shownResultIds ?? [] : []);
    const terminals = new Set(this.terminalOperatorIds());
    const shown = new Set(this.shownResultIds);
    this.resultChoices = this.enabledOperators()
      .filter(op => terminals.has(op.operatorID) || viewed.has(op.operatorID) || listed.has(op.operatorID))
      .map(op => ({
        operatorID: op.operatorID,
        label: this.formBindingService.operatorLabel(op),
        shown: shown.has(op.operatorID),
      }));
  }

  /**
   * Re-read the saved form config and rebuild everything derived from it. An input whose operator
   * has since been deleted is NOT dropped here: resolveFields marks it broken, a reader never sees
   * it (visibleFields), and an author sees it as an empty card with the reason and removes it
   * explicitly. Deleting it silently on entering edit mode would be a config write nobody asked for,
   * and would leave the author guessing where an input went.
   */
  private readConfig(): void {
    const config = this.formBindingService.getConfig();
    this.parameters = this.formBindingService.resolveFields();
    this.instructionTitle = config.instruction?.title ?? "";
    this.instructionBody = config.instruction?.body ?? "";
    this.refreshShownResults();
    this.rebuildResultChoices();
    // Readers always see the instruction rendered; an author sees it rendered only while previewing
    // (otherwise they are editing the raw markdown in the textarea).
    if (!this.authoring || this.instructionMode === "preview") {
      void this.renderInstruction();
    }
    this.buildForm();
  }

  /**
   * Decide which operators' result cards to show. The engine materializes a result for every terminal
   * operator (no enabled downstream) as well as every view-result operator, so a terminal's result is
   * always available. The author's default is the saved list (shownResultIds: exactly those steps, an
   * empty list meaning none) or, until the author has chosen, every terminal step. A viewer who has
   * toggled the picker on this page sees their own set instead (viewerResultIds), which is never
   * written anywhere. Either way this is a pure display filter that reads the graph and never writes
   * it, so a normal canvas user's result-viewing is unaffected. A step that is neither viewed nor
   * terminal (or was deleted) drops out rather than rendering a stale card.
   */
  private refreshShownResults(): void {
    const graph = this.workflowActionService.getTexeraGraph();
    const viewed = graph.getOperatorsToViewResult();
    // A result exists for an operator that is view-result (the eye) or terminal (no enabled
    // downstream); the engine materializes both (WorkflowCompiler stores terminal operators plus
    // opsToViewResult) -- but only for enabled operators, since a disabled one is left out of the
    // compiled plan. A terminal id comes from terminalOperatorIds (enabled operators only) already; a
    // view-result id keeps its eye while disabled, so it is checked against the enabled set here.
    const terminals = new Set(this.terminalOperatorIds());
    const disabled = new Set(
      graph
        .getAllOperators()
        .filter(op => op.isDisabled ?? false)
        .map(op => op.operatorID)
    );
    const availableOnCanvas = (id: string): boolean => (viewed.has(id) && !disabled.has(id)) || terminals.has(id);
    // The author's default: the saved list when there is one, otherwise the final steps. The
    // downstream hasNonEmptyResult filter drops steps that produced no data.
    const authorsDefault = this.formBindingService.getConfig().shownResultIds ?? [...terminals];
    const wanted = this.viewerResultIds ? [...this.viewerResultIds] : authorsDefault;
    this.shownResultIds = [...new Set(wanted)].filter(availableOnCanvas);
  }

  /** The workflow's terminal operators: enabled operators with no enabled downstream link. Matches the
   *  backend's storage rule (WorkflowCompiler treats out-degree-0 operators of the enabled plan as
   *  terminal and always materializes them), so a disabled link or operator does not mislead this. */
  private terminalOperatorIds(): string[] {
    const graph = this.workflowActionService.getTexeraGraph();
    const hasEnabledDownstream = new Set(graph.getAllEnabledLinks().map(link => link.source.operatorID));
    return graph
      .getAllOperators()
      .filter(op => !(op.isDisabled ?? false) && !hasEnabledDownstream.has(op.operatorID))
      .map(op => op.operatorID);
  }

  /**
   * Build the form from the operators' JSON schemas (FormlyJsonschema), keeping the one field per
   * exposed property. Each input gets its own form keyed by binding id.
   */
  private buildForm(): void {
    this.formsRebuilt.next();
    this.rendered = this.visibleFields
      .map(field => this.renderField(field))
      .filter((r): r is RenderedField => r !== undefined);
  }

  private renderField(resolved: ResolvedField): RenderedField | undefined {
    const { binding } = resolved;
    // A broken input (its operator gone) has no schema to build a field from. Only an author ever
    // sees it (visibleFields drops it for readers), rendered as an empty card so the author can
    // remove it; a reader never reaches here for one.
    if (resolved.brokenReason) {
      return { resolved, fields: [], form: new FormGroup({}), model: {} };
    }
    const schema = this.operatorSchemaFor(binding.operatorID);
    if (!schema) {
      return undefined;
    }
    const operator = this.workflowActionService.getTexeraGraph().getOperator(binding.operatorID);
    const operatorType = operator?.operatorType;
    const full = this.formlyJsonschema.toFieldConfig(cloneDeep(schema) as never, {
      map: (mapped, source) => {
        // Render the exact custom widget the operator property panel would (file/model/dataset
        // pickers, image/audio uploaders, ...), shared via customFormlyFieldType so an exposed
        // property shows its real control instead of degrading to a text box.
        const customType = customFormlyFieldType({
          key: mapped.key,
          operatorType,
          description: (source as { description?: string })?.description,
          currentType: mapped.type,
        });
        // Canvas-only widgets (code editor, drag-reorder) do not work here; an older workflow may
        // already carry one, so leave it to formly's default editable control rather than a widget
        // that cannot function on a form.
        if (customType && !CANVAS_ONLY_FORMLY_TYPES.has(customType)) {
          mapped.type = customType;
        }
        return mapped;
      },
    });
    const source = (full.fieldGroup ?? []).find(child => child.key === binding.propertyKey);
    if (!source) {
      return undefined;
    }

    const field = cloneDeep(source);
    // The schema's own title ("Attributes", "Limit", "File") -- the reader's title when unnamed.
    // Falls back to this, not the lower-camel key ("fileName"), which would read inconsistently.
    const schemaLabel = (source.props?.label as string) || binding.propertyKey;
    field.key = binding.id;
    field.props = {
      ...(field.props ?? {}),
      label: binding.displayName || schemaLabel,
    };

    const form = new FormGroup({});
    // Seed the model with the operator's other properties as read-only context, not just this
    // input's own value: some custom widgets read a sibling to decide what to show -- the
    // HuggingFace model picker reads `task` to load the right models and label the field. Only
    // this binding's value is ever written back (see below); the context is never persisted, and
    // it is cloned so a widget that mutates it cannot reach through to the real operator.
    const model: Record<string, unknown> = {
      ...cloneDeep(operator?.operatorProperties ?? {}),
      [binding.id]: cloneDeep(resolved.value),
    };
    if (this.canEdit) {
      form.valueChanges
        .pipe(debounceTime(FORM_DEBOUNCE_TIME_MS), takeUntil(this.formsRebuilt), untilDestroyed(this))
        .subscribe(() => {
          // Formly emits the schema's empty default while building the control, before any edit;
          // writing that back silently wiped the operator's real value (both views edit one
          // workflow). So only accept a dirtied form, or a value that differs from the operator's
          // without being emptier (some controls set values without marking dirty).
          const next = model[binding.id];
          const current = this.formBindingService.readValue(binding.operatorID, binding.propertyKey);
          const isEmpty = (v: unknown) => v === undefined || v === null || v === "";
          const unchanged = JSON.stringify(next ?? null) === JSON.stringify(current ?? null);
          if (unchanged || (!form.dirty && isEmpty(next) && !isEmpty(current))) {
            return;
          }
          // Write straight onto the operator (the same edit the canvas makes) and refresh this
          // card's snapshot, which the template reads.
          this.formBindingService.writeValue(binding, next);
          this.parameters = this.formBindingService.resolveFields();
          const refreshed = this.parameters.find(p => p.binding.id === binding.id);
          const card = this.rendered.find(r => r.resolved.binding.id === binding.id);
          if (refreshed && card) {
            card.resolved = refreshed;
          }
          this.cdr.detectChanges();
        });
    } else {
      // A read-only viewer sees the author's values and can run with them, but cannot change them.
      // Disable at the field level, not with form.disable(): formly builds its controls into the
      // form after this, and a FormGroup disabled while still empty does not disable controls added
      // later (it re-enables itself), so the input stayed editable. props.disabled is what formly
      // honours, and it cascades to a nested property's sub-fields. No write-back is wired either.
      field.props = { ...(field.props ?? {}), disabled: true };
    }

    this.applyFieldOverrides(field, binding, schemaLabel);
    return { resolved, fields: [field], form, model };
  }

  /**
   * The template for one row of a repeated section. formly's `fieldArray` may be the template
   * object or a function that builds one per row; resolve both so an array property's sub-fields
   * are reachable (treating the function case as a leaf hid them). @internal, exported for tests.
   */
  public static arrayItemOf(node: FormlyFieldConfig): FormlyFieldConfig | undefined {
    const fa = node.fieldArray;
    if (!fa) {
      return undefined;
    }
    if (typeof fa !== "function") {
      return fa;
    }
    try {
      return fa(node);
    } catch {
      // A builder that needs more context than we can give it tells us nothing about the row's
      // shape; better to list no sub-fields than to guess at them.
      return undefined;
    }
  }

  /**
   * The override path for a child field: the parent path joined with the child's key, but array
   * indices are dropped so one override entry covers every row of a repeated section. @internal,
   * exported for tests.
   */
  public static childPath(parent: string, key: unknown): string {
    if (typeof key !== "string" || key === "" || /^\d+$/.test(key)) {
      return parent;
    }
    return parent ? parent + "." + key : key;
  }

  /**
   * Walk the field and its sub-fields, dropping the operator schema's own per-field descriptions
   * (author notes about the operator, not guidance to a form reader) and applying the author's
   * stored per-sub-field overrides (rename, hide), keyed by field path. A repeated section builds
   * its row template on demand, so its builder is wrapped to decorate every row formly ever makes.
   *
   * One set of controls per path: a repeated section's rows all share one override, and a name box
   * and an eye on every row would be that many copies of one control, none following the others.
   * The first row this walk meets for a path carries the controls; later rows show the same name
   * and hidden state statically and follow the controls' edits (followersByPath), so the rows agree
   * without a rebuild. A scalar array's rows are walked as rows, never as the input's root, so the
   * input's own title box appears once, above them.
   */
  private applyFieldOverrides(field: FormlyFieldConfig, binding: FormFieldBinding, schemaLabel: string): void {
    for (const key of [...this.followersByPath.keys()]) {
      if (key.startsWith(binding.id + "/")) {
        this.followersByPath.delete(key);
      }
    }
    const controlsAt = new Set<string>();
    const walk = (node: FormlyFieldConfig, path: string, root: boolean): void => {
      // Drop the schema's own description on every field, nested ones included: on this page the
      // one piece of guidance is the help text the form's author writes, rendered once by the card.
      node.props = { ...(node.props ?? {}), description: "" };
      if (root && this.authoring) {
        // Author mode, the input itself: its name is renamed in place by clicking the title, like
        // every nested field. No eye here -- a whole input leaves via Remove, not a hide toggle. The
        // editable label becomes the single title, so formly's own label is cleared to avoid printing
        // it twice. A repeated input has no labelable control of its own, so its title names it as a
        // group instead.
        EditableLabelWrapperComponent.decorate(
          node,
          {
            authoring: true,
            name: binding.displayName ?? "",
            hidden: false,
            fallback: schemaLabel,
            canHide: false,
            group: node.type === "array",
          },
          name => this.onBindingNamed(binding.id, name)
        );
        node.props = { ...(node.props ?? {}), label: "" };
      } else if (root && node.type === "array") {
        // Reader mode, a repeated input: the shared array widget prints its label at the BOTTOM,
        // beside its add button (the canvas panel's convention), while every other widget and the
        // author's editable title sit above. Left alone, the title would jump from above the rows
        // in edit mode to below them on Done. Give it the same static title above instead; the
        // wrapper blanks the widget's own label and names the rows as a group.
        EditableLabelWrapperComponent.decorate(node, {
          authoring: false,
          name: binding.displayName ?? "",
          hidden: false,
          fallback: schemaLabel,
          canHide: false,
          group: true,
        });
      }
      // Apply the author's stored overrides so a reader sees each sub-field renamed and hidden as
      // set up. The root (path "") carries the binding's own displayName, set in renderField.
      if (path) {
        const override = binding.overrides?.[path] ?? {};
        // The schema's own label, read BEFORE the override replaces it: it is the name box's
        // placeholder and tooltip, and what clearing the box yields, since an empty name deletes the
        // override (setFieldOverride).
        const schemaOwnLabel = (node.props?.label as string) || path;
        if (override.displayName) {
          node.props = { ...(node.props ?? {}), label: override.displayName };
        }
        if (this.authoring) {
          // An author edits the sub-field's label where it appears and keeps hidden fields on
          // screen (faded, via the wrapper) so they can be brought back, rather than removed from
          // the DOM as they are for a reader. The first node met for a path carries the controls;
          // any later row of a repeated section shows the same state and follows the controls.
          const carriesControls = !controlsAt.has(path);
          controlsAt.add(path);
          EditableLabelWrapperComponent.decorate(
            node,
            {
              authoring: carriesControls,
              name: override.displayName ?? "",
              hidden: override.hidden === true,
              fallback: schemaOwnLabel,
              group: node.type === "array",
            },
            carriesControls ? name => this.onSubFieldNamed(binding.id, path, name) : undefined,
            carriesControls ? hidden => this.onSubFieldHiddenAt(binding.id, path, hidden) : undefined
          );
          if (!carriesControls) {
            const key = WorkflowFormComponent.followerKey(binding.id, path);
            this.followersByPath.set(key, [...(this.followersByPath.get(key) ?? []), node]);
          }
        } else if (override.hidden) {
          node.hide = true;
          // Hidden means "not shown", not "cleared". Formly 7's resetFieldOnHide extra defaults to
          // true, so a field that renders hidden has its value stripped from the model -- and this
          // card writes the whole nested object back, so that strip would delete the author's pinned
          // value for the hidden sub-field the moment a writer opens the form. Opt this field out so
          // its value survives, matching FormFieldOverride.hidden's contract (the value still
          // applies; it is only hidden).
          node.resetOnHide = false;
        }
      }
      // A repeated section may build its row template on demand, once per row. Decorating the
      // object it returns is pointless -- the next row gets a fresh one. Wrap the builder instead,
      // so every row formly ever creates comes out decorated.
      if (typeof node.fieldArray === "function") {
        const build = node.fieldArray;
        node.fieldArray = (f: FormlyFieldConfig) => {
          const row = build(f);
          // Walk what is INSIDE each row, never the row container itself: the container carries the
          // array property's own name, so decorating it as a root (path "") printed the group title
          // a second time above the rows. Its sub-fields keep their own key paths, the same ones
          // their overrides are stored under.
          const children = row.fieldGroup ?? [];
          if (children.length === 0) {
            // A scalar array (a list of strings): the builder returns a leaf row with no sub-fields,
            // so decorate the row itself -- as a row, not as the input's root, or the input's title
            // box would appear on every row under the one already at the top.
            walk(row, path, false);
          } else {
            // An object row: not walked as a root (that reprints the array's group title), but its
            // own schema description (the items.description) still renders once per row via the
            // field wrapper's nzExtra, so drop just that -- the description-removal the walk does for
            // every other field, minus the title-reprinting root treatment.
            row.props = { ...(row.props ?? {}), description: "" };
          }
          for (const child of children) {
            walk(child, WorkflowFormComponent.childPath(path, child.key), false);
          }
          return row;
        };
        return;
      }
      const arrayItem = WorkflowFormComponent.arrayItemOf(node);
      const children = node.fieldGroup ?? arrayItem?.fieldGroup ?? [];
      for (const child of children) {
        walk(child, WorkflowFormComponent.childPath(path, child.key), false);
      }
      // A scalar array (e.g. a list of strings) has a row template with no sub-fields of its own;
      // decorate it directly (as a row, see above) so its schema description is dropped like every
      // other field's.
      if (arrayItem && !arrayItem.fieldGroup) {
        walk(arrayItem, path, false);
      } else if (arrayItem) {
        // A static object-array template: its sub-fields are walked above, but the template
        // container's own items.description still renders once per row, so drop just that (not
        // walking it as a root, which would reprint the array's group title).
        arrayItem.props = { ...(arrayItem.props ?? {}), description: "" };
      }
    };
    walk(field, "", true);
  }

  /** Followers are kept per input and path; "/" cannot occur in a binding id (a uuid). */
  private static followerKey(bindingId: string, path: string): string {
    return bindingId + "/" + path;
  }

  private operatorSchemaFor(operatorID: string): object | undefined {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(operatorID)) {
      return undefined;
    }
    try {
      // Prefer the per-instance schema: it carries the upstream column names, so an attribute
      // picker renders as a dropdown of real columns rather than a text box.
      return this.dynamicSchemaService.getDynamicSchema(operatorID).jsonSchema;
    } catch {
      try {
        return this.operatorMetadataService.getOperatorSchema(graph.getOperator(operatorID).operatorType).jsonSchema;
      } catch {
        return undefined;
      }
    }
  }

  /**
   * The inputs a reader is offered. Broken bindings (the operator was deleted, or the property key
   * no longer exists) are left out, since filling one in could not affect a run; an author sees them
   * (below), to repair or remove them.
   */
  public get visibleFields(): ResolvedField[] {
    // A reader never sees a broken input (its operator is gone, so filling it could not affect the
    // run); an author sees it, to repair or remove it.
    return this.authoring ? this.parameters : this.parameters.filter(field => !field.brokenReason);
  }

  public trackByRendered(_: number, rendered: RenderedField): string {
    return rendered.resolved.binding.id;
  }

  /** A pill per step: a toggle rebuilds the choices as new objects, and the pressed pill must stay the
   *  same element or the keyboard focus on it is lost. */
  public trackByChoice(_: number, choice: ResultChoice): string {
    return choice.operatorID;
  }

  // ---------------------------------------------------------------------------
  // Results: the final step's output plus the chosen steps', shown under the workflow that produced it
  // ---------------------------------------------------------------------------

  public get hasResults(): boolean {
    return this.shownResultIds.some(id => this.workflowResultService.hasNonEmptyResult(id));
  }

  /**
   * The shown steps (terminal plus chosen) that actually produced a result, so only those get a
   * card. Whether a Python UDF yields a result cannot be known from the graph -- some (a
   * download/publish step) never do -- so a step earns its card at runtime rather than sitting on a
   * permanent "No result yet.".
   */
  public get resultIdsToShow(): string[] {
    return this.shownResultIds.filter(id => this.workflowResultService.hasNonEmptyResult(id));
  }

  public isTabularResult(operatorID: string): boolean {
    return this.workflowResultService.hasPaginatedResult(operatorID);
  }

  /**
   * Whether this step's visualisation drew something. A visualiser reserves a fixed canvas even
   * when empty, so gating on real content lets an empty result collapse to the compact "No result
   * yet" line instead of a tall blank box. Tables are excluded (they take the tabular branch).
   */
  public vizHasContent(operatorID: string): boolean {
    if (this.isTabularResult(operatorID)) {
      return false;
    }
    const snapshot = this.workflowResultService.getResultService(operatorID)?.getCurrentResultSnapshot();
    return !!snapshot && snapshot.length > 0;
  }

  /** The operator's friendly label for a result card, falling back to its raw id. */
  public resultLabel(operatorID: string): string {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    return operator ? this.formBindingService.operatorLabel(operator) : operatorID;
  }

  public trackByKey(_: number, key: string): string {
    return key;
  }

  /**
   * A per-result identity that changes on each result-update for that operator, used as the chart's
   * *ngFor key so the frame is rebuilt (not reused) when the result changes: the chart reads its
   * content once at creation, so a reused frame kept showing the old (or "undefined") picture. A
   * visualisation's result arrives as a single snapshot, so in practice this bumps about once per
   * result rather than per tuple.
   */
  public resultKey(operatorID: string): string {
    return operatorID + "#" + (this.resultVersion.get(operatorID) ?? 0);
  }

  public resultZoom(operatorID: string): number {
    return this.zoomByResult.get(operatorID) ?? 1;
  }

  public zoomResult(operatorID: string, delta: number): void {
    const next = Math.min(2, Math.max(0, this.resultZoom(operatorID) + delta));
    this.zoomByResult.set(operatorID, next);
    // Let the new card height land, then have the chart redraw into it -- growing the frame alone
    // leaves the picture at its old size until something asks it to re-measure.
    this.cdr.detectChanges();
    this.later(() => this.fitVisualisations(), 60);
  }

  /**
   * Scale each visualisation to its card. They render in a same-origin srcdoc iframe at natural
   * size, so we inject a stylesheet to fit the content to the card and fire a resize so chart
   * libraries re-lay out. The operator's output is untouched.
   */
  /* v8 ignore start -- iframe/Plotly DOM fitting; no coverage in jsdom */
  private fitVisualisations(): void {
    const frames = this.host.nativeElement.querySelectorAll<HTMLIFrameElement>(".result-body iframe");
    frames.forEach(frame => {
      const apply = () => {
        try {
          const doc = frame.contentDocument;
          if (!doc?.body) {
            return;
          }
          if (!doc.getElementById("pc-fit")) {
            const style = doc.createElement("style");
            style.id = "pc-fit";
            style.textContent = `
              html, body { margin: 0; padding: 8px; overflow-x: hidden; }
              .js-plotly-plot, .plot-container, .plotly, .svg-container { width: 100% !important; height: 100% !important; }
              img, svg, canvas, video { max-width: 100% !important; height: auto !important; }
              table { max-width: 100%; }
            `;
            doc.head?.appendChild(style);
          }
          const win = frame.contentWindow as (Window & { Plotly?: any }) | null;
          const plots = doc.querySelectorAll<HTMLElement>(".js-plotly-plot");
          if (win?.Plotly?.Plots?.resize && plots.length) {
            plots.forEach(plot => {
              plot.style.width = "100%";
              plot.style.height = "100%";
              try {
                win.Plotly.Plots.resize(plot);
              } catch {
                // A chart mid-render cannot be resized; the next call will catch it.
              }
            });
          }
          win?.dispatchEvent(new Event("resize"));
        } catch {
          // A cross-origin document cannot be styled from here; leave it as it came.
        }
      };
      apply();
      // Re-apply after the iframe (re)loads. Bind once per frame element (guarded by a data flag):
      // a `{ once: true }` listener added on every fit call never fires for an already-loaded frame,
      // so repeated zoom/fit calls would pile up detached listeners. A single persistent listener
      // per frame re-fits on each reload and is torn down with the frame.
      if (!frame.dataset.pcFitBound) {
        frame.dataset.pcFitBound = "1";
        frame.addEventListener("load", apply);
      }
    });
  }
  /* v8 ignore stop */

  // ---------------------------------------------------------------------------
  // Instruction: the author's one piece of guidance, shown as rendered markdown
  // ---------------------------------------------------------------------------

  public get hasInstruction(): boolean {
    return this.instructionBody.trim().length > 0;
  }

  private async renderInstruction(): Promise<void> {
    // Capture the body this render is for: parsing can resolve on a later microtask, and a fresh
    // readConfig() may start another render meanwhile. If the configured body changed while we were
    // parsing, this result is stale -- drop it so the newer render's output stands.
    const body = this.instructionBody;
    const html = body.trim() ? await Promise.resolve(this.markdownService.parse(body)) : "";
    if (body !== this.instructionBody) {
      return;
    }
    this.instructionPreviewHtml = html;
    this.cdr.detectChanges();
  }

  public toggleInstruction(): void {
    this.instructionOpen = !this.instructionOpen;
  }

  // ---------------------------------------------------------------------------
  // Author mode: editing the form in place (write access only). Graph edits go through the same
  // shared graph the operator canvas edits; form-binding edits go through the form-binding config
  // (local until #8351 shares it). Each edit then re-reads the config.
  // ---------------------------------------------------------------------------

  public toggleAuthoring(): void {
    // Entering edit mode needs write access. The Edit button is only rendered for a writer, but the
    // guard belongs here, at the method, so no other caller can put a reader into a mode whose every
    // action writes the shared config. Leaving edit mode is always allowed.
    if (!this.authoring && !this.canEdit) {
      return;
    }
    if (this.authoring) {
      // Leaving: dismiss the step panel BEFORE the frame stops being an editor. The property editor
      // clears the "currently editing" marker it published only while it acts as one (the same gate as
      // the publish), and a remount as a viewer clears nothing -- co-editors would keep seeing this
      // session as editing the step after Done.
      this.closeOperatorPanel();
    }
    this.authoring = !this.authoring;
    if (this.authoring) {
      // An author picks fields off the workflow, so show it.
      this.showWorkflow();
      // In edit mode the picker sets the default for everyone, so the author's own view of the
      // results (if they toggled any as a viewer) gives way to that default.
      this.viewerResultIds = undefined;
    } else {
      this.workflowOpen = false;
    }
    // Edit mode is what makes operator properties (and the embedded canvas) editable here.
    this.applyEditability();
    this.readConfig();
  }

  /**
   * Only presentation is editable here (the input's shown name and its help text). Which operator
   * property an input drives is decided by ticking it in the property panel, so there is nothing to
   * type and no way to point an input at a property that does not exist.
   */
  public onEditHelpText(resolved: ResolvedField, value: string): void {
    // Help text is presentation only and does not change which inputs the form has, so it is NOT
    // followed by readConfig: rebuilding the whole form on every keystroke would churn every card
    // (heavy file/model widgets included) and jump the cursor. Like the instruction, it is saved to
    // the config and reflected on the next full re-read. (A binding's shown name is edited through
    // the editable title, not here -- see onBindingNamed.)
    this.reflectLocally(() => this.formBindingService.updateBinding(resolved.binding.id, { helpText: value }));
  }

  /**
   * Take an input off the form. The card goes with it, so the keyboard focus that was on its Remove
   * button is handed to the next card's Remove (else the previous card's, else the Inputs heading)
   * once the list has rebuilt; dropped focus would send a keyboard author back to the top of the page.
   */
  public onRemoveBinding(resolved: ResolvedField): void {
    const at = this.rendered.findIndex(card => card.resolved.binding.id === resolved.binding.id);
    const neighbour = this.rendered[at + 1] ?? this.rendered[at - 1];
    // Re-read once, here (the write's own announcement would rebuild a second time), so the focus
    // hand-off below lands on the one rebuilt list.
    this.reflectLocally(() => this.formBindingService.removeBinding(resolved.binding.id));
    this.readConfig();
    this.later(() => this.focusAfterRemoval(neighbour?.resolved.binding.id), 0);
  }

  private focusAfterRemoval(neighbourId: string | undefined): void {
    const host: HTMLElement = this.host.nativeElement;
    const target =
      (neighbourId ? host.querySelector<HTMLElement>(`.remove[data-binding="${neighbourId}"]`) : null) ??
      host.querySelector<HTMLElement>(".pc-section-head .label");
    target?.focus();
  }

  /** The name a card's controls are announced with: the author's name for the input, else its key. */
  public cardName(card: RenderedField): string {
    return card.resolved.binding.displayName || card.resolved.binding.propertyKey;
  }

  public onDrop(event: CdkDragDrop<unknown>): void {
    this.moveRenderedCard(event.previousIndex, event.currentIndex);
  }

  /**
   * Keyboard counterpart of the drag: the Move up / Move down buttons on an author's card step it
   * one place. CDK drag-drop offers no keyboard path of its own and the drag handle is decorative,
   * so without these a keyboard-only author could not reorder at all.
   */
  public onMoveBinding(card: RenderedField, delta: -1 | 1): void {
    const at = this.rendered.indexOf(card);
    this.moveRenderedCard(at, at + delta);
  }

  /**
   * Move the card at one rendered position onto another. The positions are indices into `rendered`,
   * which can be shorter than the saved fields (a binding whose operator is live but whose schema is
   * momentarily unavailable renders no card), so reordering the saved fields by those raw indices
   * could move the wrong one. Translate both ends to the saved field they name, by binding id, and
   * reorder those. A position off either end, or a card the config no longer holds, moves nothing.
   */
  private moveRenderedCard(fromIndex: number, toIndex: number): void {
    const fields = this.formBindingService.getConfig().fields;
    const movedId = this.rendered[fromIndex]?.resolved.binding.id;
    const targetId = this.rendered[toIndex]?.resolved.binding.id;
    const from = fields.findIndex(f => f.id === movedId);
    const to = fields.findIndex(f => f.id === targetId);
    if (from === -1 || to === -1) {
      return;
    }
    // Re-read once, here: the write's own announcement would rebuild the form a second time.
    this.reflectLocally(() => this.formBindingService.reorder(from, to));
    this.readConfig();
  }

  public onInstructionChange(): void {
    this.formBindingService.updateConfig({
      instruction: { title: this.instructionTitle, body: this.instructionBody },
    });
  }

  public setInstructionMode(mode: "write" | "preview"): void {
    this.instructionMode = mode;
    if (mode === "preview") {
      void this.renderInstruction();
    }
  }

  /**
   * A pill in the picker. In edit mode this sets the default everyone sees: the step joins or leaves
   * the saved list (which the first choice starts from the final steps, the default until then), and
   * the config is re-read. Anyone else, a writer merely viewing included, only changes their own view
   * of this page: nothing is written, so a reader without write access can choose too.
   */
  public onToggleResult(choice: ResultChoice): void {
    if (this.authoring && this.canEdit) {
      // Re-read once, here: the write's own announcement would rebuild the form a second time.
      this.reflectLocally(() =>
        this.formBindingService.toggleShownResult(choice.operatorID, this.terminalOperatorIds())
      );
      this.readConfig();
      return;
    }
    const next = new Set(this.shownResultIds);
    if (next.has(choice.operatorID)) {
      next.delete(choice.operatorID);
    } else {
      next.add(choice.operatorID);
    }
    this.viewerResultIds = next;
    this.refreshShownResults();
    this.rebuildResultChoices();
    this.cdr.markForCheck();
  }

  private showWorkflow(): void {
    if (!this.workflowOpen) {
      this.workflowOpen = true;
      this.openWorkflowStrip();
    }
  }

  /**
   * Renaming the input itself, from its own title. Like the help text, a name or a hide flag is
   * presentation only: the wrapper that took the edit already shows it, so the form is NOT rebuilt
   * here. A rebuild would replace the very control the author is on (the name box, the eye) and drop
   * the keyboard focus with it; the stored override is applied on the next full re-read (Done).
   */
  private onBindingNamed(bindingId: string, value: string): void {
    this.reflectLocally(() => this.formBindingService.updateBinding(bindingId, { displayName: value }));
  }

  private onSubFieldNamed(bindingId: string, path: string, value: string): void {
    this.reflectLocally(() => this.formBindingService.setFieldOverride(bindingId, path, { displayName: value }));
    // The later rows of a repeated section show this name statically; keep them in step with the
    // box on the first row, so the rows agree without a rebuild.
    for (const node of this.followersByPath.get(WorkflowFormComponent.followerKey(bindingId, path)) ?? []) {
      node.props = { ...(node.props ?? {}), authorName: value };
    }
  }

  private onSubFieldHiddenAt(bindingId: string, path: string, hidden: boolean): void {
    this.reflectLocally(() => this.formBindingService.setFieldOverride(bindingId, path, { hidden }));
    for (const node of this.followersByPath.get(WorkflowFormComponent.followerKey(bindingId, path)) ?? []) {
      node.props = { ...(node.props ?? {}), authorHidden: hidden };
    }
  }

  /**
   * Make a presentation write (a name, a hide flag, help text, a result pick) without the rebuild
   * its own announcement would trigger. The control that took the edit already shows it, and the
   * announcement (formBindingChanged$) would otherwise rebuild the whole form synchronously inside
   * the click -- the eye is a button, so the typing hold does not cover it -- replacing that control,
   * dropping the keyboard focus and reverting a value typed elsewhere within its write debounce. The
   * announcement still reaches the autosave; only this page's own rebuild is skipped. Structural
   * writes (expose, remove, reorder) rebuild as before, through the announcement or through their
   * caller's own re-read.
   */
  private reflectLocally(write: () => void): void {
    this.reflectingLocally = true;
    try {
      write();
    } finally {
      this.reflectingLocally = false;
    }
  }

  // ---------------------------------------------------------------------------
  // Running the same workflow the canvas runs, through the same execute/kill service. The canvas
  // wraps its run with completion-email options (executeWorkflowWithEmailNotification); this page
  // runs plainly (executeWorkflow), so a form-started run does not send that email.
  // ---------------------------------------------------------------------------

  public get isRunning(): boolean {
    return (
      this.executionState !== ExecutionState.Uninitialized &&
      this.executionState !== ExecutionState.Completed &&
      this.executionState !== ExecutionState.Failed &&
      this.executionState !== ExecutionState.Killed &&
      this.executionState !== ExecutionState.Terminated
    );
  }

  /**
   * A run has started and ended, as opposed to never having run. Lets the empty results section say
   * "this run produced nothing" after a completed-but-empty run, instead of the "press Run" hint
   * that wrongly implies nothing has run yet.
   */
  public get hasRunFinished(): boolean {
    return (
      this.executionState === ExecutionState.Completed ||
      this.executionState === ExecutionState.Failed ||
      this.executionState === ExecutionState.Killed ||
      this.executionState === ExecutionState.Terminated
    );
  }

  /**
   * A unit is picked but its socket is still coming up -- the same window the operator canvas shows
   * "Connecting" and disables its run button. Read from the exact condition the canvas uses
   * (menu.component's getRunButtonBehavior), so the two stay in step.
   */
  public get isConnecting(): boolean {
    return (
      this.computingUnitStatus !== ComputingUnitState.NoComputingUnit && !this.workflowWebsocketService.isConnected
    );
  }

  /** No unit chosen yet: the button shows a disabled "Connect" hint and the unit is picked in the
   *  embedded selector -- unlike the canvas, where the Connect button is itself the click target. */
  public get hasNoComputingUnit(): boolean {
    return this.computingUnitStatus === ComputingUnitState.NoComputingUnit;
  }

  /** Write access to the chosen unit: the canvas gates execution on this (a READ/NONE-shared unit
   *  can be selected and viewed but not run on), so the form must too, or a reader could execute on
   *  a unit they only have read access to. */
  public get hasUnitWriteAccess(): boolean {
    return this.selectedUnit?.accessPrivilege === "WRITE";
  }

  /**
   * The Run button's label, icon and disabled state. It shares the operator canvas's disable
   * conditions -- an invalid or empty workflow, a unit still connecting, or no unit chosen each
   * disable it and say why -- but deliberately simplifies the execution states a reader needs down
   * to Run and Stop, with no pause/resume: while a run is in flight the button stops (kills) it,
   * otherwise it runs. (The canvas offers Pause/Resume/Submitting and a clickable Connect; a form
   * reader does not, and picks the unit in the embedded selector instead.)
   */
  public get runButtonState(): { label: string; icon: string; disabled: boolean } {
    // Connecting is checked before Stop on purpose: if the socket drops mid-run, a "Stop" would
    // send killWorkflow() through a dead socket and do nothing, so a disconnected unit disables the
    // button (as the canvas does) rather than offering a kill that cannot be delivered.
    if (this.isConnecting) {
      return { label: "Connecting", icon: "loading", disabled: true };
    }
    if (this.isRunning) {
      return { label: "Stop", icon: "stop", disabled: false };
    }
    if (!this.isWorkflowValid) {
      return { label: "Invalid", icon: "warning", disabled: true };
    }
    if (this.isWorkflowEmpty) {
      return { label: "Empty", icon: "info-circle", disabled: true };
    }
    if (this.hasNoComputingUnit) {
      return { label: "Connect", icon: "plus-circle", disabled: true };
    }
    // A unit is chosen and connected, but shared to this reader read-only: the canvas gates
    // execution on write access to the unit, so the form disables Run rather than sending a request
    // that the unit would reject.
    if (!this.hasUnitWriteAccess) {
      return { label: "No access", icon: "lock", disabled: true };
    }
    return { label: "Run", icon: "caret-right", disabled: false };
  }

  public onRun(): void {
    if (this.isRunning) {
      this.executeWorkflowService.killWorkflow();
      return;
    }
    // The button is disabled in exactly the states a run cannot start from (invalid/empty workflow,
    // connecting, or no unit), so a stray call here would be a silent no-op.
    if (this.runButtonState.disabled) {
      return;
    }
    this.runError = "";
    // Run as-is, like the canvas -- no client-side "fill everything first" gate (it diverged from
    // the canvas and could not guarantee success anyway). Empty/invalid inputs surface as a real
    // engine error via the execution-state stream (see the Failed handler).
    this.executeWorkflowService.executeWorkflow(this.workflowName);
  }

  /**
   * Turn an engine error into something a reader can act on: raw SQL/jOOQ/Java traces collapse to
   * one plain sentence, a short human message is kept (minus any Java prefix). The full text is
   * always logged for developers.
   */
  private friendlyRunError(raw: string): string {
    if (raw) {
      // eslint-disable-next-line no-console
      console.error("[workflow-form] run failed:", raw);
    }
    const opaque =
      !raw || /\bSQL \[|org\.jooq|org\.apache|org\.postgresql|foreign key|constraint|jdbc|\bat [\w.$]+\(/i.test(raw);
    if (opaque) {
      return "Run failed -- please reload and try again.";
    }
    const cleaned = raw
      .replace(/^[\w.$]+(?:Exception|Error):\s*/, "")
      .replace(/^requirement failed:\s*/i, "")
      .trim();
    return `Run failed: ${cleaned || "please check your inputs and try again."}`;
  }

  /**
   * Whether any exposed input that is required is still empty. Reuses formly's own per-field
   * required validation -- the very thing that renders "This field is required" under the box -- so
   * the run-failure message stays consistent with the field hint.
   */
  private hasEmptyRequiredInputs(): boolean {
    // Specifically a `required` error (a required field left empty), not just any invalid control:
    // a pattern or range failure is a different problem and should not be answered with "fill in the
    // required fields". Walk the control tree for a real required error.
    const hasRequiredError = (control: AbstractControl): boolean => {
      if (control.hasError("required")) {
        return true;
      }
      if (control instanceof FormGroup) {
        return Object.values(control.controls).some(hasRequiredError);
      }
      if (control instanceof FormArray) {
        return control.controls.some(hasRequiredError);
      }
      return false;
    };
    return this.rendered.some(r => hasRequiredError(r.form));
  }

  // ---------------------------------------------------------------------------
  // Inspecting a step: its property panel, opened read-only from the preview
  // ---------------------------------------------------------------------------

  /**
   * Move the panel to whatever the preview currently has highlighted, read-only. Exactly one
   * highlighted step opens the panel for it; none, or a shift-click multi-select, closes it -- with
   * more than one the property editor shows no single operator either, so opening it for whichever
   * step was clicked last would show the wrong settings.
   *
   * Both the highlight and the un-highlight stream run this, because each carries only the ids that
   * changed rather than the selection that resulted: dropping one of two selected steps leaves
   * exactly one and so has to OPEN the panel, yet only the un-highlight stream fires for it.
   */
  private syncSelectionFromHighlight(): void {
    const highlighted = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    // A highlighted id can already be gone from the graph -- a co-editor deleting the step emits the
    // un-highlight, and reading it back would open a panel on nothing.
    const graph = this.workflowActionService.getTexeraGraph();
    this.selectedOperatorId =
      highlighted.length === 1 && graph.hasOperator(highlighted[0]) ? highlighted[0] : undefined;
    this.cdr.detectChanges();
  }

  /** Dismiss the panel: the selection is what holds it open, so drop the highlight and the selection. */
  public closeOperatorPanel(): void {
    const wrapper = this.workflowActionService.getJointGraphWrapper();
    // Through the action service rather than the joint wrapper: only the service also publishes the
    // resulting highlight set on the shared awareness channel, so co-editors stop seeing this
    // reader's selection ringed on their own canvas.
    this.workflowActionService.unhighlightOperators(...wrapper.getCurrentHighlightedOperatorIDs());
    // Cleared here too, not left to the un-highlight stream: the wrapper emits nothing for an
    // operator that was not highlighted, and a dismiss has to close the panel regardless.
    this.selectedOperatorId = undefined;
    this.cdr.detectChanges();
  }

  /** Open or close the workflow preview; opening it builds the canvas the first time. */
  public toggleWorkflow(): void {
    this.workflowOpen = !this.workflowOpen;
    if (this.workflowOpen) {
      this.openWorkflowStrip();
    }
  }

  /**
   * Reveal the strip, then build the canvas a frame later (so JointJS measures the strip's
   * real size, not a zero-sized frame that misroutes links), then centre the graph a frame
   * after that so the fit runs against a canvas that exists. The editor keeps its own paper
   * sized via its container ResizeObserver, so nothing more is needed here.
   *
   * Each deferred step rechecks `workflowOpen`: a reader who opens then immediately collapses
   * the strip must not have the children mounted into a now-hidden (0-sized) body -- the
   * embedded mini-map is a fixed-size widget with no resize observer, so mounting it collapsed
   * would leave it blank on the next open.
   */
  private openWorkflowStrip(): void {
    this.later(() => {
      if (!this.workflowOpen) {
        return;
      }
      this.workflowEverOpened = true;
      this.cdr.detectChanges();
      this.later(() => {
        if (this.workflowOpen) {
          this.workflowActionService.getTexeraGraph().triggerCenterEvent();
        }
      });
    });
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
    // Save first and hand over only once the save has completed: the full-page load unloads this
    // document, and a request still in flight at that moment is aborted, so navigating right after
    // firing the save could lose the very edit the switch is meant to carry across. A save that
    // fails keeps the author here with the error shown, rather than leaving with changes that were
    // never stored. A reader, who has nothing to save, goes straight over.
    this.save(() => this.openCanvasPage());
  }

  /**
   * The full-page handover to the operator canvas, apart from the save so the order is testable.
   * Excluded from coverage as a whole: jsdom cannot navigate, so the specs stub this method and
   * assert when it is called rather than what it does.
   */
  /* v8 ignore start */
  private openCanvasPage(): void {
    window.location.href = `${USER_WORKSPACE}/${this.wid}`;
  }
  /* v8 ignore stop */

  /**
   * Save the same way the operator canvas does. Both views edit one workflow, so the
   * form has to write through the same debounced persist -- otherwise an author's
   * setup, or a value someone filled in, would be gone on the next visit.
   *
   * Saves go out one at a time, in order (persistQueue). Two persists in flight at once can reach
   * the backend out of order, and then the older content wins; with the queue, the save behind the
   * Canvas switch is sent only after an autosave already on its way has completed, and the last
   * one enqueued carries the latest content. The drain is deliberately NOT tied to this component's
   * lifetime: the final save on the way out (ngOnDestroy) has to line up behind an autosave still in
   * flight too, or the older snapshot could commit after it. ngOnDestroy completes the queue, so the
   * drain ends by itself once the last save has gone out; every request is a one-shot HTTP call.
   */
  private registerAutoPersist(): void {
    this.persistQueue
      .pipe(
        concatMap(workflow =>
          this.persistNow(workflow).pipe(
            // A failed save has reported itself; it must not let anyone navigate away from changes
            // that were never stored, so whatever was waiting for the drain is dropped.
            tap({ error: () => (this.afterDrain = []) }),
            // A failed save must not take the queue down with it: the next one still goes out.
            catchError(() => EMPTY),
            // Complete or failed, this save is done. Once nothing is left in the queue, run what was
            // waiting for the drain: by then every save asked for so far, this one and any enqueued
            // behind it while it was in flight, has gone out and come back.
            finalize(() => {
              this.queuedSaves--;
              if (this.queuedSaves === 0) {
                // Hand-over is waiting, but an edit arrived after the last snapshot and its debounced
                // autosave has not fired yet: the full-page load would kill that edit. Flush it into
                // the queue first; this drain check runs again once the flush has gone out. (When the
                // flush cannot be enqueued -- save()'s own guards -- fall through as save() itself
                // would: there is nothing left this page can store.)
                if (this.afterDrain.length > 0 && this.dirtySinceLastEnqueue) {
                  this.save();
                }
                if (this.queuedSaves > 0) {
                  return;
                }
                const waiting = this.afterDrain;
                this.afterDrain = [];
                waiting.forEach(run => run());
              }
            })
          )
        )
      )
      // Deliberately no untilDestroyed: the drain must outlive the component (see above) and ends
      // when ngOnDestroy completes the queue.
      // eslint-disable-next-line rxjs-angular/prefer-takeuntil
      .subscribe();
    this.workflowActionService
      .workflowChanged()
      .pipe(
        // Mark the edit before the debounce, so the drain can tell an edit is still waiting in it.
        tap(() => (this.dirtySinceLastEnqueue = true)),
        debounceTime(SAVE_DEBOUNCE_TIME_IN_MS),
        untilDestroyed(this)
      )
      .subscribe(() => this.save());
  }

  /**
   * Save the workflow this page opened, and only that one. The persist endpoint creates a
   * workflow when the payload has no id, so saving whatever the graph holds would spawn
   * stray "Untitled workflow" rows when the page is left before its workflow loaded.
   */
  private save(afterwards?: () => void): void {
    // A read-only viewer can open and run the form (execution is gated on computing-unit access,
    // not workflow access) but must never persist: every such save is a guaranteed 403 that would
    // spam "Could not save" on each debounce. Their inputs are non-editable, so nothing is lost.
    // `afterwards` runs once every queued save has completed -- this one and any asked for while it
    // was in flight -- or at once when there is nothing to save; it does not run when a save fails,
    // so a caller that navigates on it stays put instead.
    if (!this.canEdit) {
      afterwards?.();
      return;
    }
    if (!this.userService.isLogin() || !this.workflowPersistService.isWorkflowPersistEnabled()) {
      afterwards?.();
      return;
    }
    const workflow = this.workflowActionService.getWorkflow();
    if (workflow.wid === undefined || workflow.wid !== this.wid) {
      afterwards?.();
      return;
    }
    // Snapshot now, not when the request's turn comes: on the way out ngOnDestroy clears the graph
    // right after asking for this save, and the queue may only reach it after that.
    const preserved: Workflow = {
      ...workflow,
      content: { ...workflow.content, operatorPositions: this.positionsToSave(workflow.content) },
    };
    if (afterwards) {
      this.afterDrain.push(afterwards);
    }
    // The snapshot above carries everything reported up to now, the debounce included.
    this.dirtySinceLastEnqueue = false;
    this.queuedSaves++;
    this.persistQueue.next(preserved);
  }

  /** One persist of the given snapshot, reporting its outcome; the queue orders them. */
  private persistNow(preserved: Workflow): Observable<Workflow> {
    return this.workflowPersistService.persistWorkflow(preserved).pipe(
      tap({
        // Feed the saved workflow back, exactly as the operator canvas does: this advances
        // lastModifiedTime (and the normalised name), and the metadata subscription then repaints
        // the title bar -- so "Saved at ..." moves past the moment the page opened.
        next: updatedWorkflow => {
          // The page may be gone by the time a queued save returns (the drain outlives it): the graph
          // has been cleared, so there is nothing to repaint and the singleton must not be refilled.
          if (this.destroyed) {
            return;
          }
          // The response reflects the snapshot that was sent. A rename made since must not be undone
          // by it (its own save is already queued behind this one); what this feedback is for is the
          // server-owned part, the timestamp above all, and the normalised name when nothing changed.
          const current = this.workflowActionService.getWorkflowMetadata();
          this.workflowActionService.setWorkflowMetadata(
            current.name !== preserved.name ? { ...updatedWorkflow, name: current.name } : updatedWorkflow
          );
        },
        // A save that fails silently is the worst thing this page can do: the author walks
        // away believing the form they just built is stored.
        error: () => this.notificationService.error("Could not save. Your latest changes are not stored yet."),
      })
    );
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
   * Run after the current frame (or a delay), unless the page is gone by then: these callbacks
   * touch the view, and detectChanges on a destroyed view throws -- reachable by navigating away
   * while the name field is waiting to be measured, or the preview canvas to be built.
   */
  private later(fn: () => void, delayMs?: number): void {
    const run = () => {
      if (!this.destroyed) {
        fn();
      }
    };
    if (delayMs === undefined) {
      requestAnimationFrame(run);
    } else {
      setTimeout(run, delayMs);
    }
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
    // The final save joins the queue behind anything still in flight, then the queue is closed: the
    // drain (not tied to this component) sends what is left in order and ends by itself.
    this.save();
    this.persistQueue.complete();
    this.workflowActionService.clearWorkflow();
    this.computingUnitStatusService.disconnect();
    this.executeWorkflowService.resetExecutionAndWorkers();
    this.workflowConsoleService.clearConsoleMessages();
    this.workflowResultService.clearResults();
  }
}
