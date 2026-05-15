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

import { AfterViewInit, ChangeDetectorRef, Component, ElementRef, OnDestroy, OnInit } from "@angular/core";
import { combineLatest, fromEvent, merge, Subject } from "rxjs";
import { NzModalCommentBoxComponent } from "./comment-box-modal/nz-modal-comment-box.component";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { DragDropService } from "../../service/drag-drop/drag-drop.service";
import { DynamicSchemaService } from "../../service/dynamic-schema/dynamic-schema.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { fromJointPaperEvent, JointUIService, linkPathStrokeColor } from "../../service/joint-ui/joint-ui.service";
import { ValidationWorkflowService } from "../../service/validation/validation-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { ProfilerService, ProfilerState } from "../../service/profiler/profiler.service";
import { formatDeltaHoverHeadline, formatHoverHeadline, formatViewLabel } from "../../service/profiler/profiler-hover";
import {
  ComparableOperator,
  computeAllDeltas,
  computeDeltaIntensity,
  indexBaseline,
  maxAbsRuntimeDelta,
  statsToComparable,
} from "../../service/profiler/profiler-delta";
import { ProfilerSuggestionsService } from "../../service/profiler/profiler-suggestions.service";
import { Suggestion } from "../../service/profiler/profiler-suggestions";
import { WorkflowUtilService } from "../../service/workflow-graph/util/workflow-util.service";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";
import { LogicalPort, OperatorLink, OperatorPredicate } from "../../types/workflow-common.interface";
import { auditTime, filter, map, takeUntil, withLatestFrom } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UndoRedoService } from "../../service/undo-redo/undo-redo.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { OperatorMenuService } from "../../service/operator-menu/operator-menu.service";
import { NzContextMenuService, NzDropdownMenuComponent } from "ng-zorro-antd/dropdown";
import { ActivatedRoute, Router } from "@angular/router";
import * as _ from "lodash";
import * as joint from "jointjs";
import { isDefined } from "../../../common/util/predicate";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { line, curveCatmullRomClosed } from "d3-shape";
import concaveman from "concaveman";
import { OperatorResultSummary, AgentService } from "../../service/agent/agent.service";
import { NzNoAnimationDirective } from "ng-zorro-antd/core/animation";
import { ContextMenuComponent } from "./context-menu/context-menu/context-menu.component";
import { NgIf, NgFor, AsyncPipe, DecimalPipe, NgSwitch, NgSwitchCase } from "@angular/common";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzIconModule } from "ng-zorro-antd/icon";
import { AgentInteractionComponent } from "../agent/agent-interaction/agent-interaction.component";

// jointjs interactive options for enabling and disabling interactivity
// https://resources.jointjs.com/docs/jointjs/v3.2/joint.html#dia.Paper.prototype.options.interactive
const defaultInteractiveOption = { vertexAdd: false, labelMove: false };
const disableInteractiveOption = {
  linkMove: false,
  labelMove: false,
  arrowheadMove: false,
  vertexMove: false,
  vertexAdd: false,
  vertexRemove: false,
  elementMove: false, // TODO: This is only a temporary change, will introduce another level of disable option.
  addLinkFromMagnet: false,
};

export const MAIN_CANVAS = {
  xMin: -960,
  xMax: 2688, // xMin * 2.8
  yMin: -540,
  yMax: 1512, // yMin * 2.8
};

/**
 * WorkflowEditorComponent is the component for the main workflow editor part of the UI.
 *
 * This component is bound with the JointJS paper. JointJS handles the operations of the main workflow.
 * The JointJS UI events are wrapped into observables and exposed to other components / services.
 *
 * See JointJS documentation for the list of events that can be captured on the JointJS paper view.
 * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.events
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-workflow-editor",
  templateUrl: "workflow-editor.component.html",
  styleUrls: ["workflow-editor.component.scss"],
  imports: [NzDropdownMenuComponent, NzNoAnimationDirective, ContextMenuComponent, NgIf, NgFor, NgSwitch, NgSwitchCase, AsyncPipe, DecimalPipe, NzTooltipModule, NzIconModule, AgentInteractionComponent],
})
export class WorkflowEditorComponent implements OnInit, AfterViewInit, OnDestroy {
  editor!: HTMLElement;
  editorWrapper!: HTMLElement;
  paper!: joint.dia.Paper;
  private interactive: boolean = true;
  private _onProcessKeyboardActionObservable: Subject<void> = new Subject();
  private wrapper;
  private currentOpenedOperatorID: string | null = null;
  private removeButton!: new () => joint.linkTools.Button;
  private breakpointButton!: new () => joint.linkTools.Button;

  // Chat popover state (operator chat button)
  public chatPopoverOperator: {
    operatorId: string;
    displayName: string;
    position: { x: number; y: number };
  } | null = null;

  // Profiler hover-card state. Non-null while the cursor is over an operator that
  // has a heat score AND profiling is enabled; null otherwise.
  public profilerHover: {
    displayName: string;
    viewLabel: string;
    headline?: string;
    score: number;
    x: number;
    y: number;
  } | null = null;

  // Ghost optimization suggestions rendered as HTML overlays on top of the canvas.
  // Each ghost is a "click to materialize" Filter insertion proposed by the profiler.
  public profilerGhosts: ReadonlyArray<Suggestion & { x: number; y: number }> = [];
  private lastSuggestions: readonly Suggestion[] = [];

  /**
   * After materializing a suggestion, this floating prompt at the top of the canvas
   * invites the user to re-run the workflow (the natural next step after a structural
   * change). Null when no prompt is showing. Auto-dismisses after a few seconds.
   */
  public profilerRunPrompt: { message: string } | null = null;
  private profilerRunPromptTimeoutId?: number;

  // Cached agent result summaries for port label display

  constructor(
    private workflowActionService: WorkflowActionService,
    private dynamicSchemaService: DynamicSchemaService,
    private dragDropService: DragDropService,
    private validationWorkflowService: ValidationWorkflowService,
    private jointUIService: JointUIService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
    private nzModalService: NzModalService,
    private changeDetectorRef: ChangeDetectorRef,
    private undoRedoService: UndoRedoService,
    private workflowVersionService: WorkflowVersionService,
    private operatorMenu: OperatorMenuService,
    private route: ActivatedRoute,
    private router: Router,
    public nzContextMenu: NzContextMenuService,
    private elementRef: ElementRef,
    private config: GuiConfigService,
    private agentService: AgentService,
    public profilerService: ProfilerService,
    private profilerSuggestionsService: ProfilerSuggestionsService,
    private workflowUtilService: WorkflowUtilService,
    private workflowCompilingService: WorkflowCompilingService
  ) {
    this.wrapper = this.workflowActionService.getJointGraphWrapper();
  }

  private operatorSummaries: Map<string, OperatorResultSummary> = new Map();

  ngOnInit(): void {
    // Cache the tool constructors
    this.removeButton = WorkflowEditorComponent.getRemoveButton();
    this.breakpointButton = WorkflowEditorComponent.getBreakpointButton();

    this.agentService.operatorResultSummaries$.pipe(untilDestroyed(this)).subscribe(summaries => {
      this.operatorSummaries = summaries;
      if (this.chatPopoverOperator) {
        this.changeDetectorRef.detectChanges();
      }
    });
  }

  /**
   * This function is provided to JointJS to disallow links starting from an in port.
   *
   * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateMagnet
   */
  private static validateOperatorMagnet(
    cellView: joint.dia.CellView,
    magnet: SVGElement,
    event: joint.dia.Event
  ): boolean {
    return magnet && magnet.getAttribute("port-group") === "out";
  }

  ngAfterViewInit() {
    this.editor = document.getElementById("workflow-editor")!;
    this.editorWrapper = document.getElementById("workflow-editor-wrapper")!;
    document.addEventListener("keydown", this._handleKeyboardAction.bind(this));
    this.initializeJointPaper();
    this.handleDisableJointPaperInteractiveness();
    this.handleOperatorValidation();
    this.handlePaperRestoreDefaultOffset();
    this.handlePaperZoom();
    this.handleWindowResize();
    this.handleViewDeleteOperator();
    if (this.workflowActionService.getHighlightingEnabled()) {
      this.handleCellHighlight();
    }
    this.handleDisableOperator();
    this.handleViewOperatorResult();
    this.handleReuseCacheOperator();
    this.registerOperatorDisplayNameChangeHandler();
    this.handleViewDeleteLink();
    this.handleViewAddPort();
    this.handleViewRemovePort();
    this.handlePortClick();
    this.handlePaperPan();
    this.handleOperatorSelectionEvents();
    this.handlePortHighlightEvent();
    this.registerPortDisplayNameChangeHandler();
    this.handleOperatorStatisticsUpdate();
    this.handleProfilerHeatmap();
    this.handleProfilerHover();
    this.handleProfilerSuggestions();
    this.handleRegionEvents();
    this.handleOperatorSuggestionHighlightEvent();
    this.handleAgentHoverHighlight();
    this.handleElementDelete();
    this.handleElementSelectAll();
    this.handleElementCopy();
    this.handleElementCut();
    this.handleElementPaste();
    this.handleLinkCursorHover();
    if (this.config.env.linkBreakpointEnabled && this.workflowActionService.getHighlightingEnabled()) {
      this.handleLinkBreakpoint();
    }
    this.handlePointerEvents();
    this.handleURLFragment();
    this.invokeResize();
    this.handleCenterEvent();
    this.handleOperatorChatButton();
  }

  ngOnDestroy(): void {
    document.removeEventListener("keydown", this._handleKeyboardAction.bind(this));
  }

  private _handleKeyboardAction(event: any) {
    this._onProcessKeyboardActionObservable = new Subject();
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(takeUntil(this._onProcessKeyboardActionObservable))
      .subscribe(displayParticularWorkflowVersion => {
        if (!displayParticularWorkflowVersion) {
          // cmd/ctrl+z undo ; ctrl+y or cmd/ctrl + shift+z for redo
          if ((event.metaKey || event.ctrlKey) && !event.shiftKey && event.key.toLowerCase() === "z") {
            // UNDO
            if (this.undoRedoService.canUndo()) {
              this.undoRedoService.undoAction();
            }
          } else if (
            ((event.metaKey || event.ctrlKey) && !event.shiftKey && event.key.toLowerCase() === "y") ||
            ((event.metaKey || event.ctrlKey) && event.shiftKey && event.key.toLowerCase() === "z")
          ) {
            // redo
            if (this.undoRedoService.canRedo()) {
              this.undoRedoService.redoAction();
            }
          }
          // below for future hotkeys
        }
        this._onProcessKeyboardActionObservable.complete();
      });
  }

  private initializeJointPaper(): void {
    // attach the JointJS graph (model) to the paper (view)
    this.paper = this.wrapper.attachMainJointPaper({
      el: this.editor,
      background: { color: "#F6F6F6" },
      // enable jointjs feature that automatically snaps a link to the closest port with a radius of 30px
      snapLinks: { radius: 40 },
      // disable jointjs default action that can make a link not connect to an operator
      linkPinning: false,
      // provide a validation to determine if two ports could be connected (only output connect to input is allowed)
      validateConnection: (...args) => this.validateJointOperatorConnection(...args),
      // provide a validation to determine if the port where link starts from is an out port
      validateMagnet: (...args) => WorkflowEditorComponent.validateOperatorMagnet(...args),
      // marks all the available magnets or elements when a link is dragged
      markAvailable: true,
      // disable jointjs default action of adding vertexes to the link
      interactive: defaultInteractiveOption,
      // set a default link element used by jointjs when user creates a link on UI
      defaultLink: JointUIService.getDefaultLinkCell(),
      // disable jointjs default action that stops propagate click events on jointjs paper
      preventDefaultBlankAction: false,
      // prevents normal right click menu showing up on jointjs paper
      preventContextMenu: true,
      // draw dots in the background of the paper
      drawGrid: {
        name: "fixedDot",
        args: { color: "black", scaleFactor: 8, thickness: 1.2 },
      },
      gridSize: 1,
      // use approximate z-index sorting, this is a workaround of a bug in async rendering mode
      // see https://github.com/clientIO/joint/issues/1320
      sorting: joint.dia.Paper.sorting.APPROX,
      width: this.editor.offsetWidth,
      height: this.editor.offsetHeight,
    });
    this.editor.classList.add("hide-worker-count");
  }

  private handleDisableJointPaperInteractiveness(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(enabled => {
        if (enabled) {
          this.interactive = true;
          this.paper.setInteractivity(defaultInteractiveOption);
        } else {
          this.interactive = false;
          this.paper.setInteractivity(disableInteractiveOption);
        }
        this.changeDetectorRef.detectChanges();
      });
  }

  /**
   * This method subscribe to workflowStatusService's status stream
   * for Each processStatus that has been emitted
   *    1. enable operatorStatusTooltipDisplay because tooltip will not be empty
   *    2. for each operator in current texeraGraph:
   *        - find its Statistics in processStatus, thrown an error if not found
   *        - generate its corresponding tooltip's id
   *        - pass the tooltip id and Statistics to jointUIService
   *          the specific tooltip content will be updated
   *          - if operator is in a group, save statistics in group's operatorInfo
   *    3. Whenever a group is expanded
   *        - for each operatorInfo, display statistics if there are some saved.
   */
  private handleOperatorStatisticsUpdate(): void {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(status => {
        this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .forEach(op => {
            if (
              isDefined(status[op.operatorID]) &&
              this.executeWorkflowService.getExecutionState().state === ExecutionState.Recovering
            ) {
              status[op.operatorID] = {
                ...status[op.operatorID],
                operatorState: OperatorState.Recovering,
              };
            }

            this.jointUIService.changeOperatorStatistics(
              this.paper,
              op.operatorID,
              status[op.operatorID],
              this.isSource(op.operatorID),
              this.isSink(op.operatorID)
            );
          });
      });

    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (event.previous.state === ExecutionState.Recovering) {
          let operatorState: OperatorState;
          if (event.current.state === ExecutionState.Paused) {
            operatorState = OperatorState.Paused;
          } else if (event.current.state === ExecutionState.Completed) {
            operatorState = OperatorState.Completed;
          } else if (event.current.state === ExecutionState.Running) {
            operatorState = OperatorState.Running;
          } else {
            throw new Error("unknown state transition from recovering state: " + event.current.state);
          }
          this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .forEach(op => {
              this.jointUIService.changeOperatorState(this.paper, op.operatorID, operatorState);
            });
        }
      });
  }

  /**
   * Subscribes to the profiler state stream and repaints operator bodies according to the
   * computed heat scores. When profiling is disabled, restores each operator's default fill.
   */
  private handleProfilerHeatmap(): void {
    let lastEnabled = false;
    this.profilerService
      .getStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        const operators = this.workflowActionService.getTexeraGraph().getAllOperators();
        if (!state.enabled) {
          if (lastEnabled) {
            operators.forEach(op => this.jointUIService.resetOperatorHeatmap(this.paper, op));
          }
          lastEnabled = false;
          return;
        }
        lastEnabled = true;

        if (state.view === "delta") {
          this.applyDeltaHeatmap(state, operators);
          return;
        }

        operators.forEach(op => {
          const entry = state.scores[op.operatorID];
          if (!entry) {
            this.jointUIService.resetOperatorHeatmap(this.paper, op);
            return;
          }
          const measurableCost =
            (entry.stats.aggregatedDataProcessingTime ?? 0) > 0 ||
            (entry.stats.aggregatedOutputRowCount ?? 0) > 0;
          this.jointUIService.changeOperatorHeatmap(
            this.paper,
            op.operatorID,
            entry.score,
            entry.state,
            measurableCost
          );
        });
      });
  }

  /**
   * Paints the canvas with green/red intensities based on per-operator runtime
   * deltas against the loaded baseline. Falls back to default fills when no
   * baseline is loaded.
   */
  private applyDeltaHeatmap(state: ProfilerState, operators: readonly OperatorPredicate[]): void {
    if (!state.baseline) {
      operators.forEach(op => this.jointUIService.resetOperatorHeatmap(this.paper, op));
      return;
    }
    const baselineIndex = indexBaseline(state.baseline);
    const currentMap: Record<string, ComparableOperator> = {};
    for (const op of operators) {
      const entry = state.scores[op.operatorID];
      if (!entry) continue;
      const displayName = op.customDisplayName?.trim() || op.operatorType || op.operatorID;
      currentMap[op.operatorID] = statsToComparable({
        operatorId: op.operatorID,
        displayName,
        operatorType: op.operatorType,
        score: entry.score,
        stats: entry.stats,
      });
    }
    const deltas = computeAllDeltas(currentMap, baselineIndex);
    const maxAbs = maxAbsRuntimeDelta(deltas);

    operators.forEach(op => {
      const delta = deltas[op.operatorID];
      if (!delta) {
        // operator has no entry on either side (not yet seen this run, no baseline)
        this.jointUIService.resetOperatorHeatmap(this.paper, op);
        return;
      }
      const intensity = computeDeltaIntensity(delta, maxAbs);
      const model = this.paper.getModelById(op.operatorID);
      if (!model) return;
      model.attr("rect.body/fill", JointUIService.getDeltaHeatmapColor(intensity));
    });
  }

  /**
   * Subscribes to JointJS element hover events and surfaces a small floating card
   * with the operator's display name, current-view headline metric, and heat score.
   *
   * Visibility rules:
   *   - Card only appears when profiling is enabled AND the operator has an entry in scores.
   *   - Card is non-interactive (pointer-events: none) so it never steals clicks.
   *   - Card hides on mouseleave, on canvas pan/click, or when profiling is toggled off.
   */
  private handleProfilerHover(): void {
    fromJointPaperEvent(this.paper, "element:mouseenter")
      .pipe(untilDestroyed(this))
      .subscribe(([cellView, evt]: any) => {
        const opId = cellView?.model?.id?.toString();
        if (!opId) return;
        const state = this.profilerService.getState();
        if (!state.enabled) return;
        const entry = state.scores[opId];
        if (!entry) return;

        const wrapperRect = this.editorWrapper?.getBoundingClientRect();
        if (!wrapperRect) return;
        const clientX = evt?.clientX ?? 0;
        const clientY = evt?.clientY ?? 0;
        const op = this.workflowActionService.getTexeraGraph().hasOperator(opId)
          ? this.workflowActionService.getTexeraGraph().getOperator(opId)
          : undefined;
        const displayName = op?.customDisplayName?.trim() || op?.operatorType || opId;

        // In delta view, headline shows the current-vs-baseline runtime gap (if any).
        // In all other views, headline shows the view's primary metric.
        let headline: string | undefined;
        if (state.view === "delta" && state.baseline) {
          const baselineOp = indexBaseline(state.baseline)[opId];
          if (baselineOp) {
            const currentRuntimeMs =
              entry.stats.aggregatedDataProcessingTime && entry.stats.aggregatedDataProcessingTime > 0
                ? entry.stats.aggregatedDataProcessingTime / 1_000_000
                : null;
            const runtimeMsDelta =
              currentRuntimeMs !== null && baselineOp.runtimeMs !== null
                ? currentRuntimeMs - baselineOp.runtimeMs
                : null;
            headline = formatDeltaHoverHeadline(runtimeMsDelta);
          }
        } else {
          headline = formatHoverHeadline(state.view, entry.stats);
        }

        this.profilerHover = {
          displayName,
          viewLabel: formatViewLabel(state.view),
          headline,
          score: entry.score,
          x: clientX - wrapperRect.left + 12,
          y: clientY - wrapperRect.top + 12,
        };
        this.changeDetectorRef.detectChanges();
      });

    fromJointPaperEvent(this.paper, "element:mouseleave")
      .pipe(untilDestroyed(this))
      .subscribe(() => this.clearProfilerHover());

    fromJointPaperEvent(this.paper, "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(() => this.clearProfilerHover());

    // Hide when profiling is toggled off so the stale card doesn't linger.
    this.profilerService
      .getStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(state => {
        if (!state.enabled && this.profilerHover) {
          this.clearProfilerHover();
        }
      });
  }

  private clearProfilerHover(): void {
    if (this.profilerHover) {
      this.profilerHover = null;
      this.changeDetectorRef.detectChanges();
    }
  }

  /**
   * Subscribes to the profiler suggestions stream and renders one HTML "ghost"
   * overlay per suggestion. Ghosts are positioned at the midpoint of the edge they
   * represent, in wrapper-relative pixel coordinates. Repositions on canvas pan/zoom
   * so the ghosts stay anchored.
   */
  private handleProfilerSuggestions(): void {
    this.profilerSuggestionsService
      .getSuggestionsStream()
      .pipe(untilDestroyed(this))
      .subscribe(suggestions => {
        this.lastSuggestions = suggestions;
        this.recomputeGhostPositions();
      });

    // The menu popover's "Apply" buttons fire materialize requests through the service
    // (instead of depending on this editor component directly). Subscribe and route them
    // through the same materialize logic the canvas ghost click uses.
    this.profilerSuggestionsService
      .getMaterializeRequestStream()
      .pipe(untilDestroyed(this))
      .subscribe(s => this.materializeSuggestion(s));

    // JointJS emits `translate` / `scale` events when the user pans/zooms the canvas.
    // Reposition ghosts on each so they remain anchored to their edges.
    this.paper.on("translate", () => this.recomputeGhostPositions());
    this.paper.on("scale", () => this.recomputeGhostPositions());
  }

  private recomputeGhostPositions(): void {
    if (!this.editorWrapper) {
      this.profilerGhosts = [];
      return;
    }
    const wrapperRect = this.editorWrapper.getBoundingClientRect();
    const positioned: Array<Suggestion & { x: number; y: number }> = [];
    // Track per-anchor occupancy so overlapping ghosts get a small vertical offset.
    const anchorBucket = new Map<string, number>();
    for (const s of this.lastSuggestions) {
      const placed = this.computeGhostPosition(s, wrapperRect, anchorBucket);
      if (placed) positioned.push(placed);
    }
    this.profilerGhosts = positioned;
    this.changeDetectorRef.detectChanges();
  }

  /**
   * Computes wrapper-relative pixel coordinates for a single suggestion. Branches
   * on suggestion type so edge ghosts (INSERT_FILTER) sit at edge midpoints and
   * operator-attached ghosts (BUMP_WORKERS) sit above their operator. Returns
   * undefined when the referenced operator(s) are no longer on the canvas.
   */
  private computeGhostPosition(
    s: Suggestion,
    wrapperRect: DOMRect,
    anchorBucket: Map<string, number>
  ): (Suggestion & { x: number; y: number }) | undefined {
    if (s.type === "INSERT_FILTER") {
      const upElem = this.paper.getModelById(s.upstreamOpId);
      const downElem = this.paper.getModelById(s.downstreamOpId);
      if (!upElem || !downElem) return undefined;
      const upBBox = (upElem as joint.dia.Element).getBBox();
      const downBBox = (downElem as joint.dia.Element).getBBox();
      const midPaperX = (upBBox.x + upBBox.width + downBBox.x) / 2;
      const midPaperY = (upBBox.y + upBBox.height / 2 + downBBox.y + downBBox.height / 2) / 2;
      const pagePoint = this.paper.localToPagePoint(midPaperX, midPaperY);
      const anchorKey = `edge:${s.upstreamOpId}->${s.downstreamOpId}`;
      const stackIdx = anchorBucket.get(anchorKey) ?? 0;
      anchorBucket.set(anchorKey, stackIdx + 1);
      return {
        ...s,
        x: pagePoint.x - wrapperRect.left,
        y: pagePoint.y - wrapperRect.top + stackIdx * 32,
      };
    }
    // BUMP_WORKERS: anchor above the operator.
    const opElem = this.paper.getModelById(s.operatorId);
    if (!opElem) return undefined;
    const bbox = (opElem as joint.dia.Element).getBBox();
    const paperX = bbox.x + bbox.width / 2;
    const paperY = bbox.y - 24; // 24px above the operator's top edge
    const pagePoint = this.paper.localToPagePoint(paperX, paperY);
    const anchorKey = `op:${s.operatorId}`;
    const stackIdx = anchorBucket.get(anchorKey) ?? 0;
    anchorBucket.set(anchorKey, stackIdx + 1);
    return {
      ...s,
      x: pagePoint.x - wrapperRect.left,
      y: pagePoint.y - wrapperRect.top - stackIdx * 32,
    };
  }

  /**
   * Applies a ghost suggestion to the canvas. Branches on the suggestion type:
   *   - INSERT_FILTER → inserts a Filter operator on the upstream→downstream edge.
   *   - BUMP_WORKERS  → sets the operator's `workers` property to the proposed value.
   * In both cases, after the change we highlight the affected operator so the
   * property panel opens for the user to fine-tune.
   */
  public materializeSuggestion(ghost: Suggestion): void {
    if (ghost.type === "INSERT_FILTER") {
      this.materializeInsertFilter(ghost);
      return;
    }
    if (ghost.type === "BUMP_WORKERS") {
      this.materializeBumpWorkers(ghost);
      return;
    }
  }

  private materializeInsertFilter(ghost: Extract<Suggestion, { type: "INSERT_FILTER" }>): void {
    const upElem = this.paper.getModelById(ghost.upstreamOpId);
    const downElem = this.paper.getModelById(ghost.downstreamOpId);
    if (!upElem || !downElem) {
      this.profilerSuggestionsService.dismiss(ghost.id);
      return;
    }
    const upBBox = (upElem as joint.dia.Element).getBBox();
    const downBBox = (downElem as joint.dia.Element).getBBox();
    const newPos = {
      x: (upBBox.x + upBBox.width + downBBox.x) / 2 - 60,
      y: (upBBox.y + upBBox.height / 2 + downBBox.y + downBBox.height / 2) / 2 - 30,
    };

    // Pre-populate the Filter with a starter predicate on the first upstream column,
    // condition `is not null` (a safe no-op the user will immediately edit). This makes
    // the property panel look "ready" instead of an empty form. We can't infer the
    // actual filter values without sample data or AI — that's covered by Phase 3
    // (agent integration) of the suggestions plan.
    const firstColumn = this.firstUpstreamColumnName(ghost.upstreamOpId);
    const newFilterBase = this.workflowUtilService.getNewOperatorPredicate("Filter");
    const newFilter: OperatorPredicate = firstColumn
      ? {
          ...newFilterBase,
          operatorProperties: {
            ...newFilterBase.operatorProperties,
            predicates: [{ attribute: firstColumn, condition: "is not null", value: "" }],
          },
        }
      : newFilterBase;

    const graph = this.workflowActionService.getTexeraGraph();
    const existingLinks = graph
      .getOutputLinksByOperatorId(ghost.upstreamOpId)
      .filter(l => l.target.operatorID === ghost.downstreamOpId);

    graph.bundleActions(() => {
      this.workflowActionService.addOperator(newFilter, newPos);
      if (existingLinks.length > 0) {
        const linkToBreak = existingLinks[0];
        const newInputPortId = newFilter.inputPorts[0]?.portID;
        const newOutputPortId = newFilter.outputPorts[0]?.portID;
        if (newInputPortId && newOutputPortId) {
          this.workflowActionService.deleteLink(linkToBreak.source, linkToBreak.target);
          this.workflowActionService.addLink({
            linkID: this.workflowUtilService.getLinkRandomUUID(),
            source: linkToBreak.source,
            target: { operatorID: newFilter.operatorID, portID: newInputPortId },
          });
          this.workflowActionService.addLink({
            linkID: this.workflowUtilService.getLinkRandomUUID(),
            source: { operatorID: newFilter.operatorID, portID: newOutputPortId },
            target: linkToBreak.target,
          });
        }
      }
    });

    // Open the property panel on the new Filter so the user can configure its predicate.
    this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(false);
    this.workflowActionService.getJointGraphWrapper().highlightOperators(newFilter.operatorID);

    this.profilerSuggestionsService.dismiss(ghost.id);
    this.showRunPrompt(ghost);
  }

  /**
   * Picks the first column name from the upstream operator's output schema, or
   * `undefined` if the schema isn't available (e.g. workflow hasn't been compiled
   * yet). Used to seed a sensible default predicate when materializing a Filter ghost.
   */
  private firstUpstreamColumnName(upstreamOpId: string): string | undefined {
    const outputMap = this.workflowCompilingService.getOperatorOutputSchemaMap(upstreamOpId);
    if (!outputMap) return undefined;
    for (const portKey of Object.keys(outputMap)) {
      const portSchema = outputMap[portKey];
      if (portSchema && portSchema.length > 0) {
        return portSchema[0].attributeName;
      }
    }
    return undefined;
  }

  private materializeBumpWorkers(ghost: Extract<Suggestion, { type: "BUMP_WORKERS" }>): void {
    const graph = this.workflowActionService.getTexeraGraph();
    if (!graph.hasOperator(ghost.operatorId)) {
      this.profilerSuggestionsService.dismiss(ghost.id);
      return;
    }
    const op = graph.getOperator(ghost.operatorId);
    // Merge: preserve all other properties, override `workers`.
    const newProperties = {
      ...(op.operatorProperties ?? {}),
      workers: ghost.proposedWorkers,
    };
    this.workflowActionService.setOperatorProperty(ghost.operatorId, newProperties);

    // Open the property panel on the bumped operator so the user can verify / fine-tune.
    this.workflowActionService.getJointGraphWrapper().setMultiSelectMode(false);
    this.workflowActionService.getJointGraphWrapper().highlightOperators(ghost.operatorId);

    this.profilerSuggestionsService.dismiss(ghost.id);
    this.showRunPrompt(ghost);
  }

  public dismissSuggestion(ghost: Suggestion, event: MouseEvent): void {
    event.stopPropagation();
    this.profilerSuggestionsService.dismiss(ghost.id);
  }

  public trackGhostById(_index: number, ghost: Suggestion): string {
    return ghost.id;
  }

  /**
   * Briefly surface a "Run now" prompt at the top of the canvas after the user
   * materializes a suggestion. Auto-dismisses after 10 seconds; the user can also
   * click ×. The Run button itself stays the canonical way to execute — this is just
   * a one-click shortcut so the user doesn't have to chase the toolbar after applying.
   */
  private showRunPrompt(suggestion: Suggestion): void {
    const message =
      suggestion.type === "INSERT_FILTER"
        ? "Filter inserted — re-run to see how the workflow performs with the change?"
        : "Worker count updated — re-run to see how the workflow performs with the change?";
    this.profilerRunPrompt = { message };
    if (this.profilerRunPromptTimeoutId !== undefined) {
      window.clearTimeout(this.profilerRunPromptTimeoutId);
    }
    this.profilerRunPromptTimeoutId = window.setTimeout(() => {
      this.profilerRunPrompt = null;
      this.profilerRunPromptTimeoutId = undefined;
      this.changeDetectorRef.detectChanges();
    }, 10000);
    this.changeDetectorRef.detectChanges();
  }

  public triggerRunFromPrompt(): void {
    this.profilerSuggestionsService.requestWorkflowRun();
    this.dismissRunPrompt();
  }

  public dismissRunPrompt(): void {
    this.profilerRunPrompt = null;
    if (this.profilerRunPromptTimeoutId !== undefined) {
      window.clearTimeout(this.profilerRunPromptTimeoutId);
      this.profilerRunPromptTimeoutId = undefined;
    }
    this.changeDetectorRef.detectChanges();
  }

  private handleRegionEvents(): void {
    this.editor.classList.add("hide-region");
    const Region = joint.dia.Element.define(
      "region",
      {
        attrs: {
          body: {
            fill: "rgba(158,158,158,0.2)",
            pointerEvents: "none",
            class: "region",
          },
        },
      },
      {
        markup: [{ tagName: "path", selector: "body" }],
      }
    );

    let regionMap: { regionElement: joint.dia.Element; operators: joint.dia.Cell[] }[] = [];
    // update region elements on execution
    this.executeWorkflowService
      .getRegionUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        this.paper.model
          .getCells()
          .filter(element => element instanceof Region)
          .forEach(element => element.remove());

        regionMap = event.regions.map(([id, region]) => {
          const element = new Region({ id: "region-" + id });
          const ops = region.map(id => this.paper.getModelById(id));
          this.paper.model.addCell(element);
          this.updateRegionElement(element, ops);
          return { regionElement: element, operators: ops };
        });
      });

    this.paper.model.on("change:position", operator => {
      regionMap
        .filter(region => region.operators.includes(operator))
        .forEach(region => this.updateRegionElement(region.regionElement, region.operators));
    });

    // update region element colors on execution
    this.executeWorkflowService
      .getRegionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(region => {
        const colorMap: Record<string, string> = {
          ExecutingDependeePortsPhase: "rgba(33,150,243,0.2)",
          ExecutingNonDependeePortsPhase: "rgba(255,213,79,0.2)",
          Completed: "rgba(76,175,80,0.2)",
        };
        this.paper.getModelById("region-" + region.id).attr("body/fill", colorMap[region.state]);
      });
  }

  private updateRegionElement(regionElement: joint.dia.Element, operators: joint.dia.Cell[]) {
    const points = operators.flatMap(op => {
      const { x, y, width, height } = op.getBBox(),
        padding = 15;
      return [
        [x - padding, y - padding],
        [x + width + padding, y - padding],
        [x - padding, y + height + padding + 10],
        [x + width + padding, y + height + padding + 10],
      ];
    });
    regionElement.attr("body/d", line().curve(curveCatmullRomClosed)(concaveman(points, 2, 0) as [number, number][]));
  }

  /**
   * Handles restore offset default event by translating jointJS paper
   *  back to original position
   */
  private handlePaperRestoreDefaultOffset(): void {
    this.wrapper
      .getRestorePaperOffsetStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.wrapper.setZoomProperty(1);
        this.paper.translate(0, 0);
      });
  }

  /**
   * Handles zoom events to make the jointJS paper larger or smaller.
   */
  private handlePaperZoom(): void {
    this.wrapper
      .getWorkflowEditorZoomStream()
      .pipe(untilDestroyed(this))
      .subscribe(newRatio => this.paper.scale(newRatio, newRatio));
  }

  private handlePaperPan(): void {
    fromJointPaperEvent(this.paper, "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(() =>
        fromEvent<MouseEvent>(document, "mousemove")
          .pipe(takeUntil(fromEvent(document, "mouseup")))
          .subscribe(event =>
            this.paper.translate(
              this.paper.translate().tx + event.movementX / this.paper.scale().sx,
              this.paper.translate().ty + event.movementY / this.paper.scale().sy
            )
          )
      );
  }

  /**
   * This is the handler for window resize event
   * When the window is resized, trigger an event to set papaer offset and dimension
   *  and limit the event to at most one every 30ms.
   *
   * When user open the result panel and resize, the paper will resize to the size relative
   *  to the result panel, therefore we also need to listen to the event from opening
   *  and closing of the result panel.
   */
  private handleWindowResize(): void {
    // when the window is resized (limit to at most one event every 30ms).
    merge(fromEvent(window, "resize").pipe(auditTime(30)))
      .pipe(untilDestroyed(this))
      .subscribe(() => this.paper.setDimensions(this.editorWrapper.offsetWidth, this.editorWrapper.offsetHeight));
  }

  private handleCellHighlight(): void {
    this.handleHighlightMouseDBClickInput();
    this.handleHighlightMouseInput();
    this.handleElementHightlightEvent();
  }

  private handleDisableOperator(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getDisabledOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newDisabled.concat(event.newEnabled).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorDisableStatus(this.paper, op);
        });
      });
  }

  private handleViewOperatorResult(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getViewResultOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newViewResultOps.concat(event.newUnviewResultOps).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorViewResultStatus(this.paper, op, op.viewResult);
        });
      });
  }

  private handleReuseCacheOperator(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getReuseCacheOperatorsChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.newReuseCacheOps.concat(event.newUnreuseCacheOps).forEach(opID => {
          const op = this.workflowActionService.getTexeraGraph().getOperator(opID);
          this.jointUIService.changeOperatorReuseCacheStatus(this.paper, op);
        });
      });
  }

  private registerOperatorDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDisplayName }) => {
        const op = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
        this.jointUIService.changeOperatorJointDisplayName(op, this.paper, newDisplayName);
      });
  }

  private registerPortDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getPortDisplayNameChangedSubject()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, portID, newDisplayName }) => {
        const operatorJointElement = <joint.dia.Element>this.workflowActionService.getJointGraph().getCell(operatorID);
        operatorJointElement.portProp(portID, "attrs/.port-label", {
          text: newDisplayName,
        });
      });
  }

  private handleHighlightMouseDBClickInput(): void {
    // on user mouse double-clicks a comment box, open that comment box
    // on user mouse double-clicks an operator, highlight it and open result panel
    fromJointPaperEvent(this.paper, "cell:pointerdblclick")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const clickedElement = event[0].model;
        if (clickedElement.isElement()) {
          const elementID = clickedElement.id.toString();
          this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);

          if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.openCommentBox(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.openResultPanel();
          }
        }
      });
  }

  /**
   * Handles user mouse down events to trigger logically highlight and unhighlight an operator or group.
   * If user clicks the operator/group while pressing the shift key, multiselect mode is turned on.
   * When pressing the shift key, user can unhighlight a highlighted operator/group by clicking on it.
   * User can also unhighlight all operators and groups by clicking on the blank area of the graph.
   */
  private handleHighlightMouseInput(): void {
    // on user mouse clicks an operator/group cell, highlight that operator/group
    // operator status tooltips should never be highlighted
    merge(fromJointPaperEvent(this.paper, "cell:pointerdown"), fromJointPaperEvent(this.paper, "cell:contextmenu"))
      // event[0] is the JointJS CellView; event[1] is the original JQuery Event
      .pipe(
        filter(event => event[0].model.isElement()),
        filter(
          event =>
            this.workflowActionService.getTexeraGraph().hasOperator(event[0].model.id.toString()) ||
            this.workflowActionService.getTexeraGraph().hasCommentBox(event[0].model.id.toString())
        )
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // multiselect mode on if holding shift
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);

        const elementID = event[0].model.id.toString();
        const highlightedOperatorIDs = this.wrapper.getCurrentHighlightedOperatorIDs();
        const highlightedCommentBoxIDs = this.wrapper.getCurrentHighlightedCommentBoxIDs();
        if (event[1].shiftKey) {
          // if in multiselect toggle highlights on click
          if (highlightedOperatorIDs.includes(elementID)) {
            this.workflowActionService.unhighlightOperators(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.highlightOperators(<boolean>event[1].shiftKey, elementID);
          }
          if (highlightedCommentBoxIDs.includes(elementID)) {
            this.wrapper.unhighlightCommentBoxes(elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.workflowActionService.highlightCommentBoxes(<boolean>event[1].shiftKey, elementID);
          }
          // if in the multiselect mode, also highlight the links in between two highlighted operators
          const allLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
          const linksToBeHighlighted: string[] = allLinks
            .filter(link => {
              const currentHighlightedOperatorIDs = this.wrapper.getCurrentHighlightedOperatorIDs();
              for (let sourceOperatorID of currentHighlightedOperatorIDs) {
                // first make sure the link is not already highlighted
                if (!(link.linkID in this.wrapper.getCurrentHighlightedLinkIDs)) {
                  if (sourceOperatorID === link.source.operatorID) {
                    // iterate through all the other highlighted operators
                    for (let targetOperatorID of currentHighlightedOperatorIDs.filter(
                      each => each != sourceOperatorID
                    )) {
                      if (targetOperatorID === link.target.operatorID) {
                        return true;
                      }
                    }
                  }
                }
              }
            })
            .map(link => link.linkID);
          this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, ...linksToBeHighlighted);
        } else {
          // else only highlight a single operator or group
          if (this.workflowActionService.getTexeraGraph().hasOperator(elementID)) {
            this.workflowActionService.highlightOperators(<boolean>event[1].shiftKey, elementID);
          } else if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementID)) {
            this.wrapper.highlightCommentBoxes(elementID);
          }
        }
      });

    // on user mouse clicks on blank area, unhighlight all operators and groups
    merge(fromJointPaperEvent(this.paper, "blank:pointerdown"), fromJointPaperEvent(this.paper, "blank:contextmenu"))
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.wrapper.unhighlightElements(this.wrapper.getCurrentHighlights());
      });
  }

  private handleElementHightlightEvent(): void {
    // handle logical operator and group highlight / unhighlight events to let JointJS
    //  use our own custom highlighter
    const highlightOptions = {
      name: "stroke",
      options: {
        attrs: {
          "stroke-width": 2,
          stroke: "#4A95FF",
        },
      },
    };

    // highlight on OperatorHighlightStream or GroupHighlightStream or CommentBoxHighlightStream
    merge(
      this.wrapper.getJointOperatorHighlightStream(),
      this.wrapper.getJointGroupHighlightStream(),
      this.wrapper.getJointCommentBoxHighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID => {
          this.paper.findViewByModel(elementID).highlight("rect.body", { highlighter: highlightOptions });
        })
      );

    // unhighlight on OperatorUnhighlightStream or GroupUnhighlightStream or CommentBoxUnhighlightStream
    merge(
      this.wrapper.getJointOperatorUnhighlightStream(),
      this.wrapper.getJointGroupUnhighlightStream(),
      this.wrapper.getJointCommentBoxUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(elementIDs =>
        elementIDs.forEach(elementID => {
          const elem = this.paper.findViewByModel(elementID);
          if (elem !== undefined) {
            elem.unhighlight("rect.body", { highlighter: highlightOptions });
          }
        })
      );
  }

  private handlePortHighlightEvent(): void {
    this.wrapper
      .getJointPortHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(operatorPortIDs => {
        operatorPortIDs.forEach(operatorPortID => {
          const operatorJointElement = <joint.dia.Element>(
            this.workflowActionService.getJointGraph().getCell(operatorPortID.operatorID)
          );
          operatorJointElement.portProp(operatorPortID.portID, "attrs/.port-body", {
            r: 8,
            stroke: "#4A95FF",
            "stroke-width": 3,
          });
        });
      });

    this.wrapper
      .getJointPortUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(operatorPortIDs => {
        operatorPortIDs.forEach(operatorPortID => {
          const operatorJointElement = <joint.dia.Element>(
            this.workflowActionService.getJointGraph().getCell(operatorPortID.operatorID)
          );
          operatorJointElement.portProp(operatorPortID.portID, "attrs/.port-body", {
            r: 5,
            stroke: "none",
          });
        });
      });
  }

  private openCommentBox(commentBoxID: string): void {
    const commentBox = this.workflowActionService.getTexeraGraph().getSharedCommentBoxType(commentBoxID);
    const modalRef: NzModalRef = this.nzModalService.create({
      // modal title
      nzTitle: "Comments",
      nzContent: NzModalCommentBoxComponent,
      // set component @Input attributes
      nzData: { commentBox: commentBox }, // set the index value and page size to the modal for navigation
      // prevent browser focusing close button (ugly square highlight)
      nzAutofocus: null,
      // modal footer buttons
      nzFooter: null,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => {
      this.wrapper.unhighlightCommentBoxes(commentBoxID);
      this.setURLFragment(null);
    });
  }

  private handleOperatorSuggestionHighlightEvent(): void {
    const highlightOptions = {
      name: "stroke",
      options: {
        attrs: {
          "stroke-width": 5,
          stroke: "#551A8B70",
        },
      },
    };

    this.dragDropService
      .getOperatorSuggestionHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(value => this.paper.findViewByModel(value).highlight("rect.body", { highlighter: highlightOptions }));

    this.dragDropService
      .getOperatorSuggestionUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.paper.findViewByModel(value).unhighlight("rect.body", { highlighter: highlightOptions })
      );
  }

  /**
   * Handles the event where the Delete button is clicked for an Operator,
   *  and call workflowAction to delete the corresponding operator.
   *
   * JointJS doesn't have delete button built-in with an operator element,
   *  the delete button is Texera's own customized element.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `element:delete`.
   * The name of this callback event is registered in `JointUIService.getCustomOperatorStyleAttrs`
   */
  private handleViewDeleteOperator(): void {
    // bind the delete button event to call the delete operator function in joint model action
    fromJointPaperEvent(this.paper, "element:delete")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.deleteOperator(elementView.model.id.toString());
        }
        if (this.workflowActionService.getTexeraGraph().hasCommentBox(elementView.model.id.toString())) {
          this.workflowActionService.deleteCommentBox(elementView.model.id.toString());
        }
      });
  }

  private handleViewAddPort(): void {
    fromJointPaperEvent(this.paper, "element:add-input-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.addPort(elementView.model.id.toString(), true, false);
        }
      });
    fromJointPaperEvent(this.paper, "element:add-output-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.addPort(elementView.model.id.toString(), false);
        }
      });
  }

  private handleViewRemovePort(): void {
    fromJointPaperEvent(this.paper, "element:remove-input-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.removePort(elementView.model.id.toString(), true);
        }
      });
    fromJointPaperEvent(this.paper, "element:remove-output-port")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        if (this.workflowActionService.getTexeraGraph().hasOperator(elementView.model.id.toString())) {
          this.workflowActionService.removePort(elementView.model.id.toString(), false);
        }
      });
  }

  private handlePortClick(): void {
    fromJointPaperEvent(this.paper, "element:magnet:pointerclick")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // set the multi-select mode
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);

        const clickedPortID: LogicalPort = {
          operatorID: event[0].model.id as string,
          portID: event[2].getAttribute("port") as string,
        };

        if (event[1].shiftKey) {
          if (_.find(this.wrapper.getCurrentHighlightedPortIDs(), clickedPortID) !== undefined) {
            // if the link being clicked is already highlighted, unhighlight it
            this.workflowActionService.unhighlightPorts(clickedPortID);
          } else if (this.workflowActionService.getTexeraGraph().hasOperator(clickedPortID.operatorID)) {
            // highlight the link if the link has not already been highlighted
            this.workflowActionService.highlightPorts(<boolean>event[1].shiftKey, clickedPortID);
          }
        } else {
          // if user doesn't click on the shift key, highlight only a single port
          if (this.workflowActionService.getTexeraGraph().hasOperator(clickedPortID.operatorID)) {
            this.workflowActionService.highlightPorts(<boolean>event[1].shiftKey, clickedPortID);
          }
        }
      });
  }

  private handleOperatorSelectionEvents(): void {
    fromJointPaperEvent(this.paper, "element:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const operatorID = event[0].model.id.toString();

        if (this.currentOpenedOperatorID !== null && this.paper.getModelById(this.currentOpenedOperatorID)) {
          this.jointUIService.foldOperatorDetails(this.paper, this.currentOpenedOperatorID);
        }

        this.currentOpenedOperatorID = operatorID;
        this.jointUIService.unfoldOperatorDetails(this.paper, operatorID);
      });

    fromJointPaperEvent(this.paper, "element:contextmenu")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const operatorID = event[0].model.id.toString();

        if (this.currentOpenedOperatorID !== null && this.paper.getModelById(this.currentOpenedOperatorID)) {
          this.jointUIService.foldOperatorDetails(this.paper, this.currentOpenedOperatorID);
        }

        this.currentOpenedOperatorID = operatorID;
        this.jointUIService.unfoldOperatorDetails(this.paper, operatorID);
      });

    // Handle right-click on links
    fromJointPaperEvent(this.paper, "link:contextmenu")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const linkID = event[0].model.id.toString();
        // Highlight the link when right-clicked
        this.workflowActionService.highlightLinks(false, linkID);
      });

    fromJointPaperEvent(this.paper, "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.currentOpenedOperatorID !== null && this.paper.getModelById(this.currentOpenedOperatorID)) {
          this.jointUIService.foldOperatorDetails(this.paper, this.currentOpenedOperatorID);
          this.currentOpenedOperatorID = null;
        }
      });
  }

  /**
   * Handles the event where the Delete button is clicked for a Link,
   *  and call workflowAction to delete the corresponding link.
   *
   * We handle link deletion on our own by defining a custom markup.
   * Therefore JointJS doesn't come with default handler for delete an operator,
   *  we need to handle the callback event `tool:remove`.
   */
  private handleViewDeleteLink(): void {
    fromJointPaperEvent(this.paper, "tool:remove")
      .pipe(
        filter(() => this.interactive),
        map(value => value[0])
      )
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        this.workflowActionService.deleteLinkWithID(elementView.model.id.toString());
      });
  }

  /**
   * if the operator is valid , the border of the box will be default
   */
  private handleOperatorValidation(): void {
    this.validationWorkflowService
      .getOperatorValidationStream()
      .pipe(untilDestroyed(this))
      .subscribe(value =>
        this.jointUIService.changeOperatorColor(this.paper, value.operatorID, value.validation.isValid)
      );
  }

  /**
   * This function is provided to JointJS to disable some invalid connections on the UI.
   * If the connection is invalid, users are not able to connect the links on the UI.
   *
   * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateConnection
   */
  private validateJointOperatorConnection(
    sourceView: joint.dia.CellView,
    sourceMagnet: SVGElement | undefined,
    targetView: joint.dia.CellView,
    targetMagnet: SVGElement | undefined,
    end: joint.dia.LinkEnd,
    linkView: joint.dia.LinkView
  ): boolean {
    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute("port-group") === "in") {
      return false;
    }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute("port-group") === "out") {
      return false;
    }

    const sourceCellID = sourceView.model.id.toString();
    const sourcePortID = sourceMagnet?.getAttribute("port");
    const targetCellID = targetView.model.id.toString();
    const targetPortID = targetMagnet?.getAttribute("port");

    return this.validateOperatorConnection(sourceCellID, sourcePortID, targetCellID, targetPortID);
  }

  private validateOperatorConnection(
    sourceCellID: string,
    sourcePortID: string | null | undefined,
    targetCellID: string,
    targetPortID: string | null | undefined
  ): boolean {
    // cannot connect to itself
    if (sourceCellID === targetCellID) {
      return false;
    }

    // must connect to ports
    if (!sourcePortID || !targetPortID) {
      return false;
    }

    // must connect to operators
    if (
      !this.workflowActionService.getTexeraGraph().hasOperator(sourceCellID) ||
      !this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)
    ) {
      return false;
    }

    // find all the links that are connected to the target operator and port
    const connectedLinksToTargetPort = this.workflowActionService
      .getTexeraGraph()
      .getAllLinks()
      .filter(link => link.target.operatorID === targetCellID && link.target.portID === targetPortID);

    // check if this link already exists, duplicate links are not allowed
    const isDuplicateLink =
      connectedLinksToTargetPort.filter(
        link => link.source.operatorID === sourceCellID && link.source.portID === sourcePortID
      ).length > 0;
    if (isDuplicateLink) {
      return false;
    }

    let disallowMultiInput = false;
    if (this.workflowActionService.getTexeraGraph().hasOperator(targetCellID)) {
      const portIndex = this.workflowActionService
        .getTexeraGraph()
        .getOperator(targetCellID)
        .inputPorts.findIndex(p => p.portID === targetPortID);
      if (portIndex >= 0) {
        const portInfo =
          this.dynamicSchemaService.getDynamicSchema(targetCellID).additionalMetadata.inputPorts[portIndex];
        disallowMultiInput = portInfo?.disallowMultiLinks ?? false;
      }
    }
    return !(connectedLinksToTargetPort.length > 0 && disallowMultiInput);
  }

  /**
   * Deletes currently highlighted operators and groups when user presses the delete key.
   * When the focus is not on root document body, operator should not be deleted
   */
  private handleElementDelete(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        filter(event => event.key === "Backspace" || event.key === "Delete")
      )
      .pipe(untilDestroyed(this))
      .subscribe(() => this.deleteElements());
  }

  private deleteElements(): void {
    // Capture all highlighted IDs before starting deletion to avoid modification during iteration
    const highlightedOperatorIDs = Array.from(this.wrapper.getCurrentHighlightedOperatorIDs());
    const highlightedCommentBoxIDs = Array.from(this.wrapper.getCurrentHighlightedCommentBoxIDs());
    const highlightedLinkIDs = Array.from(this.wrapper.getCurrentHighlightedLinkIDs());

    // Bundle all deletions together for proper undo/redo support
    this.workflowActionService.getTexeraGraph().bundleActions(() => {
      // Delete operators and their connected links
      this.workflowActionService.deleteOperatorsAndLinks(highlightedOperatorIDs);

      // Delete standalone selected links
      highlightedLinkIDs.forEach(highlightedLinkID => {
        // Only delete if the link still exists (might have been deleted with operators)
        if (this.workflowActionService.getTexeraGraph().hasLinkWithID(highlightedLinkID)) {
          this.workflowActionService.deleteLinkWithID(highlightedLinkID);
        }
      });

      // Delete comment boxes
      highlightedCommentBoxIDs.forEach(highlightedCommentBoxID =>
        this.workflowActionService.deleteCommentBox(highlightedCommentBoxID)
      );
    });
  }

  /**
   * Highlight all operators and groups on the graph when user presses command/ctrl + A.
   */
  private handleElementSelectAll(): void {
    fromEvent<KeyboardEvent>(document, "keydown")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(event => (event.metaKey || event.ctrlKey) && event.key === "a")
      )
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        event.preventDefault();
        const allOperators = this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID);
        const allLinks = this.workflowActionService
          .getTexeraGraph()
          .getAllLinks()
          .map(link => link.linkID);
        const allCommentBoxes = this.workflowActionService
          .getTexeraGraph()
          .getAllCommentBoxes()
          .map(CommentBox => CommentBox.commentBoxID);
        this.wrapper.setMultiSelectMode(allOperators.length + allCommentBoxes.length > 1);
        this.workflowActionService.highlightLinks(allLinks.length > 1, ...allLinks);
        this.workflowActionService.highlightOperators(allOperators.length > 1, ...allOperators);
        this.workflowActionService.highlightCommentBoxes(
          allOperators.length + allCommentBoxes.length > 1,
          ...allCommentBoxes
        );
      });
  }

  /**
   * Caches the currently highlighted operators' info when user
   * triggers the copy event (i.e. presses command/ctrl + c on
   * keyboard or selects copy option from the browser menu).
   */
  private handleElementCopy(): void {
    fromEvent<ClipboardEvent>(document, "copy")
      .pipe(
        filter(_ => document.activeElement === document.body),
        withLatestFrom(this.operatorMenu.highlightedOperators$, this.operatorMenu.highlightedCommentBoxes$),
        untilDestroyed(this)
      )
      .subscribe(([_, highlightedOperators, highlightedCommentBoxes]) => {
        if (highlightedOperators.length > 0 || highlightedCommentBoxes.length > 0) {
          this.operatorMenu.saveHighlightedElements();
        }
      });
  }

  /**
   * Caches the currently highlighted operators' info and deletes it
   * when user triggers the cut event (i.e. presses command/ctrl + x
   * on keyboard or selects cut option from the browser menu).
   */
  private handleElementCut(): void {
    fromEvent<ClipboardEvent>(document, "cut")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        withLatestFrom(this.operatorMenu.highlightedOperators$, this.operatorMenu.highlightedCommentBoxes$),
        untilDestroyed(this)
      )
      .subscribe(([_, highlightedOperators, highlightedCommentBoxes]) => {
        if (highlightedOperators.length > 0 || highlightedCommentBoxes.length > 0) {
          this.operatorMenu.saveHighlightedElements();
          this.deleteElements();
        }
      });
  }

  /**
   * Pastes the cached operators onto the workflow graph and highlights them
   * when user triggers the paste event (i.e. presses command/ctrl + v on
   * keyboard or selects paste option from the browser menu).
   */
  private handleElementPaste(): void {
    fromEvent<ClipboardEvent>(document, "paste")
      .pipe(
        filter(() => document.activeElement === document.body),
        filter(() => this.interactive),
        untilDestroyed(this)
      )
      .subscribe(() => this.operatorMenu.performPasteOperation());
  }

  /**
   * handle the events of the cursor enter/leave a jointJS link cell
   *
   * Originally, such "hover -> appear" feature came as a default setting with JointJS library
   * However, in order to achieve conditional disappearance for the breakpoint button,
   * every interaction between the cursor and the link tools, including the delete button,
   * need to be handled manually
   */
  private handleLinkCursorHover(): void {
    // When the cursor hovers over a link, the delete button and the breakpoint button appear
    fromJointPaperEvent(this.paper, "link:mouseenter")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(linkView => {
        // Create an array to hold the tools
        const tools: joint.dia.ToolView[] = [new this.removeButton()];

        // If breakpoints are enabled, also add the breakpoint button
        if (this.config.env.linkBreakpointEnabled) {
          tools.push(new this.breakpointButton());
        }

        const toolsView = new joint.dia.ToolsView({ tools });
        linkView.addTools(toolsView);
      });

    /**
     * When the cursor leaves a link, the delete button disappears.
     * If there is no breakpoint present on that link, the breakpoint button also disappears,
     * otherwise, the breakpoint button is not changed.
     */
    fromJointPaperEvent(this.paper, "link:mouseleave")
      .pipe(map(value => value[0]))
      .pipe(untilDestroyed(this))
      .subscribe(elementView => {
        // ensure that the link element exists
        if (this.paper.getModelById(elementView.model.id)) {
          const LinksWithBreakpoint = this.wrapper.getLinkIDsWithBreakpoint();
          if (!LinksWithBreakpoint.includes(elementView.model.id.toString())) {
            this.paper.getModelById(elementView.model.id).findView(this.paper).hideTools();
          }
          this.paper.getModelById(elementView.model.id).attr({
            ".tool-remove": { display: "none" },
          });
        }
      });
  }

  /**
   * handles events/observables related to the breakpoint
   */
  private handleLinkBreakpoint(): void {
    this.handleLinkBreakpointToolAttachment();
    this.handleLinkBreakpointButtonClick();
    this.handleLinkBreakpointHighlightEvents();
    this.handleLinkBreakpointToggleEvents();
  }

  // when a link is added, append a breakpoint link-tool to its LinkView
  private handleLinkBreakpointToolAttachment(): void {
    this.wrapper
      .getJointLinkCellAddStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(link => {
        const linkView = link.findView(this.paper);
        const breakpointButtonTool = this.breakpointButton;
        const breakpointButton = new breakpointButtonTool();
        const toolsView = new joint.dia.ToolsView({
          name: "basic-tools",
          tools: [breakpointButton],
        });
        linkView.addTools(toolsView);
        // tools remain hidden until the cursor hovers over it or a break point is added
        linkView.hideTools();
      });
  }

  /**
   * handles the events of the breakpoint button is clicked for a link
   * and converts that event to a workflow action
   */
  private handleLinkBreakpointButtonClick(): void {
    fromJointPaperEvent(this.paper, "tool:breakpoint")
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        // set the multi-select mode
        this.wrapper.setMultiSelectMode(<boolean>event[1].shiftKey);
        const clickedLinkID = event[0].model.id.toString();
        if (event[1].shiftKey) {
          if (this.wrapper.getCurrentHighlightedLinkIDs().includes(clickedLinkID)) {
            // if the link being clicked is already highlighted, unhighlight it
            this.workflowActionService.unhighlightLinks(clickedLinkID);
          } else if (this.workflowActionService.getTexeraGraph().hasLinkWithID(clickedLinkID)) {
            // highlight the link if the link has not already been highlighted
            this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, clickedLinkID);
          }
        } else {
          // if user doesn't click on the shift key, highlight only a single link
          if (this.workflowActionService.getTexeraGraph().hasLinkWithID(clickedLinkID)) {
            this.workflowActionService.highlightLinks(<boolean>event[1].shiftKey, clickedLinkID);
          }
        }
      });
  }

  /**
   * Highlight/unhighlight the link according to the observable value received.
   */
  private handleLinkBreakpointHighlightEvents(): void {
    this.wrapper
      .getLinkHighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.paper.getModelById(linkID).attr({
            ".connection": { stroke: "orange" },
            ".marker-source": { fill: "orange" },
            ".marker-target": { fill: "orange" },
          });
        });
      });

    this.wrapper
      .getLinkUnhighlightStream()
      .pipe(untilDestroyed(this))
      .subscribe(linkIDs => {
        linkIDs.forEach(linkID => {
          this.paper.findViewByModel(linkID);
          if (this.paper.getModelById(linkID)) {
            // ensure that the link still exist
            this.paper.getModelById(linkID).attr({
              ".connection": { stroke: linkPathStrokeColor },
              ".marker-source": { fill: "none" },
              ".marker-target": { fill: "none" },
            });
          }
        });
      });
  }

  /**
   * show/hide the breakpoint button according to the observable value received
   */
  private handleLinkBreakpointToggleEvents(): void {
    this.wrapper
      .getLinkBreakpointShowStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.paper.getModelById(linkID.linkID).findView(this.paper).showTools();
      });

    this.wrapper
      .getLinkBreakpointHideStream()
      .pipe(this.wrapper.jointGraphContext.bufferWhileAsync, untilDestroyed(this))
      .subscribe(linkID => {
        this.paper.getModelById(linkID.linkID).findView(this.paper).hideTools();
      });
  }

  private isSource(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).inputPorts.length == 0;
  }

  private isSink(operatorID: string): boolean {
    return this.workflowActionService.getTexeraGraph().getOperator(operatorID).outputPorts.length == 0;
  }

  /**
   * Handles mouse events to enable shared cursor.
   */
  private handlePointerEvents(): void {
    fromEvent<MouseEvent>(this.editor, "mousemove")
      .pipe(untilDestroyed(this))
      .subscribe(e => {
        const jointPoint = this.paper.clientToLocalPoint({ x: e.clientX, y: e.clientY });
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("userCursor", jointPoint);
      });
    fromEvent<MouseEvent>(this.editor, "mouseleave")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("isActive", false);
      });
    fromEvent<MouseEvent>(this.editor, "mouseenter")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("isActive", true);
      });
  }

  private setURLFragment(fragment: string | null): void {
    this.router.navigate([], {
      relativeTo: this.route,
      fragment: fragment !== null ? fragment : undefined,
      preserveFragment: false,
    });
  }

  private handleURLFragment(): void {
    // when operator/link/comment box is highlighted/unhighlighted, update URL fragment
    merge(
      this.wrapper.getJointOperatorHighlightStream(),
      this.wrapper.getJointOperatorUnhighlightStream(),
      this.wrapper.getLinkHighlightStream(),
      this.wrapper.getLinkUnhighlightStream(),
      this.wrapper.getJointCommentBoxHighlightStream(),
      this.wrapper.getJointCommentBoxUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // add element ID to URL fragment when only one element is highlighted
        // clear URL fragment when no element or multiple elements are highlighted
        //          from state      -> to state
        // case 1a: no highlighted  -> highlight one element
        // case 1b: more than one elements highlighted -> unhighlight some elements so that only one element is highlighted
        // for case 1: set URL fragment to the highlighted element
        // case 2a: one element highlighted -> unhighlight the element
        // case 2b: one element highlighted -> highlight another element
        // for case 2: clear URL fragment
        // other cases, do nothing
        const highlightedIds = this.wrapper.getCurrentHighlightedIDs();
        if (highlightedIds.length === 1) {
          this.setURLFragment(highlightedIds[0]);
        } else {
          this.setURLFragment(null);
        }
      });

    // special case: open comment box when URL fragment is set
    this.workflowActionService
      .getTexeraGraph()
      .getCommentBoxAddStream()
      .pipe(untilDestroyed(this))
      .subscribe(box => {
        if (this.route.snapshot.fragment === box.commentBoxID) {
          this.openCommentBox(box.commentBoxID);
        }
      });
  }
  invokeResize() {
    const resizeEvent = new Event("resize");
    setTimeout(() => {
      window.dispatchEvent(resizeEvent);
    }, 175);
  }

  /**
   * Handles the center event triggered from the group
   */
  private handleCenterEvent(): void {
    const CENTER_OFFSET_RATIO = 0.15; // Offset ratio used to leave margin when centering
    this.workflowActionService
      .getTexeraGraph()
      .getCenterEventStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflowActionService.calculateTopLeftOperatorPosition();

        const centerCoord = this.workflowActionService.getCenterPoint();
        const offsetX = this.editor.offsetWidth * CENTER_OFFSET_RATIO;
        const offsetY = this.editor.offsetHeight * CENTER_OFFSET_RATIO;

        const targetCoord = {
          x: centerCoord.x - offsetX,
          y: centerCoord.y - offsetY,
        };

        this.paper.translate(-targetCoord.x, -targetCoord.y);
      });
  }

  /**
   * Handle agent hover highlighting to show "viewed", "added", and "modified" labels on operators
   */
  private handleAgentHoverHighlight(): void {
    const setupAgentHoverSubscription = () => {
      this.agentService
        .getAllAgents()
        .pipe(untilDestroyed(this))
        .subscribe(agents => {
          agents.forEach(agent => {
            // Subscribe to each agent's hover operators stream
            this.agentService
              .getHoveredMessageOperatorsObservable(agent.id)
              .pipe(untilDestroyed(this))
              .subscribe(({ viewedOperatorIds, addedOperatorIds, modifiedOperatorIds }) => {
                // Clear all previous labels first
                this.clearAllAgentActionLabels();

                // Show "viewed" labels on viewed operators
                viewedOperatorIds.forEach(operatorId => {
                  if (this.workflowActionService.getTexeraGraph().hasOperator(operatorId)) {
                    this.jointUIService.showAgentActionLabel(this.paper, operatorId, "viewed", agent.name);
                  }
                });

                // Show "added" labels on added operators
                addedOperatorIds.forEach(operatorId => {
                  if (this.workflowActionService.getTexeraGraph().hasOperator(operatorId)) {
                    this.jointUIService.showAgentActionLabel(this.paper, operatorId, "added", agent.name);
                  }
                });

                // Show "modified" labels on modified operators
                modifiedOperatorIds.forEach(operatorId => {
                  if (this.workflowActionService.getTexeraGraph().hasOperator(operatorId)) {
                    this.jointUIService.showAgentActionLabel(this.paper, operatorId, "modified", agent.name);
                  }
                });
              });
          });
        });
    };

    // Subscribe to agent changes to set up hover subscriptions
    this.agentService.agentChange$.pipe(untilDestroyed(this)).subscribe(() => {
      setupAgentHoverSubscription();
    });

    // Initial setup
    setupAgentHoverSubscription();
  }

  /**
   * Clear all agent action labels from all operators
   */
  private clearAllAgentActionLabels(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .forEach(op => {
        this.jointUIService.hideAgentActionLabel(this.paper, op.operatorID);
      });
  }

  /**
   * Handle the chat button click on operators.
   * Opens a chat popover for the operator to interact with agents.
   */
  private handleOperatorChatButton(): void {
    fromJointPaperEvent(this.paper, "element:chat")
      .pipe(
        map(value => value[0]),
        untilDestroyed(this)
      )
      .subscribe(elementView => {
        const operatorId = elementView.model.id.toString();
        if (!this.workflowActionService.getTexeraGraph().hasOperator(operatorId)) {
          return;
        }

        // Toggle chat popover for this operator
        if (this.chatPopoverOperator?.operatorId === operatorId) {
          // Close if clicking the same operator
          this.chatPopoverOperator = null;
        } else {
          // Open chat popover for this operator
          const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorId);
          const operatorSchema = this.dynamicSchemaService.getDynamicSchema(operatorId);
          const displayName =
            operator.customDisplayName ?? operatorSchema?.additionalMetadata.userFriendlyName ?? operator.operatorType;

          const position = this.getOperatorChatPopoverPosition(operatorId);
          if (position) {
            this.chatPopoverOperator = {
              operatorId,
              displayName,
              position,
            };
          }
        }
        this.changeDetectorRef.detectChanges();
      });

    // Close chat popover when clicking on blank area
    fromJointPaperEvent(this.paper, "blank:pointerdown")
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.chatPopoverOperator) {
          this.closeChatPopover();
        }
      });

    // Update chat popover and context positions when operator moves
    this.paper.model.on("change:position", (cell: joint.dia.Cell) => {
      const cellId = cell.id.toString();

      // Update popover position if the chat operator moves
      if (this.chatPopoverOperator && cellId === this.chatPopoverOperator.operatorId) {
        const position = this.getOperatorChatPopoverPosition(this.chatPopoverOperator.operatorId);
        if (position) {
          this.chatPopoverOperator = { ...this.chatPopoverOperator, position };
        }
      }

      this.changeDetectorRef.detectChanges();
    });

    // Update position on zoom/pan
    this.wrapper
      .getWorkflowEditorZoomStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.chatPopoverOperator) {
          const position = this.getOperatorChatPopoverPosition(this.chatPopoverOperator.operatorId);
          if (position) {
            this.chatPopoverOperator = { ...this.chatPopoverOperator, position };
          }
        }

        this.changeDetectorRef.detectChanges();
      });
  }

  /**
   * Get the screen position for the chat popover relative to an operator.
   */
  private getOperatorChatPopoverPosition(operatorId: string): { x: number; y: number } | null {
    const jointCell = this.paper.getModelById(operatorId);
    if (!jointCell) {
      return null;
    }

    const bbox = jointCell.getBBox();
    const scale = this.paper.scale();
    const translate = this.paper.translate();

    // Position popover below the operator, centered horizontally
    // Add extra offset for the display name text below the operator box
    const screenX = (bbox.x + bbox.width / 2) * scale.sx + translate.tx;
    const screenY = (bbox.y + bbox.height) * scale.sy + translate.ty + 40;

    return { x: screenX, y: screenY };
  }

  /**
   * Close the chat popover.
   */
  closeChatPopover(): void {
    this.chatPopoverOperator = null;
    this.changeDetectorRef.detectChanges();
  }

  getOperatorSampleRecords(operatorId: string): Record<string, any>[] | undefined {
    return this.operatorSummaries.get(operatorId)?.sampleRecords;
  }

  getOperatorResultStatistics(operatorId: string): Record<string, string> | undefined {
    return this.operatorSummaries.get(operatorId)?.resultStatistics;
  }

  isOperatorVisualization(operatorId: string): boolean {
    return this.operatorSummaries.get(operatorId)?.sampleRecords?.[0]?.["__is_visualization__"] === true;
  }

  /**
   * Info button on link between operator shown when user hovers over links
   */
  private static getBreakpointButton(): new () => joint.linkTools.Button {
    return joint.linkTools.Button.extend({
      name: "info-button",
      options: {
        markup: [
          {
            tagName: "circle",
            selector: "info-button",
            attributes: {
              r: 10,
              fill: "#001DFF",
              cursor: "pointer",
            },
          },
          {
            tagName: "path",
            selector: "icon",
            attributes: {
              d: "M -2 4 2 4 M 0 3 0 0 M -2 -1 1 -1 M -1 -4 1 -4",
              fill: "none",
              stroke: "#FFFFFF",
              "stroke-width": 2,
              "pointer-events": "none",
            },
          },
        ],
        distance: -60,
        offset: 0,
        action: function (event: JQuery.Event, linkView: joint.dia.LinkView) {
          // when this button is clicked, it triggers an joint paper event
          if (linkView.paper) {
            linkView.paper.trigger("tool:breakpoint", linkView, event);
          }
        },
      },
    });
  }

  /**
   * Remove button on link between operator shown when user hovers over links
   */
  private static RemoveButton: new () => joint.linkTools.Button;

  private static getRemoveButton(): new () => joint.linkTools.Button {
    if (!WorkflowEditorComponent.RemoveButton) {
      WorkflowEditorComponent.RemoveButton = joint.linkTools.Button.extend({
        name: "remove-button",
        options: {
          markup: [
            {
              tagName: "circle",
              selector: "button",
              attributes: {
                r: 9,
                fill: "none",
                stroke: "#D8656A",
                "stroke-width": 2,
                "pointer-events": "visibleFill",
                cursor: "pointer",
              },
            },
            {
              tagName: "path",
              selector: "icon",
              attributes: {
                d: "M -4 -4 L 4 4 M 4 -4 L -4 4",
                fill: "none",
                stroke: "#D8656A",
                "stroke-width": 2,
                "stroke-linecap": "round",
                "pointer-events": "none",
              },
            },
          ],
          distance: -90,
          offset: 0,
          action: function (evt: JQuery.Event, linkView: joint.dia.LinkView) {
            if (linkView.paper) {
              linkView.paper.trigger("tool:remove", linkView, evt);
            }
          },
        },
      });
    }

    return WorkflowEditorComponent.RemoveButton;
  }
}
