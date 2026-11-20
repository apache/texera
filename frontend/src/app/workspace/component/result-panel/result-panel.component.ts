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

import {
  ChangeDetectorRef,
  Component,
  ElementRef,
  HostListener,
  OnDestroy,
  OnInit,
  Type,
  ViewChild,
} from "@angular/core";
import { merge } from "rxjs";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecutionState, ExecutionStateInfo } from "../../types/execute-workflow.interface";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { filter } from "rxjs/operators";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isPythonUdf, isSink } from "../../service/workflow-graph/model/workflow-graph";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { ErrorFrameComponent } from "./error-frame/error-frame.component";
import { AiFixFrameComponent } from "./ai-fix/ai-fix-frame.component";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";
import { NzResizeEvent, NzResizableDirective, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { VisualizationFrameContentComponent } from "../visualization-panel-content/visualization-frame-content.component";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { isDefined } from "../../../common/util/predicate";
import { CdkDragEnd, CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { PanelService } from "../../service/panel/panel.service";
import { WorkflowCompilingService } from "../../service/compile-workflow/workflow-compiling.service";
import { CompilationState } from "../../types/workflow-compiling.interface";
import { WorkflowFatalError } from "../../types/workflow-websocket.interface";
import { NzMenuDirective, NzMenuItemComponent, NzMenuDividerDirective } from "ng-zorro-antd/menu";
import { NgClass, NgIf, NgFor, NgComponentOutlet, KeyValuePipe } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";

export const DEFAULT_WIDTH = 800;
export const DEFAULT_HEIGHT = 500;
/**
 * ResultPanelComponent is the bottom level area that displays the
 *  execution result of a workflow after the execution finishes.
 */
@UntilDestroy()
@Component({
  selector: "texera-result-panel",
  templateUrl: "./result-panel.component.html",
  styleUrls: ["./result-panel.component.scss"],
  imports: [
    NzMenuDirective,
    NgClass,
    NgIf,
    NzMenuItemComponent,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzMenuDividerDirective,
    CdkDrag,
    NzResizableDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    CdkDragHandle,
    NzTabsComponent,
    NzTabComponent,
    NgFor,
    NgComponentOutlet,
    NzResizeHandlesComponent,
    KeyValuePipe,
  ],
})
export class ResultPanelComponent implements OnInit, OnDestroy {
  @ViewChild("dynamicComponent")
  componentOutlets!: ElementRef;
  frameComponentConfigs: Map<string, { component: Type<any>; componentInputs: {} }> = new Map();
  protected readonly window = window;
  // The docked panel sits this far above the workspace's bottom edge. The return position is
  // computed against the default height, so the two have to be the same number.
  protected readonly defaultHeight = DEFAULT_HEIGHT;
  id = -1;
  width = DEFAULT_WIDTH;
  height = DEFAULT_HEIGHT;
  operatorTitle = "";
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };

  // the highlighted operator ID for display result table / visualization / breakpoint
  currentOperatorId?: string | undefined;

  previewWorkflowVersion: boolean = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowResultService: WorkflowResultService,
    private workflowVersionService: WorkflowVersionService,
    private changeDetectorRef: ChangeDetectorRef,
    private workflowConsoleService: WorkflowConsoleService,
    private resizeService: PanelResizeService,
    private panelService: PanelService,
    private config: GuiConfigService
  ) {
    this.width = 0;
    this.height = Number(localStorage.getItem("result-panel-height")) || this.height;
    this.resizeService.changePanelSize(this.width, this.height);
  }

  ngOnInit(): void {
    const style = localStorage.getItem("result-panel-style");
    if (style) document.getElementById("result-container")!.style.cssText = style;
    const translates = document.getElementById("result-container")!.style.transform;
    const [xOffset, yOffset, _] = calculateTotalTranslate3d(translates);
    this.returnPosition = { x: -xOffset, y: -yOffset };
    this.updateReturnPosition(DEFAULT_HEIGHT, this.height);
    this.registerAutoRerenderResultPanel();
    this.registerOperatorDisplayNameChangeHandler();
    this.registerAutoOpenResultPanel();
    this.registerResultClearedHandler();
    this.handleResultPanelForVersionPreview();
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.closePanel());
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openPanel();
    });
    this.workflowActionService.resultPanelOpen$.pipe(untilDestroyed(this)).subscribe(open => {
      if (open) {
        this.openPanel();
      } else {
        this.closePanel();
      }
    });
    // Once, for what is already there. Every re-render above waits for an event -- a state change,
    // a highlight, a result arriving -- and none of those follows a hand-over between a workflow's
    // two views: the highlight and the results are kept in root-provided services, so this panel
    // would come up empty beside an operator that is still shown selected, until the next event.
    this.rerenderResultPanel();
  }

  @HostListener("window:beforeunload")
  ngOnDestroy(): void {
    localStorage.setItem("result-panel-width", String(this.width));
    localStorage.setItem("result-panel-height", String(this.height));

    const resultContainer = document.getElementById("result-container");
    if (resultContainer) {
      localStorage.setItem("result-panel-style", resultContainer.style.cssText);
    }
  }

  handleResultPanelForVersionPreview() {
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(untilDestroyed(this))
      .subscribe(displayVersionFlag => {
        this.previewWorkflowVersion = displayVersionFlag;
      });
  }

  registerAutoOpenResultPanel() {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        const currentlyHighlighted = this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs();

        // display panel when execution is completed and highlight sink to show results
        // condition must be (Running -> Completed) to prevent cases like
        //   (Uninitialized -> Completed) (a completed workflow is reloaded)
        if (event.previous.state === ExecutionState.Running && event.current.state === ExecutionState.Completed) {
          const activeSinkOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter(op => isSink(op))
            .filter(op => !op.isDisabled)
            .map(op => op.operatorID);

          if (activeSinkOperators.length > 0) {
            if (!(currentlyHighlighted.length == 1 && activeSinkOperators.includes(currentlyHighlighted[0]))) {
              this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
              this.workflowActionService.getJointGraphWrapper().highlightOperators(activeSinkOperators[0]);
            }
          }
        }

        // display panel and highlight a python UDF operator when workflow starts running
        if (event.current.state === ExecutionState.Running) {
          const activePythonUDFOperators = this.workflowActionService
            .getTexeraGraph()
            .getAllOperators()
            .filter(op => isPythonUdf(op))
            .filter(op => !op.isDisabled)
            .map(op => op.operatorID);

          if (activePythonUDFOperators.length > 0) {
            if (!(currentlyHighlighted.length == 1 && activePythonUDFOperators.includes(activePythonUDFOperators[0]))) {
              this.workflowActionService.getJointGraphWrapper().unhighlightOperators(...currentlyHighlighted);
              this.workflowActionService.getJointGraphWrapper().highlightOperators(activePythonUDFOperators[0]);
            }
          }
        }
      });
  }

  /**
   * Wipe the panel when results are dropped (e.g. switching computing units): a
   * still-highlighted operator isn't re-rendered, so its stale frames would linger.
   */
  registerResultClearedHandler() {
    this.workflowResultService
      .getResultClearedStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.clearResultPanel();
        this.currentOperatorId = undefined;
        this.operatorTitle = "";
        this.changeDetectorRef.detectChanges();
      });
  }

  registerAutoRerenderResultPanel() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(filter(event => ResultPanelComponent.needRerenderOnStateChange(event))),
      this.workflowCompilingService.getCompilationStateInfoChangedStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream(),
      this.workflowResultService.getResultInitiateStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.rerenderResultPanel();
        this.changeDetectorRef.detectChanges();
        this.updateOperatorTitle();
      });

    // A Python UDF that raises while its operator is already selected moves none of the
    // streams above: the execution stays Running and the highlight never moves, so the AI
    // Fix tab would not appear until the user clicked away and back. Only that one tab
    // depends on the console, so this toggles it instead of calling rerenderResultPanel():
    // a full rerender rebuilds every frame, losing the Console frame's type filter, scroll
    // position and debug input, and the Result table, whenever any operator prints.
    this.workflowConsoleService
      .getConsoleMessageUpdateStream()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        // Nothing is remembered between emissions here. syncAiFixFrame compares against the
        // map itself, which rerenderResultPanel writes to as well: a key remembered by the
        // pipe went stale the moment the rerender dropped the tab, so the next error mapped
        // to the same value, read as "no change", and the tab never came back -- in the
        // fix-and-re-run loop this panel is for.
        if (this.syncAiFixFrame()) {
          this.changeDetectorRef.detectChanges();
        }
      });
  }

  /** True when the selected operator reports an error the AI panel can be opened on. */
  private shouldOfferAiFix(): boolean {
    if (!this.config.env.copilotEnabled || this.currentOperatorId === undefined) {
      return false;
    }
    if (!this.hasConsoleError(this.currentOperatorId)) {
      return false;
    }
    // Python UDFs only, because the fix this panel writes is a change to `code`. A JVM
    // operator can report an error the classifier recognises and still have nothing to
    // patch, so offering the tab there spends a model call to reach a refusal.
    const graph = this.workflowActionService.getTexeraGraph();
    return graph.hasOperator(this.currentOperatorId) && isPythonUdf(graph.getOperator(this.currentOperatorId));
  }

  /**
   * Keys the frame tabs by their config object.
   *
   * `keyvalue` rebuilds its pair objects on every change to the map, so without a trackBy
   * every tab is torn down and recreated whenever one is added or removed -- a result table
   * losing its scroll position and paging because a console error appeared elsewhere.
   *
   * By the config object rather than by the tab name: `NgComponentOutlet` only recreates a
   * component when its type changes, so keying by name would also keep every other frame
   * alive across re-renders and operator switches, which is more than this needs to change.
   * A re-render re-`set`s each entry with a fresh object, so those frames are recreated as
   * before, while syncAiFixFrame touching only its own entry leaves the others' identity --
   * and so their components -- untouched.
   */
  public trackFrameByConfig(_index: number, config: { value: unknown }): unknown {
    return config.value;
  }

  /**
   * Adds or removes the AI Fix entry, leaving every other frame untouched.
   *
   * Returns whether it changed anything, so a caller can skip change detection. The decision
   * is made against the entry that is actually there, including which operator it points at:
   * this runs from the console stream and from rerenderResultPanel, and anything remembered
   * outside the map goes stale as soon as the other path writes to it.
   */
  private syncAiFixFrame(): boolean {
    const operatorId = this.shouldOfferAiFix() ? this.currentOperatorId : undefined;
    const existing = this.frameComponentConfigs.get("AI Fix");
    const shown = existing ? (existing.componentInputs as { operatorId?: string }).operatorId : undefined;
    if (operatorId === shown) {
      return false;
    }
    if (operatorId) {
      this.frameComponentConfigs.set("AI Fix", {
        component: AiFixFrameComponent,
        componentInputs: { operatorId },
      });
    } else {
      // Mirrors the Static Error frame's teardown: once the operator no longer reports an
      // error -- a re-run cleared the console, or the selection moved to a healthy operator
      // -- a tab offering to fix it has nothing to act on.
      this.frameComponentConfigs.delete("AI Fix");
    }
    return true;
  }

  rerenderResultPanel(): void {
    // if the workflow on the paper is a version preview then this is a temporary workaround until a future PR
    // TODO: let the results be tied with an execution ID instead of a workflow ID
    if (this.previewWorkflowVersion) {
      return;
    }
    // update highlighted operator
    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const currentHighlightedOperator = highlightedOperators.length === 1 ? highlightedOperators[0] : undefined;

    if (this.currentOperatorId !== currentHighlightedOperator) {
      // clear everything, prepare for state change
      this.clearResultPanel();
      this.currentOperatorId = currentHighlightedOperator;

      if (!this.currentOperatorId) {
        this.operatorTitle = "";
      }
    }

    if (
      this.executeWorkflowService.getExecutionState().state === ExecutionState.Failed ||
      this.workflowCompilingService.getWorkflowCompilationState() === CompilationState.Failed
    ) {
      if (this.currentOperatorId == null) {
        this.displayError(this.currentOperatorId);
      } else {
        const errorMessages = this.getWorkflowFatalErrors(this.currentOperatorId);
        if (errorMessages.length > 0) {
          this.displayError(this.currentOperatorId);
        } else {
          this.frameComponentConfigs.delete("Static Error");
        }
      }
    } else {
      this.frameComponentConfigs.delete("Static Error");
    }

    if (this.currentOperatorId) {
      this.displayResult(this.currentOperatorId);
      const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      if (this.workflowConsoleService.hasConsoleMessages(this.currentOperatorId) || isPythonUdf(operator)) {
        this.displayConsole(this.currentOperatorId, isPythonUdf(operator));
      }
      // A Python UDF exception never becomes a fatal error: the worker pauses and the
      // traceback arrives as an ERROR console message while the execution stays Running,
      // so the Failed branch above never fires for the case this panel exists to fix.
      this.syncAiFixFrame();
    }
  }

  /** True when the operator reported an ERROR console message (e.g. a Python UDF traceback). */
  private hasConsoleError(operatorId: string): boolean {
    return (this.workflowConsoleService.getConsoleMessages(operatorId) ?? []).some(
      message => message.msgType.name === "ERROR"
    );
  }

  clearResultPanel(): void {
    this.frameComponentConfigs.clear();
  }

  displayConsole(operatorId: string, consoleInputEnabled: boolean) {
    this.frameComponentConfigs.set("Console", {
      component: ConsoleFrameComponent,
      componentInputs: { operatorId, consoleInputEnabled },
    });
  }

  displayError(operatorId: string | undefined) {
    this.frameComponentConfigs.set("Static Error", {
      component: ErrorFrameComponent,
      componentInputs: { operatorId },
    });
  }

  displayResult(operatorId: string) {
    const resultService = this.workflowResultService.getResultService(operatorId);
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);
    if (paginatedResultService) {
      // display table result if it has paginated results
      this.frameComponentConfigs.set("Result", {
        component: ResultTableFrameComponent,
        componentInputs: { operatorId },
      });
    } else if (resultService) {
      // display visualization result
      this.frameComponentConfigs.set("Result", {
        component: VisualizationFrameContentComponent,
        componentInputs: { operatorId },
      });
    }
  }

  private getWorkflowFatalErrors(operatorId?: string): readonly WorkflowFatalError[] {
    // first fetch the error messages from the execution state store
    let errorMessages = this.executeWorkflowService.getErrorMessages();
    // then fetch error from the compilation state store
    errorMessages = errorMessages.concat(Object.values(this.workflowCompilingService.getWorkflowCompilationErrors()));
    // finally, if any operatorId is given, filter out those with matched Id
    if (operatorId) {
      errorMessages = errorMessages.filter(err => err.operatorId === operatorId);
    }
    return errorMessages;
  }

  /** Refreshes the title for whichever operator the panel now shows. */
  private updateOperatorTitle(): void {
    if (this.currentOperatorId) {
      this.operatorTitle =
        this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).customDisplayName ?? "";
    }
  }

  /**
   * Registered once from ngOnInit, not from the rerender subscriber: this takes out a
   * subscription without releasing a previous one, so calling it per rerender grows the
   * handler count for as long as the panel lives.
   */
  private registerOperatorDisplayNameChangeHandler(): void {
    this.workflowActionService
      .getTexeraGraph()
      .getOperatorDisplayNameChangedStream()
      .pipe(untilDestroyed(this))
      .subscribe(({ operatorID, newDisplayName }) => {
        if (operatorID === this.currentOperatorId) {
          this.operatorTitle = newDisplayName;
        }
      });
  }

  private static needRerenderOnStateChange(event: {
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }): boolean {
    // transitioning from any state to failed state
    if (event.current.state === ExecutionState.Failed) {
      return true;
    }

    // force refresh after fixing all editing-time errors
    if (event.previous.state === ExecutionState.Failed) {
      return true;
    }

    // transition from uninitialized / completed to anything else indicates a new execution of the workflow
    return event.previous.state === ExecutionState.Uninitialized || event.previous.state === ExecutionState.Completed;
  }

  openPanel() {
    this.height = DEFAULT_HEIGHT;
    this.width = DEFAULT_WIDTH;
    this.resizeService.changePanelSize(this.width, this.height);
  }

  closePanel() {
    this.height = 32.5;
    this.width = 0;
  }

  resetPanelPosition() {
    this.dragPosition = { x: this.returnPosition.x, y: this.returnPosition.y };
  }

  isPanelDocked() {
    return this.returnPosition.x === this.dragPosition.x && this.returnPosition.y === this.dragPosition.y;
  }

  handleStartDrag() {
    let visualizationResult = this.componentOutlets.nativeElement.querySelector("#html-content");
    if (visualizationResult !== null) {
      visualizationResult.style.zIndex = -1;
    }
  }

  handleEndDrag({ source }: CdkDragEnd) {
    /**
     * records the most recent panel location, updating dragPosition when dragging is over
     */
    const { x, y } = source.getFreeDragPosition();
    this.dragPosition = { x: x, y: y };
    let visualizationResult = this.componentOutlets.nativeElement.querySelector("#html-content");
    if (visualizationResult !== null) {
      visualizationResult.style.zIndex = 0;
    }
  }

  onResize({ width, height }: NzResizeEvent) {
    cancelAnimationFrame(this.id);
    this.updateReturnPosition(this.height, height);
    this.id = requestAnimationFrame(() => {
      this.width = width!;
      this.height = height!;
      this.resizeService.changePanelSize(this.width, this.height);
    });
  }

  updateReturnPosition(prevHeight: number, newHeight: number | undefined) {
    /**
     * Updating returnPosition ensures that even if the panel gets resized,it can be docked correctly to the left-bottom corner of the canvas.
     */
    if (!isDefined(newHeight)) return;
    this.returnPosition = { x: this.returnPosition.x, y: this.returnPosition.y + prevHeight - newHeight };
  }
}
