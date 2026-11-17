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

import { ChangeDetectorRef, Component, HostListener, OnDestroy, OnInit } from "@angular/core";
import { merge } from "rxjs";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ConsoleFrameComponent } from "./console-frame/console-frame.component";
import { PanelResizeService } from "../../service/workflow-result/panel-resize/panel-resize.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { isPythonUdf } from "../../service/workflow-graph/model/workflow-graph";
import { NzResizeEvent, NzResizableDirective, NzResizeHandlesComponent } from "ng-zorro-antd/resizable";
import { calculateTotalTranslate3d } from "../../../common/util/panel-dock";
import { isDefined } from "../../../common/util/predicate";
import { CdkDragEnd, CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { PanelService } from "../../service/panel/panel.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { NzMenuDirective, NzMenuItemComponent, NzMenuDividerDirective } from "ng-zorro-antd/menu";
import { NgClass, NgIf } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { AiFixFrameComponent } from "./ai-fix/ai-fix-frame.component";
import { GuiConfigService } from "../../../common/service/gui-config.service";
import { WorkflowConsoleService } from "../../service/workflow-console/workflow-console.service";

export const DEFAULT_WIDTH = 800;
export const DEFAULT_HEIGHT = 500;
/**
 * ResultPanelComponent is the bottom level area that holds the console. An operator's result is
 * shown beside it on the canvas instead, so this panel shows only what the operators printed and
 * the errors: every operator's, or the highlighted operator's alone.
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
    ConsoleFrameComponent,
    NzResizeHandlesComponent,
    NzTabsComponent,
    NzTabComponent,
    AiFixFrameComponent,
  ],
})
export class ResultPanelComponent implements OnInit, OnDestroy {
  protected readonly window = window;
  // The docked panel sits this far above the workspace's bottom edge. The return position is
  // computed against the default height, so the two have to be the same number.
  protected readonly defaultHeight = DEFAULT_HEIGHT;
  id = -1;
  width = DEFAULT_WIDTH;
  height = DEFAULT_HEIGHT;
  dragPosition = { x: 0, y: 0 };
  returnPosition = { x: 0, y: 0 };

  // the one highlighted operator, whose messages alone the console shows; none shows every operator's
  currentOperatorId?: string;
  // a Python UDF takes debugger commands, so its console has an input
  consoleInputEnabled = false;
  // the operator the AI Fix tab offers to fix; none hides the tab
  aiFixOperatorId?: string;

  constructor(
    private workflowActionService: WorkflowActionService,
    private changeDetectorRef: ChangeDetectorRef,
    private resizeService: PanelResizeService,
    private panelService: PanelService,
    private workflowConsoleService: WorkflowConsoleService,
    private executeWorkflowService: ExecuteWorkflowService,
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
    this.registerFollowHighlight();
    this.registerSelectPythonUdfOnRun();
    this.registerAiFixTab();
    this.panelService.closePanelStream.pipe(untilDestroyed(this)).subscribe(() => this.closePanel());
    this.panelService.resetPanelStream.pipe(untilDestroyed(this)).subscribe(() => {
      this.resetPanelPosition();
      this.openPanel();
    });
    // Once, for what is already there: the highlight is kept in a root-provided service, so after a
    // hand-over between a workflow's two views an operator can already be shown selected.
    this.updateShownOperator();
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

  registerFollowHighlight() {
    merge(
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.updateShownOperator();
        this.changeDetectorRef.detectChanges();
      });
  }

  /** A run selects the first Python UDF, so the console shows its messages and takes debugger commands. */
  registerSelectPythonUdfOnRun() {
    this.executeWorkflowService
      .getExecutionStateStream()
      .pipe(untilDestroyed(this))
      .subscribe(event => {
        if (event.current.state !== ExecutionState.Running) return;
        const pythonUdfs = this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .filter(op => isPythonUdf(op) && !op.isDisabled)
          .map(op => op.operatorID);
        const wrapper = this.workflowActionService.getJointGraphWrapper();
        const highlighted = wrapper.getCurrentHighlightedOperatorIDs();
        if (pythonUdfs.length === 0 || (highlighted.length === 1 && pythonUdfs.includes(highlighted[0]))) return;
        wrapper.unhighlightOperators(...highlighted);
        wrapper.highlightOperators(pythonUdfs[0]);
      });
  }

  /** One highlighted operator narrows the console to it. None, or several, shows every operator. */
  updateShownOperator(): void {
    const highlighted = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    const graph = this.workflowActionService.getTexeraGraph();
    this.currentOperatorId = highlighted.length === 1 && graph.hasOperator(highlighted[0]) ? highlighted[0] : undefined;
    this.consoleInputEnabled =
      isDefined(this.currentOperatorId) && isPythonUdf(graph.getOperator(this.currentOperatorId));
    this.updateAiFixTab();
  }

  /**
   * A Python UDF that raises while it is already selected moves no highlight: the run stays
   * Running and the traceback arrives as an ERROR console message. A new run clears the console
   * without a console update, so the run's state is watched too.
   */
  registerAiFixTab() {
    merge(
      this.workflowConsoleService.getConsoleMessageUpdateStream(),
      this.executeWorkflowService.getExecutionStateStream()
    )
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.updateAiFixTab()) this.changeDetectorRef.detectChanges();
      });
  }

  /**
   * The AI Fix tab is offered on the selected Python UDF while its console has an error, as the fix
   * it writes is a change to the operator's code. Returns whether the tab changed.
   */
  private updateAiFixTab(): boolean {
    const operatorId = this.currentOperatorId;
    const offered =
      this.config.env.copilotEnabled &&
      isDefined(operatorId) &&
      this.consoleInputEnabled &&
      (this.workflowConsoleService.getConsoleMessages(operatorId) ?? []).some(m => m.msgType.name === "ERROR");
    const next = offered ? operatorId : undefined;
    if (next === this.aiFixOperatorId) return false;
    this.aiFixOperatorId = next;
    return true;
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

  handleEndDrag({ source }: CdkDragEnd) {
    /**
     * records the most recent panel location, updating dragPosition when dragging is over
     */
    const { x, y } = source.getFreeDragPosition();
    this.dragPosition = { x: x, y: y };
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
