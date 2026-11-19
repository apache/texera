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

import { AfterViewInit, Component, Input, OnDestroy, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SafeStyle } from "@angular/platform-browser";
import { isDefined } from "../../../common/util/predicate";
import * as monaco from "monaco-editor";
import { MonacoBreakpoint } from "monaco-breakpoints";
import { UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input/breakpoint-condition-input.component";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { combineLatest } from "rxjs";
import { distinctUntilChanged, map, startWith } from "rxjs/operators";
import { ExecutionState, OperatorState } from "../../types/execute-workflow.interface";

type MonacoEditor = monaco.editor.IStandaloneCodeEditor;
type EditorMouseEvent = monaco.editor.IEditorMouseEvent;
type EditorMouseTarget = monaco.editor.IMouseTargetMargin;
type ModelDecorationOptions = monaco.editor.IModelDecorationOptions;
type Range = monaco.IRange;
const MouseTargetType = monaco.editor.MouseTargetType;

// the states of a run whose workers are up to take a breakpoint
const RUN_STATES_WITH_WORKERS: readonly ExecutionState[] = [
  ExecutionState.Running,
  ExecutionState.Pausing,
  ExecutionState.Paused,
  ExecutionState.Resuming,
  ExecutionState.Recovering,
];

/**
 * This component is the main component for the code debugger.
 */
@UntilDestroy()
@Component({
  selector: "texera-code-debugger",
  templateUrl: "code-debugger.component.html",
  imports: [BreakpointConditionInputComponent],
})
export class CodeDebuggerComponent implements AfterViewInit, OnDestroy, SafeStyle {
  @Input() monacoEditor!: MonacoEditor;
  @Input() currentOperatorId!: string;
  @ViewChild(BreakpointConditionInputComponent) breakpointConditionInput!: BreakpointConditionInputComponent;

  public monacoBreakpoint: MonacoBreakpoint | undefined = undefined;
  public breakpointConditionLine: number | undefined = undefined;

  constructor(
    private udfDebugService: UdfDebugService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {}

  ngAfterViewInit() {
    this.registerStateChangeHandler();
    this.registerBreakpointRenderingHandler();
  }

  ngOnDestroy() {
    // the operator's Monaco model outlives this editor, so what this editor drew has to come off now
    this.removeMonacoBreakpointMethods();
  }

  setupMonacoBreakpointMethods(editor: MonacoEditor) {
    // mimic the enum in monaco-breakpoints
    enum BreakpointEnum {
      Exist,
    }

    this.monacoBreakpoint = new MonacoBreakpoint({
      editor,
      hoverMessage: {
        added: {
          value: "Click to remove the breakpoint.",
        },
        unAdded: {
          value: "Click to add a breakpoint at this line.",
        },
      },
    });
    // override the default createBreakpointDecoration so that it considers
    //  1) hovering breakpoints;
    //  2) exist breakpoints;
    //  3) conditional breakpoints. (conditional breakpoints are also exist breakpoints)
    this.monacoBreakpoint["createBreakpointDecoration"] = (
      range: Range,
      breakpointEnum: BreakpointEnum
    ): { range: Range; options: ModelDecorationOptions } => {
      const condition = this.udfDebugService.getCondition(this.currentOperatorId, range.startLineNumber);

      const isConditional = Boolean(condition?.trim());
      const exists = breakpointEnum === BreakpointEnum.Exist;

      const glyphMarginClassName = exists
        ? isConditional
          ? "monaco-conditional-breakpoint"
          : "monaco-breakpoint"
        : "monaco-hover-breakpoint";

      return { range, options: { glyphMarginClassName } };
    };

    // override the default mouseDownDisposable to handle
    //  1) left click to add/remove breakpoints;
    //  2) right click to open breakpoint condition input.
    this.monacoBreakpoint["mouseDownDisposable"]?.dispose();
    this.monacoBreakpoint["mouseDownDisposable"] = editor.onMouseDown((evt: EditorMouseEvent) => {
      const { type, detail, position } = { ...(evt.target as EditorMouseTarget) };
      const model = editor.getModel()!;
      if (model && type === MouseTargetType.GUTTER_GLYPH_MARGIN) {
        if (detail.isAfterLines) {
          return;
        }
        if (evt.event.leftButton) {
          this.onMouseLeftClick(position.lineNumber);
        } else {
          this.onMouseRightClick(position.lineNumber);
        }
      }
    });
  }

  removeMonacoBreakpointMethods() {
    // a condition entered once breakpoints are off would go to a run that is over
    this.closeBreakpointConditionInput();
    const breakpoint = this.monacoBreakpoint;
    this.monacoBreakpoint = undefined;
    if (!isDefined(breakpoint)) {
      return;
    }
    // MonacoBreakpoint.dispose() lets go of its editor before removing its decorations, so they would
    // stay on the editor's model, which outlives this editor: remove them while the editor is attached.
    breakpoint.clearBreakpoints();
    breakpoint.removeHighlight();
    breakpoint["mouseDownDisposable"]?.dispose();
    breakpoint.dispose();
  }

  /**
   * This function is called when the user left clicks on the gutter of the editor.
   * It adds or removes a breakpoint on the line number that the user clicked on.
   * @param lineNum the line number that the user clicked on
   * @private
   */
  private onMouseLeftClick(lineNum: number) {
    // This indicates that the current position of the mouse is over the total number of lines in the editor
    this.udfDebugService.doModifyBreakpoint(this.currentOperatorId, lineNum);
  }

  /**
   * This function is called when the user right clicks on the gutter of the editor.
   * It opens the breakpoint condition input for the line number that the user clicked on.
   * @param lineNum   the line number that the user clicked on
   * @private
   */
  private onMouseRightClick(lineNum: number) {
    if (!this.monacoBreakpoint!["lineNumberAndDecorationIdMap"].has(lineNum)) {
      return;
    }

    this.breakpointConditionLine = lineNum;
  }

  closeBreakpointConditionInput() {
    this.breakpointConditionLine = undefined;
  }

  /**
   * This function registers a handler that listens to the changes in the lineNumToBreakpointMapping.
   * @private
   */
  private registerBreakpointRenderingHandler() {
    this.udfDebugService.getDebugState(this.currentOperatorId).observe(evt => {
      evt.changes.keys.forEach((change, lineNum) => {
        switch (change.action) {
          case "add":
            const addedValue = evt.target.get(lineNum)!;
            if (isDefined(addedValue.breakpointId)) {
              this.createBreakpointDecoration(Number(lineNum));
            }
            break;
          case "delete":
            const deletedValue = change.oldValue;
            if (isDefined(deletedValue.breakpointId)) {
              this.removeBreakpointDecoration(Number(lineNum));
            }
            break;
          case "update":
            const oldValue = change.oldValue;
            const newValue = evt.target.get(lineNum)!;
            if (newValue.hit) {
              this.monacoBreakpoint?.setLineHighlight(Number(lineNum));
            } else {
              this.monacoBreakpoint?.removeHighlight();
            }
            if (oldValue.condition !== newValue.condition) {
              // recreate the decoration with condition
              this.removeBreakpointDecoration(Number(lineNum));
              this.createBreakpointDecoration(Number(lineNum));
            }
            break;
        }
      });
    });
  }

  // Both do nothing while breakpoints are off (no run in progress, or the operator neither running nor
  // paused); the breakpoints are drawn by rerenderExistingBreakpoints once they are turned on.
  private createBreakpointDecoration(lineNum: number) {
    if (!isDefined(this.monacoBreakpoint)) {
      return;
    }
    this.monacoBreakpoint["createSpecifyDecoration"]({
      startLineNumber: Number(lineNum),
      startColumn: 1,
      endLineNumber: Number(lineNum),
      endColumn: 1,
    });
  }

  private removeBreakpointDecoration(lineNum: number) {
    if (!isDefined(this.monacoBreakpoint)) {
      return;
    }
    const decorationId = this.monacoBreakpoint["lineNumberAndDecorationIdMap"].get(lineNum);
    this.monacoBreakpoint["removeSpecifyDecoration"](decorationId, lineNum);
  }

  rerenderExistingBreakpoints() {
    this.udfDebugService.getDebugState(this.currentOperatorId).forEach(({ breakpointId }, lineNumStr) => {
      if (!isDefined(breakpointId)) {
        return;
      }
      this.createBreakpointDecoration(Number(lineNumStr));
    });
  }

  private registerStateChangeHandler() {
    combineLatest([
      this.workflowStatusService.getStateUpdateStream(),
      // the stream only carries changes, so an editor opened after a run ended starts from the state now
      this.executeWorkflowService.getExecutionStateStream().pipe(
        map(({ current }) => current.state),
        startWith(this.executeWorkflowService.getExecutionState().state)
      ),
    ])
      .pipe(
        // A killed or failed run sends no final status for its operators, and a run still starting has no
        // workers yet, so the run's own state decides too
        map(
          ([operatorStates, executionState]) =>
            RUN_STATES_WITH_WORKERS.includes(executionState) &&
            (operatorStates[this.currentOperatorId] === OperatorState.Running ||
              operatorStates[this.currentOperatorId] === OperatorState.Paused)
        ),
        distinctUntilChanged(),
        untilDestroyed(this)
      )
      .subscribe(enable => {
        // Only enable the breakpoint methods while a run is in progress and the operator is running or paused
        if (enable) {
          this.setupMonacoBreakpointMethods(this.monacoEditor);
          this.rerenderExistingBreakpoints();
        } else {
          // for other states, remove the breakpoint methods
          this.removeMonacoBreakpointMethods();
        }
      });
  }
}
