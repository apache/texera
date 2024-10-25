import { AfterViewInit, Component, Input, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SafeStyle } from "@angular/platform-browser";
import "@codingame/monaco-vscode-python-default-extension";
import "@codingame/monaco-vscode-r-default-extension";
import "@codingame/monaco-vscode-java-default-extension";
import { isDefined } from "../../../common/util/predicate";
import { editor } from "monaco-editor/esm/vs/editor/editor.api.js";
import {
  EditorMouseEvent,
  EditorMouseTarget,
  ModelDecorationOptions,
  MonacoEditor,
  Range,
} from "monaco-breakpoints/dist/types";
import { MonacoBreakpoint } from "monaco-breakpoints";
import { UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input/breakpoint-condition-input.component";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { distinctUntilChanged, map } from "rxjs/operators";
import { OperatorState } from "../../types/execute-workflow.interface";
import MouseTargetType = editor.MouseTargetType;

/**
 * This component is the main component for the code debugger.
 */
@UntilDestroy()
@Component({
  selector: "texera-code-debugger",
  templateUrl: "code-debugger.component.html",
})
export class CodeDebuggerComponent implements AfterViewInit, SafeStyle {
  @Input() monacoEditor!: MonacoEditor;
  @Input() currentOperatorId!: string;
  @ViewChild(BreakpointConditionInputComponent) breakpointConditionInput!: BreakpointConditionInputComponent;

  public monacoBreakpoint: MonacoBreakpoint | undefined = undefined;
  public breakpointConditionLine: number | undefined = undefined;

  constructor(
    private udfDebugService: UdfDebugService,
    private workflowStatusService: WorkflowStatusService
  ) {}

  ngAfterViewInit() {
    this.registerStatusChangeHandler();
    this.registerBreakpointRenderingHandler();
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
    if (!isDefined(this.monacoBreakpoint)) {
      return;
    }
    this.monacoBreakpoint["mouseDownDisposable"]?.dispose();
    this.monacoBreakpoint.dispose();
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

  private createBreakpointDecoration(lineNum: number) {
    this.monacoBreakpoint!["createSpecifyDecoration"]({
      startLineNumber: Number(lineNum),
      endLineNumber: Number(lineNum),
    });
  }

  private removeBreakpointDecoration(lineNum: number) {
    const decorationId = this.monacoBreakpoint!["lineNumberAndDecorationIdMap"].get(lineNum);
    this.monacoBreakpoint!["removeSpecifyDecoration"](decorationId, lineNum);
  }

  rerenderExistingBreakpoints() {
    this.udfDebugService.getDebugState(this.currentOperatorId).forEach(({ breakpointId }, lineNumStr) => {
      if (!isDefined(breakpointId)) {
        return;
      }
      this.createBreakpointDecoration(Number(lineNumStr));
    });
  }

  private registerStatusChangeHandler() {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(
        map(
          event =>
            event[this.currentOperatorId]?.operatorState === OperatorState.Running ||
            event[this.currentOperatorId]?.operatorState === OperatorState.Paused
        ),
        distinctUntilChanged(),
        untilDestroyed(this)
      )
      .subscribe(enable => {
        console.log("enable", enable);
        // Only enable the breakpoint methods if the operator is running or paused
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
