import { AfterViewInit, Component, Input, ViewChild } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { SafeStyle } from "@angular/platform-browser";
import * as monaco from "monaco-editor";
import "@codingame/monaco-vscode-python-default-extension";
import "@codingame/monaco-vscode-r-default-extension";
import "@codingame/monaco-vscode-java-default-extension";
import { isDefined } from "../../../common/util/predicate";
import { editor } from "monaco-editor/esm/vs/editor/editor.api.js";
import { EditorMouseEvent, EditorMouseTarget } from "monaco-breakpoints/dist/types";
import { MonacoBreakpoint } from "monaco-breakpoints";
import { BreakpointManager, UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input/breakpoint-condition-input.component";
import IStandaloneCodeEditor = editor.IStandaloneCodeEditor;
import MouseTargetType = editor.MouseTargetType;
import ModelDecorationOptions = monaco.editor.IModelDecorationOptions;

export type Range = monaco.IRange;

export enum BreakpointEnum {
  Exist,
  Hover,
}

export const CONDITIONAL_BREAKPOINT_OPTIONS: ModelDecorationOptions = {
  glyphMarginClassName: "monaco-conditional-breakpoint",
};

export const BREAKPOINT_OPTIONS: ModelDecorationOptions = {
  glyphMarginClassName: "monaco-breakpoint",
};

export const BREAKPOINT_HOVER_OPTIONS: ModelDecorationOptions = {
  glyphMarginClassName: "monaco-hover-breakpoint",
};


@UntilDestroy()
@Component({
  selector: "texera-code-debugger",
  templateUrl: "code-debugger.component.html",
  // styleUrls: ["code-debugger.component.scss"],
})
export class CodeDebuggerComponent implements AfterViewInit, SafeStyle {
  @Input() monacoEditor!: IStandaloneCodeEditor;
  @Input() currentOperatorId!: string;
  @ViewChild(BreakpointConditionInputComponent) breakpointConditionInput!: BreakpointConditionInputComponent;

  private breakpointManager: BreakpointManager | undefined;

  public instance: MonacoBreakpoint | undefined = undefined;
  public breakpointConditionLine: number | undefined = undefined;
  public breakpointConditionMouseX: number | undefined = undefined;
  public breakpointConditionMouseY: number | undefined = undefined;

  constructor(
    public executeWorkflowService: ExecuteWorkflowService,
    public workflowWebsocketService: WorkflowWebsocketService,
    public udfDebugService: UdfDebugService,
  ) {
  }

  ngAfterViewInit() {
    this.breakpointManager = this.udfDebugService.getOrCreateManager(this.currentOperatorId);
    this.setupDebuggingActions(this.monacoEditor);
    this.registerBreakpointRenderingHandler();
  }

  private setupDebuggingActions(editor: IStandaloneCodeEditor) {
    this.instance = new MonacoBreakpoint({ editor });
    this.instance["createBreakpointDecoration"] = (
      range: Range,
      breakpointEnum: BreakpointEnum,
    ): { options: editor.IModelDecorationOptions; range: Range } => {
      let condition = this.breakpointManager?.getCondition(range.startLineNumber);
      let isConditional = false;
      if (condition && condition !== "") {
        isConditional = true;
      }
      return {
        range,
        options:
          breakpointEnum === BreakpointEnum.Exist
            ? isConditional
              ? CONDITIONAL_BREAKPOINT_OPTIONS
              : BREAKPOINT_OPTIONS
            : BREAKPOINT_HOVER_OPTIONS,
      };
    };

    this.instance["mouseDownDisposable"]?.dispose();

    this.instance["mouseDownDisposable"] = editor.onMouseDown((evt: EditorMouseEvent) => {
      const { type, detail, position } = { ...(evt.target as EditorMouseTarget) };
      const model = editor.getModel()!;
      if (model && type === MouseTargetType.GUTTER_GLYPH_MARGIN) {
        if (detail.isAfterLines) {
          return;
        }
        if (evt.event.rightButton) {
          this.onMouseRightClick(position.lineNumber, editor);
        } else {
          this.onMouseLeftClick(position.lineNumber);
        }
      }
    });


  }

  private onMouseLeftClick(lineNum: number) {
    // This indicates that the current position of the mouse is over the total number of lines in the editor
    this.udfDebugService.doModifyBreakpoint(this.currentOperatorId, lineNum);
  }

  private onMouseRightClick(lineNum: number, editor: IStandaloneCodeEditor) {
    if (!this.instance!["lineNumberAndDecorationIdMap"].has(lineNum)) {
      return;
    }

    const layoutInfo = editor.getLayoutInfo()!;
    const topPixel = editor.getTopForLineNumber(lineNum);
    const lineHeight = editor.getOption(monaco.editor.EditorOption.lineHeight);

    const editorRect = editor.getDomNode()!.getBoundingClientRect();

    const x = editorRect.left + layoutInfo.glyphMarginLeft - editor.getScrollLeft();
    const y = editorRect.top + topPixel + lineHeight / 2 - editor.getScrollTop();

    this.breakpointConditionMouseX = x;
    this.breakpointConditionMouseY = y;
    this.breakpointConditionLine = lineNum;
  }

  closeBreakpointConditionInput() {
    this.breakpointConditionLine = undefined;
  }

  private registerBreakpointRenderingHandler() {
    this.breakpointManager?.getLineNumToBreakpointMapping().observe(evt => {
      evt.changes.keys.forEach((change, lineNum) => {
        switch (change.action) {
          case "add":
            const addedValue = evt.target.get(lineNum)!;
            if (isDefined(addedValue.breakpointId)) {
              console.log("adding a breakpoint at ", lineNum);
              this.instance!["createSpecifyDecoration"]({
                startLineNumber: Number(lineNum),
                endLineNumber: Number(lineNum),
                startColumn: 0,
                endColumn: 0,
              });
            }

            break;
          case "delete":
            const deletedValue = change.oldValue;
            if (isDefined(deletedValue.breakpointId)) {
              console.log("deleting a breakpoint at ", lineNum);
              const decorationId = this.instance!["lineNumberAndDecorationIdMap"].get(Number(lineNum));
              this.instance!["removeSpecifyDecoration"](decorationId, Number(lineNum));
            }
            break;
          case "update":
            // this.setCondition(Number(key), change.oldValue);
            console.log(evt.target.get(lineNum));
            const oldValue = change.oldValue;
            const newValue = evt.target.get(lineNum)!;
            // if old hit is false and the new hit is true, then set the hit line number
            if (newValue.hit) {
              this.instance?.setLineHighlight(Number(lineNum));
            }
            if (!newValue.hit) {
              this.instance?.removeHighlight();
            }
            if (oldValue.condition !== newValue.condition) {
              const decorationId = this.instance!["lineNumberAndDecorationIdMap"].get(Number(lineNum));
              this.instance!["removeSpecifyDecoration"](decorationId, Number(lineNum));
              this.instance!["createSpecifyDecoration"]({
                startLineNumber: Number(lineNum),
                endLineNumber: Number(lineNum),
                startColumn: 0,
                endColumn: 0,
              });
            }
            break;
        }
      });
    });
  }
}
