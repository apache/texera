import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, Renderer2, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { YText } from "yjs/dist/src/types/YText";
import { getWebsocketUrl } from "src/app/common/util/url";
import { MonacoBinding } from "y-monaco";
import { catchError, from, of, Subject, take, timeout } from "rxjs";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

import { MonacoEditorLanguageClientWrapper, UserConfig } from "monaco-editor-wrapper";
import * as monaco from "monaco-editor";
import "@codingame/monaco-vscode-python-default-extension";
import "@codingame/monaco-vscode-r-default-extension";
import "@codingame/monaco-vscode-java-default-extension";
import { isDefined } from "../../../common/util/predicate";
import { editor } from "monaco-editor/esm/vs/editor/editor.api.js";
import { filter, map, switchMap } from "rxjs/operators";
import { EditorMouseEvent, EditorMouseTarget } from "monaco-breakpoints/dist/types";
import { MonacoBreakpoint } from "monaco-breakpoints";
import { BreakpointManager, UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
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


export const LANGUAGE_SERVER_CONNECTION_TIMEOUT_MS = 1000;

/**
 * CodeEditorComponent is the content of the dialogue invoked by CodeareaCustomTemplateComponent.
 *
 * It contains a shared-editable Monaco editor. When the dialogue is invoked by
 * the button in CodeareaCustomTemplateComponent, this component will use the actual y-text of the code within the
 * operator property to connect to the editor.
 *
 */
@UntilDestroy()
@Component({
  selector: "texera-code-editor",
  templateUrl: "code-editor.component.html",
  styleUrls: ["code-editor.component.scss"],
})
export class CodeEditorComponent implements AfterViewInit, SafeStyle, OnDestroy {
  @ViewChild("editor", { static: true }) editorElement!: ElementRef;
  @ViewChild("container", { static: true }) containerElement!: ElementRef;
  @ViewChild(AnnotationSuggestionComponent) annotationSuggestion!: AnnotationSuggestionComponent;
  private code?: YText;

  private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
  private currentOperatorId!: string;
  private breakpointManager: BreakpointManager | undefined;
  public title: string | undefined;
  public formControl!: FormControl;
  public componentRef: ComponentRef<CodeEditorComponent> | undefined;
  public language: string = "";
  public languageTitle: string = "";

  private editorWrapper: MonacoEditorLanguageClientWrapper = new MonacoEditorLanguageClientWrapper();
  private monacoBinding?: MonacoBinding;

  // Boolean to determine whether the suggestion UI should be shown
  public showAnnotationSuggestion: boolean = false;
  // The code selected by the user
  public currentCode: string = "";
  // The result returned by the backend AI assistant
  public currentSuggestion: string = "";
  // The range selected by the user
  public currentRange: monaco.Range | undefined;
  public suggestionTop: number = 0;
  public suggestionLeft: number = 0;
  // For "Add All Type Annotation" to show the UI individually
  private userResponseSubject?: Subject<void>;
  private isMultipleVariables: boolean = false;

  public isUpdatingBreakpoints = false;
  public instance: MonacoBreakpoint | undefined = undefined;
  public lastBreakLine = 0;

  private generateLanguageTitle(language: string): string {
    return `${language.charAt(0).toUpperCase()}${language.slice(1)} UDF`;
  }

  setLanguage(newLanguage: string) {
    this.language = newLanguage;
    this.languageTitle = this.generateLanguageTitle(newLanguage);
  }

  constructor(
    private sanitizer: DomSanitizer,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService,
    public coeditorPresenceService: CoeditorPresenceService,
    private aiAssistantService: AIAssistantService,
    public executeWorkflowService: ExecuteWorkflowService,
    public workflowWebsocketService: WorkflowWebsocketService,
    public udfDebugService: UdfDebugService,
    private renderer: Renderer2,
  ) {
    this.currentOperatorId = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    const operatorType = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).operatorType;

    if (operatorType === "RUDFSource" || operatorType === "RUDF") {
      this.setLanguage("r");
    } else if (
      operatorType === "PythonUDFV2" ||
      operatorType === "PythonUDFSourceV2" ||
      operatorType === "DualInputPortsPythonUDFV2"
    ) {
      this.setLanguage("python");
    } else {
      this.setLanguage("java");
    }
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", true);
    this.title = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).customDisplayName;
    this.code = (
      this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorType(this.currentOperatorId)
        .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>
    ).get("code") as YText;
  }

  ngAfterViewInit() {
    // hacky solution to reset view after view is rendered.
    const style = localStorage.getItem(this.currentOperatorId);
    if (style) this.containerElement.nativeElement.style.cssText = style;
    this.breakpointManager = this.udfDebugService.getOrCreateManager(this.currentOperatorId);

    // start editor
    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(untilDestroyed(this))
      .subscribe((displayParticularVersion: boolean) => {
        if (displayParticularVersion) {
          this.initializeDiffEditor();
        } else {
          this.initializeMonacoEditor();
        }
      });

    this.workflowWebsocketService
      .subscribeToEvent("ConsoleUpdateEvent")
      .pipe(untilDestroyed(this))
      .subscribe((pythonConsoleUpdateEvent: ConsoleUpdateEvent) => {
        const operatorId = pythonConsoleUpdateEvent.operatorId;
        pythonConsoleUpdateEvent.messages
          .filter(consoleMessage => consoleMessage.msgType.name === "DEBUGGER")
          .map(
            consoleMessage => {
              console.log(consoleMessage);
              const pattern = /^Breakpoint (\d+).*\.py:(\d+)\s*$/;
              return consoleMessage.title.match(pattern);
            },
          )
          .filter(isDefined)
          .forEach(match => {
            const breakpoint = Number(match[1]);
            const lineNumber = Number(match[2]);
            this.breakpointManager?.assignBreakpointId(lineNumber, breakpoint);
          });

        if (pythonConsoleUpdateEvent.messages.length === 0) {
          return;
        }
        let lastMsg = pythonConsoleUpdateEvent.messages[pythonConsoleUpdateEvent.messages.length - 1];
        if (lastMsg.title.startsWith("break")) {
          this.lastBreakLine = Number(lastMsg.title.split(" ")[1]);
        }
        if (lastMsg.title.startsWith("*** Blank or comment")) {
          this.isUpdatingBreakpoints = true;
          this.instance!["lineNumberAndDecorationIdMap"].forEach((v: string, k: number, m: Map<number, string>) => {
            if (k === this.lastBreakLine) {
              console.log("removing " + k);
              this.breakpointManager?.removeBreakpoint(k);
              this.instance!["removeSpecifyDecoration"](v, k);
            }
          });
          this.isUpdatingBreakpoints = false;
        }
      });
  }

  private getMouseEventTarget(e: EditorMouseEvent) {
    return { ...(e.target as EditorMouseTarget) };
  }

  showTooltip(mouseX: number, mouseY: number, lineNum: number, breakpointManager: BreakpointManager): void {
    // Create tooltip element
    const tooltip = this.renderer.createElement("div");
    this.renderer.addClass(tooltip, "custom-tooltip");

    // Create header element
    const header = this.renderer.createElement("div");
    this.renderer.addClass(header, "tooltip-header");
    this.renderer.setProperty(header, "innerText", `Condition on line ${lineNum}:`);

    // Create textarea element
    const textarea = this.renderer.createElement("textarea");
    this.renderer.addClass(textarea, "custom-textarea");
    let oldCondition = breakpointManager.getCondition(lineNum);
    this.renderer.setProperty(textarea, "value", oldCondition ?? "");

    // Append header and textarea to tooltip
    this.renderer.appendChild(tooltip, header);
    this.renderer.appendChild(tooltip, textarea);

    // Append tooltip to the document body
    this.renderer.appendChild(document.body, tooltip);
    textarea.focus();
    // Function to remove the tooltip
    const removeTooltip = () => {
      const inputValue = textarea.value;
      if (inputValue != oldCondition) {
        breakpointManager.setCondition(lineNum, inputValue);
      }
      if (removeTooltipListener) {
        removeTooltipListener();
      }
      if (removeFocusoutListener) {
        removeFocusoutListener();
      }
      // Add fade-out class
      this.renderer.addClass(tooltip, "fade-out");
      // Remove tooltip after the transition ends
      const transitionEndListener = this.renderer.listen(tooltip, "transitionend", () => {
        tooltip.remove();
        transitionEndListener();
      });
    };

    // Listen for Enter key press to exit edit mode
    const removeTooltipListener = this.renderer.listen(textarea, "keydown", (event: KeyboardEvent) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        removeTooltip(); // Trigger fade-out and remove the tooltip
      }
    });

    // Listen for focusout event to remove the tooltip after 1 second
    const removeFocusoutListener = this.renderer.listen(textarea, "focusout", () => {
      setTimeout(removeTooltip, 300);
    });

    // Calculate tooltip dimensions after appending to the DOM
    const tooltipRect = tooltip.getBoundingClientRect();

    // Adjust the position to appear at the left side of the mouse
    const adjustedX = mouseX - tooltipRect.width - 10; // Subtracting width and adding some offset to the left
    const adjustedY = mouseY - tooltipRect.height / 2;

    // Update tooltip position
    this.renderer.setStyle(tooltip, "top", `${adjustedY}px`);
    this.renderer.setStyle(tooltip, "left", `${adjustedX}px`);
  }

  ngOnDestroy(): void {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
    localStorage.setItem(this.currentOperatorId, this.containerElement.nativeElement.style.cssText);

    if (isDefined(this.monacoBinding)) {
      this.monacoBinding.destroy();
    }

    this.editorWrapper.dispose(true);

    if (isDefined(this.workflowVersionStreamSubject)) {
      this.workflowVersionStreamSubject.next();
      this.workflowVersionStreamSubject.complete();
    }
  }

  private restoreBreakpoints(instance: MonacoBreakpoint, lineNums: string[]) {
    console.log("trying to restore " + lineNums);
    this.isUpdatingBreakpoints = true;
    instance["lineNumberAndDecorationIdMap"].forEach((v: string, k: number, m: Map<number, string>) => {
      instance["removeSpecifyDecoration"](v, k);
    });
    for (let lineNumber of lineNums) {
      const range: monaco.IRange = {
        startLineNumber: Number(lineNumber),
        endLineNumber: Number(lineNumber),
        startColumn: 0,
        endColumn: 0,
      };
      instance["createSpecifyDecoration"](range);
    }
    this.isUpdatingBreakpoints = false;
  }

  /**
   * Specify the co-editor's cursor style. This step is missing from MonacoBinding.
   * @param coeditor
   */
  public getCoeditorCursorStyles(coeditor: Coeditor) {
    const textCSS =
      "<style>" +
      `.yRemoteSelection-${coeditor.clientId} { background-color: ${coeditor.color?.replace("0.8", "0.5")}}` +
      `.yRemoteSelectionHead-${coeditor.clientId}::after { border-color: ${coeditor.color}}` +
      `.yRemoteSelectionHead-${coeditor.clientId} { border-color: ${coeditor.color}}` +
      "</style>";
    return this.sanitizer.bypassSecurityTrustHtml(textCSS);
  }

  private getFileSuffixByLanguage(language: string): string {
    switch (language.toLowerCase()) {
      case "python":
        return ".py";
      case "r":
        return ".r";
      case "javascript":
        return ".js";
      case "java":
        return ".java";
      default:
        return ".py";
    }
  }

  /**
   * Create a Monaco editor and connect it to MonacoBinding.
   * @private
   */
  private initializeMonacoEditor() {
    const fileSuffix = this.getFileSuffixByLanguage(this.language);
    const userConfig: UserConfig = {
      wrapperConfig: {
        editorAppConfig: {
          $type: "extended",
          codeResources: {
            main: {
              text: this.code?.toString() ?? "",
              uri: `in-memory-${this.currentOperatorId}.${fileSuffix}`,
            },
          },
          userConfiguration: {
            json: JSON.stringify({
              "workbench.colorTheme": "Default Dark Modern",
            }),
          },
        },
      },
    };

    // optionally, configure python language client.
    // it may fail if no valid connection is established, yet the failure would be ignored.
    const languageServerWebsocketUrl = getWebsocketUrl("/python-language-server", "3000");
    if (this.language === "python") {
      userConfig.languageClientConfig = {
        languageId: this.language,
        options: {
          $type: "WebSocketUrl",
          url: languageServerWebsocketUrl,
        },
      };
    }

    // init monaco editor, optionally with attempt on language client.
    from(this.editorWrapper.initAndStart(userConfig, this.editorElement.nativeElement))
      .pipe(
        timeout(LANGUAGE_SERVER_CONNECTION_TIMEOUT_MS),
        switchMap(() => of(this.editorWrapper.getEditor())),
        catchError(() => of(this.editorWrapper.getEditor())),
        filter(isDefined),
        untilDestroyed(this),
      )
      .subscribe((editor: IStandaloneCodeEditor) => {
        editor.updateOptions({ readOnly: this.formControl.disabled });

        if (!this.code) {
          return;
        }
        if (this.monacoBinding) {
          this.monacoBinding.destroy();
        }
        this.monacoBinding = new MonacoBinding(
          this.code,
          editor.getModel()!,
          new Set([editor]),
          this.workflowActionService.getTexeraGraph().getSharedModelAwareness(),
        );
        this.setupAIAssistantActions(editor);
        this.setupDebuggingActions(editor);
      });
  }

  private initializeDiffEditor(): void {
    const fileSuffix = this.getFileSuffixByLanguage(this.language);
    const latestVersionOperator = this.workflowActionService
      .getTempWorkflow()
      ?.content.operators?.find(({ operatorID }) => operatorID === this.currentOperatorId);
    const latestVersionCode: string = latestVersionOperator?.operatorProperties?.code ?? "";
    const oldVersionCode: string = this.code?.toString() ?? "";
    const userConfig: UserConfig = {
      wrapperConfig: {
        editorAppConfig: {
          $type: "extended",
          codeResources: {
            main: {
              text: latestVersionCode,
              uri: `in-memory-${this.currentOperatorId}.${fileSuffix}`,
            },
            original: {
              text: oldVersionCode,
              uri: `in-memory-${this.currentOperatorId}-version.${fileSuffix}`,
            },
          },
          useDiffEditor: true,
          diffEditorOptions: {
            readOnly: true,
          },
          userConfiguration: {
            json: JSON.stringify({
              "workbench.colorTheme": "Default Dark Modern",
            }),
          },
        },
      },
    };

    this.editorWrapper.initAndStart(userConfig, this.editorElement.nativeElement);
  }

  private onMouseLeftClick(e: EditorMouseEvent, editor:IStandaloneCodeEditor) {


      const model = editor.getModel()!;
      const { type, range, detail, position } = this.getMouseEventTarget(e);
      if (model && type === MouseTargetType.GUTTER_GLYPH_MARGIN) {
        // This indicates that the current position of the mouse is over the total number of lines in the editor
        if (detail.isAfterLines) {
          return;
        }
        const lineNumber = position.lineNumber;
        this.udfDebugService.addOrRemoveBreakpoint(this.currentOperatorId, lineNumber);

        const decorationId =
          this.instance!["lineNumberAndDecorationIdMap"].get(lineNumber);

        /**
         * If a breakpoint exists on the current line,
         * it indicates that the current action is to remove the breakpoint
         */
        if (decorationId) {
          this.instance!["removeSpecifyDecoration"](decorationId, lineNumber);
        } else {
          this.instance!["createSpecifyDecoration"](range);
        }


      }
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
            ? (isConditional ? CONDITIONAL_BREAKPOINT_OPTIONS : BREAKPOINT_OPTIONS)
            : BREAKPOINT_HOVER_OPTIONS,
      };
    };
    this.instance["mouseDownDisposable"]?.dispose();

    this.instance["mouseDownDisposable"] = editor.onMouseDown(    (evt: EditorMouseEvent) => {
        if (evt.event.rightButton) {
          return;
        }
      this.onMouseLeftClick(evt, editor);
      }
    );

    (this.instance).on("breakpointChanged", lineNums => {
      if (this.isUpdatingBreakpoints) {
        return;
      }
      this.breakpointManager?.setBreakpoints(lineNums.map(n => String(n)));
      console.log("breakpointChanged: " + lineNums);
    });

    this.breakpointManager?.getBreakpointHitStream()
      .pipe(untilDestroyed(this))
      .subscribe(lineNum => {
        console.log("highlight " + lineNum);
        this.instance!.removeHighlight();
        if (lineNum != 0) {
          this.instance!.setLineHighlight(lineNum);
        }
      });
    this.breakpointManager?.getLineNumToBreakpointMappingStream().pipe(untilDestroyed(this)).subscribe(
      mapping => {
        console.log("trigger" + mapping);
        this.restoreBreakpoints(this.instance!, Array.from(mapping.keys()));
      });

    editor.onContextMenu((e: EditorMouseEvent) => {
      const { type, range, detail, position } = this.getMouseEventTarget(e);
      if (type === MouseTargetType.GUTTER_GLYPH_MARGIN) {

        // This indicates that the current position of the mouse is over the total number of lines in the editor
        if (detail.isAfterLines) {
          return;
        }

        // Get the layout info of the editor
        const layoutInfo = editor.getLayoutInfo()!;

        // Get the range start line number
        const startLineNumber = range.startLineNumber;

        if (!this.instance!["lineNumberAndDecorationIdMap"].has(startLineNumber)) {
          return;
        }
        // Get the top position for the start line number
        const topForLineNumber = editor.getTopForLineNumber(startLineNumber);
        // Calculate the middle y position for the line number
        const lineHeight = editor.getOption(monaco.editor.EditorOption.lineHeight);
        const middleForLineNumber = topForLineNumber + lineHeight / 2;

        // Get the editor's DOM node and its bounding rect
        const editorDomNode = editor.getDomNode()!;
        const editorRect = editorDomNode.getBoundingClientRect();

        // Calculate x and y positions
        const x = editorRect.left + layoutInfo.glyphMarginLeft - editor.getScrollLeft();
        const y = editorRect.top + middleForLineNumber - editor.getScrollTop();

        this.showTooltip(x, y, startLineNumber, this.breakpointManager!);
      }
    });


  }

  private setupAIAssistantActions(editor: IStandaloneCodeEditor) {
    // Check if the AI provider is "openai"
    this.aiAssistantService
      .checkAIAssistantEnabled()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (isEnabled: string) => {
          if (isEnabled === "OpenAI") {
            // "Add Type Annotation" Button
            editor.addAction({
              id: "type-annotation-action",
              label: "Add Type Annotation",
              contextMenuGroupId: "1_modification",
              contextMenuOrder: 1.0,
              run: (editor: monaco.editor.IStandaloneCodeEditor) => {
                // User selected code (including range and content)
                const selection = editor.getSelection();
                const model = editor.getModel();
                if (!model || !selection) {
                  return;
                }
                // All the code in Python UDF
                const allCode = model.getValue();
                // Content of user selected code
                const userSelectedCode = model.getValueInRange(selection);
                // Start line of the selected code
                const lineNumber = selection.startLineNumber;
                this.handleTypeAnnotation(userSelectedCode, selection, editor, lineNumber, allCode);
              },
            });
          }

          // "Add All Type Annotation" Button
          editor.addAction({
            id: "all-type-annotation-action",
            label: "Add All Type Annotations",
            contextMenuGroupId: "1_modification",
            contextMenuOrder: 1.1,
            run: (editor: monaco.editor.IStandaloneCodeEditor) => {
              const selection = editor.getSelection();
              const model = editor.getModel();
              if (!model || !selection) {
                return;
              }

              const selectedCode = model.getValueInRange(selection);
              const allCode = model.getValue();

              this.aiAssistantService
                .locateUnannotated(selectedCode, selection.startLineNumber)
                .pipe(untilDestroyed(this))
                .subscribe(variablesWithoutAnnotations => {
                  // If no unannotated variable, then do nothing.
                  if (variablesWithoutAnnotations.length == 0) {
                    return;
                  }

                  let offset = 0;
                  let lastLine: number | undefined;

                  this.isMultipleVariables = true;
                  this.userResponseSubject = new Subject<void>();

                  const processNextVariable = (index: number) => {
                    if (index >= variablesWithoutAnnotations.length) {
                      this.isMultipleVariables = false;
                      this.userResponseSubject = undefined;
                      return;
                    }

                    const currVariable = variablesWithoutAnnotations[index];

                    const variableCode = currVariable.name;
                    const variableLineNumber = currVariable.startLine;

                    // Update range
                    if (lastLine !== undefined && lastLine === variableLineNumber) {
                      offset += this.currentSuggestion.length;
                    } else {
                      offset = 0;
                    }

                    const variableRange = new monaco.Range(
                      currVariable.startLine,
                      currVariable.startColumn + offset,
                      currVariable.endLine,
                      currVariable.endColumn + offset,
                    );

                    const highlight = editor.createDecorationsCollection([
                      {
                        range: variableRange,
                        options: {
                          hoverMessage: { value: "Argument without Annotation" },
                          isWholeLine: false,
                          className: "annotation-highlight",
                        },
                      },
                    ])!;

                    this.handleTypeAnnotation(variableCode, variableRange, editor, variableLineNumber, allCode);

                    lastLine = variableLineNumber;

                    // Make sure the currVariable will not go to the next one until the user click the accept/decline button
                    if (isDefined(this.userResponseSubject)) {
                      this.userResponseSubject
                        .pipe(take(1)) // Only take one response (accept/decline)
                        .pipe(untilDestroyed(this))
                        .subscribe(() => {
                          highlight.clear();
                          processNextVariable(index + 1);
                        });
                    }
                  };
                  processNextVariable(0);
                });
            },
          });
        },
      });
  }

  private handleTypeAnnotation(
    code: string,
    range: monaco.Range,
    editor: monaco.editor.IStandaloneCodeEditor,
    lineNumber: number,
    allCode: string,
  ): void {
    this.aiAssistantService
      .getTypeAnnotations(code, lineNumber, allCode)
      .pipe(untilDestroyed(this))
      .subscribe((response: TypeAnnotationResponse) => {
        const choices = response.choices || [];
        if (!(choices.length > 0 && choices[0].message && choices[0].message.content)) {
          throw Error("Error: OpenAI response does not contain valid message content " + response);
        }
        this.currentSuggestion = choices[0].message.content.trim();
        this.currentCode = code;
        this.currentRange = range;

        const position = editor.getScrolledVisiblePosition(range.getStartPosition());
        if (position) {
          this.suggestionTop = position.top + 100;
          this.suggestionLeft = position.left + 100;
        }

        this.showAnnotationSuggestion = true;

        if (!this.annotationSuggestion) {
          return;
        }
        this.annotationSuggestion.code = this.currentCode;
        this.annotationSuggestion.suggestion = this.currentSuggestion;
        this.annotationSuggestion.top = this.suggestionTop;
        this.annotationSuggestion.left = this.suggestionLeft;
      });
  }

  // Called when the user clicks the "accept" button
  public acceptCurrentAnnotation(): void {
    // Avoid accidental calls
    if (!this.showAnnotationSuggestion || !this.currentRange || !this.currentSuggestion) {
      return;
    }

    if (this.currentRange && this.currentSuggestion) {
      const selection = new monaco.Selection(
        this.currentRange.startLineNumber,
        this.currentRange.startColumn,
        this.currentRange.endLineNumber,
        this.currentRange.endColumn,
      );

      this.insertTypeAnnotations(this.editorWrapper.getEditor()!, selection, this.currentSuggestion);

      // Only for "Add All Type Annotation"
      if (this.isMultipleVariables && this.userResponseSubject) {
        this.userResponseSubject.next();
      }
    }
    // close the UI after adding the annotation
    this.showAnnotationSuggestion = false;
  }

  // Called when the user clicks the "decline" button
  public rejectCurrentAnnotation(): void {
    // Do nothing except for closing the UI
    this.showAnnotationSuggestion = false;
    this.currentCode = "";
    this.currentSuggestion = "";

    // Only for "Add All Type Annotation"
    if (this.isMultipleVariables && this.userResponseSubject) {
      this.userResponseSubject.next();
    }
  }

  private insertTypeAnnotations(
    editor: monaco.editor.IStandaloneCodeEditor,
    selection: monaco.Selection,
    annotations: string,
  ) {
    const endLineNumber = selection.endLineNumber;
    const endColumn = selection.endColumn;
    const insertPosition = new monaco.Position(endLineNumber, endColumn);
    const insertOffset = editor.getModel()?.getOffsetAt(insertPosition) || 0;
    this.code?.insert(insertOffset, annotations);
  }

  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.currentOperatorId);
  }

  onMouseLeave() {
    if (this.instance) {
      this.instance!["removeHoverDecoration"]();
    }
  }
}
