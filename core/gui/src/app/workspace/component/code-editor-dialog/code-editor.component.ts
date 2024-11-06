import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, Type, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { YText } from "yjs/dist/src/types/YText";
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
import { filter, switchMap } from "rxjs/operators";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input/breakpoint-condition-input.component";
import { CodeDebuggerComponent } from "./code-debugger.component";
import { MonacoEditor } from "monaco-breakpoints/dist/types";
import { getWebsocketUrl } from "../../../common/util/url";
import { MonacoBinding } from "y-monaco";

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
  @ViewChild(BreakpointConditionInputComponent) breakpointConditionInput!: BreakpointConditionInputComponent;
  private code?: YText;
  private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
  public currentOperatorId!: string;

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
  public codeDebuggerComponent!: Type<any> | null;
  public editorToPass!: MonacoEditor;

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
    private aiAssistantService: AIAssistantService
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
        untilDestroyed(this)
      )
      .subscribe((editor: MonacoEditor) => {
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
          this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
        );
        this.setupAIAssistantActions(editor);
        this.initCodeDebuggerComponent(editor);
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

  private initCodeDebuggerComponent(editor: MonacoEditor) {
    this.codeDebuggerComponent = CodeDebuggerComponent;
    this.editorToPass = editor;
  }

  private setupAIAssistantActions(editor: MonacoEditor) {
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
              run: (editor: MonacoEditor) => {
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
            run: (editor: MonacoEditor) => {
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
                      currVariable.endColumn + offset
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
                    ]);

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
    editor: MonacoEditor,
    lineNumber: number,
    allCode: string
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
        this.currentRange.endColumn
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

  private insertTypeAnnotations(editor: MonacoEditor, selection: monaco.Selection, annotations: string) {
    const endLineNumber = selection.endLineNumber;
    const endColumn = selection.endColumn;
    const insertPosition = new monaco.Position(endLineNumber, endColumn);
    const insertOffset = editor.getModel()?.getOffsetAt(insertPosition) || 0;
    this.code?.insert(insertOffset, annotations);
  }

  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.currentOperatorId);
  }
}
