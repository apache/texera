import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { YText } from "yjs/dist/src/types/YText";
import { getWebsocketUrl } from "src/app/common/util/url";
import { MonacoBinding } from "y-monaco";
import { Subject, take } from "rxjs";
import { takeUntil } from "rxjs/operators";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { isUndefined } from "lodash";
import { FormControl } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

import { MonacoEditorLanguageClientWrapper, UserConfig } from "monaco-editor-wrapper";
import * as monaco from "monaco-editor";
import { from } from "rxjs";
import "@codingame/monaco-vscode-python-default-extension";
import "@codingame/monaco-vscode-r-default-extension";
import "@codingame/monaco-vscode-java-default-extension";

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
  private editor?: any;

  private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
  private operatorID!: string;
  public title: string | undefined;
  public formControl!: FormControl;
  public componentRef: ComponentRef<CodeEditorComponent> | undefined;
  public language: string = "";
  public languageTitle: string = "";

  private wrapper?: MonacoEditorLanguageClientWrapper;
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
  private componentDestroy = new Subject<void>();

  private generateLanguageTitle(language: string): string {
    return `${language.charAt(0).toUpperCase()}${language.slice(1)} UDF`;
  }

  changeLanguage(newLanguage: string) {
    this.language = newLanguage;
    console.log("change to ", newLanguage);
    if (this.editor) {
      monaco.editor.setModelLanguage(this.editor.getModel(), newLanguage);
    }
    this.languageTitle = this.generateLanguageTitle(newLanguage);
  }

  constructor(
    private sanitizer: DomSanitizer,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService,
    public coeditorPresenceService: CoeditorPresenceService,
    private aiAssistantService: AIAssistantService
  ) {
    const currentOperatorId = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    const operatorType = this.workflowActionService.getTexeraGraph().getOperator(currentOperatorId).operatorType;

    if (operatorType === "RUDFSource" || operatorType === "RUDF") {
      this.changeLanguage("r");
    } else if (
      operatorType === "PythonUDFV2" ||
      operatorType === "PythonUDFSourceV2" ||
      operatorType === "DualInputPortsPythonUDFV2"
    ) {
      this.changeLanguage("python");
    } else {
      this.changeLanguage("java");
    }
  }

  ngAfterViewInit() {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", true);
    this.operatorID = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    this.title = this.workflowActionService.getTexeraGraph().getOperator(this.operatorID).customDisplayName;
    const style = localStorage.getItem(this.operatorID);
    if (style) this.containerElement.nativeElement.style.cssText = style;
    this.code = (
      this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorType(this.operatorID)
        .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>
    ).get("code") as YText;

    console.log("added this code ", this.code);

    this.workflowVersionService
      .getDisplayParticularVersionStream()
      .pipe(takeUntil(this.workflowVersionStreamSubject))
      .subscribe((displayParticularVersion: boolean) => {
        if (displayParticularVersion) {
          this.initDiffEditor();
        } else {
          this.initMonaco();
        }
      });
  }

  ngOnDestroy(): void {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
    localStorage.setItem(this.operatorID, this.containerElement.nativeElement.style.cssText);

    if (this.monacoBinding) {
      this.monacoBinding.destroy();
    }

    if (this.wrapper) {
      this.wrapper.dispose(true);
    }

    if (this.editor !== undefined) {
      this.editor.dispose();
    }

    if (!isUndefined(this.workflowVersionStreamSubject)) {
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

  private checkPythonLanguageServerAvailability(): Promise<boolean> {
    return new Promise(resolve => {
      const socket = new WebSocket(getWebsocketUrl("/python-language-server", "3000"));

      socket.onopen = () => {
        socket.close();
        resolve(true);
      };

      socket.onerror = () => {
        resolve(false);
      };
    });
  }

  private initMonaco(): void {
    if (this.wrapper) {
      from(this.wrapper.dispose(true))
        .pipe(takeUntil(this.componentDestroy))
        .subscribe({
          next: () => {
            if (this.componentRef) {
              this.componentRef.destroy();
            }

            this.initializeMonacoEditor();
          },
        });
    } else {
      this.initializeMonacoEditor();
    }
  }

  /**
   * Create a Monaco editor and connect it to MonacoBinding.
   * @private
   */
  private initializeMonacoEditor() {
    if (!this.wrapper && this.code) {
      const fileSuffix = this.getFileSuffixByLanguage(this.language);
      this.wrapper = new MonacoEditorLanguageClientWrapper();

      const userConfig: UserConfig = {
        wrapperConfig: {
          editorAppConfig: {
            $type: "extended",
            codeResources: {
              main: {
                text: this.code.toString(),
                uri: `in-memory-${this.operatorID}.${fileSuffix}`,
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

      from(this.checkPythonLanguageServerAvailability())
        .pipe(takeUntil(this.componentDestroy))
        .subscribe(isServerAvailable => {
          if (isServerAvailable && this.language === "python") {
            userConfig.languageClientConfig = {
              languageId: "python",
              options: {
                $type: "WebSocketUrl",
                url: getWebsocketUrl("/python-language-server", "3000"),
              },
            };
          }

          from(this.wrapper!.initAndStart(userConfig, this.editorElement.nativeElement))
            .pipe(takeUntil(this.componentDestroy))
            .subscribe({
              next: () => {
                this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(() => {
                  const editorInstance = this.wrapper?.getEditor();
                  if (editorInstance) {
                    editorInstance.updateOptions({
                      readOnly: this.formControl.disabled,
                    });
                  }
                });
                this.editor = this.wrapper?.getEditor();
                if (this.code && this.editor) {
                  if (this.monacoBinding) {
                    this.monacoBinding.destroy();
                    this.monacoBinding = undefined;
                  }
                  this.monacoBinding = new MonacoBinding(
                    this.code,
                    this.editor.getModel()!,
                    new Set([this.editor]),
                    this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
                  );
                }
                this.setupAIAssistantActions();
              },
            });
        });
    }
  }

  private setupAIAssistantActions() {
    // Check if the AI provider is "openai"
    this.aiAssistantService
      .checkAIAssistantEnabled()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (isEnabled: string) => {
          if (isEnabled === "OpenAI") {
            // "Add Type Annotation" Button
            this.editor.addAction({
              id: "type-annotation-action",
              label: "Add Type Annotation",
              contextMenuGroupId: "1_modification",
              contextMenuOrder: 1.0,
              run: (ed: monaco.editor.IStandaloneCodeEditor) => {
                // User selected code (including range and content)
                const selection = ed.getSelection();
                const model = ed.getModel();
                if (!model || !selection) {
                  return;
                }
                // All the code in Python UDF
                const allcode = model.getValue();
                // Content of user selected code
                const code = model.getValueInRange(selection);
                // Start line of the selected code
                const lineNumber = selection.startLineNumber;
                this.handleTypeAnnotation(
                  code,
                  selection,
                  ed as monaco.editor.IStandaloneCodeEditor,
                  lineNumber,
                  allcode
                );
              },
            });
          }

          // "Add All Type Annotation" Button
          this.editor.addAction({
            id: "all-type-annotation-action",
            label: "Add All Type Annotations",
            contextMenuGroupId: "1_modification",
            contextMenuOrder: 1.1,
            run: (ed: monaco.editor.IStandaloneCodeEditor) => {
              const selection = ed.getSelection();
              const model = ed.getModel();
              if (!model || !selection) {
                return;
              }

              const selectedCode = model.getValueInRange(selection);
              const allCode = model.getValue();

              this.aiAssistantService
                .locateUnannotated(selectedCode, selection.startLineNumber)
                .pipe(takeUntil(this.componentDestroy))
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

                    const highlight = this.editor.createDecorationsCollection([
                      {
                        range: variableRange,
                        options: {
                          hoverMessage: { value: "Argument without Annotation" },
                          isWholeLine: false,
                          className: "annotation-highlight",
                        },
                      },
                    ]);

                    this.handleTypeAnnotation(
                      variableCode,
                      variableRange,
                      ed as monaco.editor.IStandaloneCodeEditor,
                      variableLineNumber,
                      allCode
                    );

                    lastLine = variableLineNumber;

                    // Make sure the currVariable will not go to the next one until the user click the accept/decline button
                    if (this.userResponseSubject !== undefined) {
                      const userResponseSubject = this.userResponseSubject;
                      // Only take one response (accept/decline)
                      const subscription = userResponseSubject
                        .pipe(take(1))
                        .pipe(takeUntil(this.componentDestroy))
                        .subscribe(() => {
                          highlight.clear();
                          subscription.unsubscribe();
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
    allcode: string
  ): void {
    this.aiAssistantService
      .getTypeAnnotations(code, lineNumber, allcode)
      .pipe(takeUntil(this.componentDestroy))
      .subscribe({
        next: (response: TypeAnnotationResponse) => {
          const choices = response.choices || [];
          if (choices.length > 0 && choices[0].message && choices[0].message.content) {
            this.currentSuggestion = choices[0].message.content.trim();
            this.currentCode = code;
            this.currentRange = range;

            const position = editor.getScrolledVisiblePosition(range.getStartPosition());
            if (position) {
              this.suggestionTop = position.top + 100;
              this.suggestionLeft = position.left + 100;
            }

            this.showAnnotationSuggestion = true;

            if (this.annotationSuggestion) {
              this.annotationSuggestion.code = this.currentCode;
              this.annotationSuggestion.suggestion = this.currentSuggestion;
              this.annotationSuggestion.top = this.suggestionTop;
              this.annotationSuggestion.left = this.suggestionLeft;
            }
          } else {
            console.error("Error: OpenAI response does not contain valid message content", response);
          }
        },
        error: (error: unknown) => {
          console.error("Error fetching type annotations:", error);
        },
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
      this.insertTypeAnnotations(this.editor, selection, this.currentSuggestion);

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
    annotations: string
  ) {
    const endLineNumber = selection.endLineNumber;
    const endColumn = selection.endColumn;
    const insertPosition = new monaco.Position(endLineNumber, endColumn);
    const insertOffset = editor.getModel()?.getOffsetAt(insertPosition) || 0;
    this.code?.insert(insertOffset, annotations);
  }

  private initDiffEditor(): void {
    if (this.wrapper) {
      from(this.wrapper.dispose(true))
        .pipe(takeUntil(this.componentDestroy))
        .subscribe({
          next: () => {
            if (this.componentRef) {
              this.componentRef.destroy();
            }
            this.initializeDiffEditor();
          },
        });
    } else {
      this.initializeDiffEditor();
    }
  }

  private initializeDiffEditor(): void {
    if (this.code && !this.wrapper) {
      this.wrapper = new MonacoEditorLanguageClientWrapper();
      const fileSuffix = this.getFileSuffixByLanguage(this.language);
      const currentWorkflowVersionCode = this.workflowActionService
        .getTempWorkflow()
        ?.content.operators?.filter(
          operator =>
            operator.operatorID ===
            this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0]
        )?.[0].operatorProperties.code;

      const userConfig: UserConfig = {
        wrapperConfig: {
          editorAppConfig: {
            $type: "extended",
            codeResources: {
              main: {
                text: currentWorkflowVersionCode,
                uri: `in-memory-${this.operatorID}-version.${fileSuffix}`,
              },
              original: {
                text: this.code.toString(),
                uri: `in-memory-${this.operatorID}.${fileSuffix}`,
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

      from(this.checkPythonLanguageServerAvailability())
        .pipe(takeUntil(this.componentDestroy))
        .subscribe(isServerAvailable => {
          if (isServerAvailable && this.language === "python") {
            userConfig.languageClientConfig = {
              languageId: "python",
              options: {
                $type: "WebSocketUrl",
                url: getWebsocketUrl("/python-language-server", "3000"),
              },
            };
          }

          this.wrapper!.initAndStart(userConfig, this.editorElement.nativeElement);
        });
    }
  }

  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.operatorID);
  }
}
