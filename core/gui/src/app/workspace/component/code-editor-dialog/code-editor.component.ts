import { AfterViewInit, Component, ComponentRef, ElementRef, OnDestroy, ViewChild } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import { YText } from "yjs/dist/src/types/YText";
import { MonacoBinding } from "y-monaco";
import { Subject } from "rxjs";
import { takeUntil } from "rxjs/operators";
import { MonacoLanguageClient } from "monaco-languageclient";
import { toSocket, WebSocketMessageReader, WebSocketMessageWriter } from "vscode-ws-jsonrpc";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { getWebsocketUrl } from "src/app/common/util/url";
import { isUndefined } from "lodash";
import { CloseAction, ErrorAction } from "vscode-languageclient/lib/common/client.js";
import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";
import { FormControl } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";

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
  private languageServerSocket?: WebSocket;
  private workflowVersionStreamSubject: Subject<void> = new Subject<void>();
  private operatorID!: string;
  public title: string | undefined;
  public formControl!: FormControl;
  public componentRef: ComponentRef<CodeEditorComponent> | undefined;
  public language: string = "";
  public languageTitle: string = "";

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
          this.formControl.statusChanges.pipe(untilDestroyed(this)).subscribe(_ => {
            this.editor.updateOptions({
              readOnly: this.formControl.disabled,
            });
          });
        }
      });
  }

  ngOnDestroy(): void {
    this.workflowActionService.getTexeraGraph().updateSharedModelAwareness("editingCode", false);
    localStorage.setItem(this.operatorID, this.containerElement.nativeElement.style.cssText);
    if (
      this.languageServerSocket !== undefined &&
      this.languageServerSocket.readyState === this.languageServerSocket.OPEN
    ) {
      this.languageServerSocket.close();
      this.languageServerSocket = undefined;
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

  /**
   * Create a Monaco editor and connect it to MonacoBinding.
   * @private
   */
  private initMonaco() {
    const editor = monaco.editor.create(this.editorElement.nativeElement, {
      language: this.language,
      fontSize: 11,
      theme: "vs-dark",
      automaticLayout: true,
    });

    if (this.code) {
      new MonacoBinding(
        this.code,
        editor.getModel()!,
        new Set([editor]),
        this.workflowActionService.getTexeraGraph().getSharedModelAwareness()
      );
    }
    this.editor = editor;

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
              run: ed => {
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
        },
      });
    if (this.language == "python") {
      this.connectLanguageServer();
    }
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
      .pipe(takeUntil(this.workflowVersionStreamSubject))
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
  }

  // Add the type annotation into monaco editor
  private insertTypeAnnotations(
    editor: monaco.editor.IStandaloneCodeEditor,
    selection: monaco.Selection,
    annotations: string
  ) {
    const endLineNumber = selection.endLineNumber;
    const endColumn = selection.endColumn;
    const range = new monaco.Range(
      // Insert the content to the end of the selected code
      endLineNumber,
      endColumn,
      endLineNumber,
      endColumn
    );
    const text = `${annotations}`;
    const op = {
      range: range,
      text: text,
      forceMoveMarkers: true,
    };
    editor.executeEdits("add annotation", [op]);
  }

  private connectLanguageServer() {
    if (this.languageServerSocket === undefined) {
      this.languageServerSocket = new WebSocket(getWebsocketUrl("/python-language-server", "3000"));
      this.languageServerSocket.onopen = () => {
        if (this.languageServerSocket !== undefined) {
          const socket = toSocket(this.languageServerSocket);
          const reader = new WebSocketMessageReader(socket);
          const writer = new WebSocketMessageWriter(socket);
          const languageClient = new MonacoLanguageClient({
            name: "Python UDF Language Client",
            clientOptions: {
              documentSelector: ["python"],
              errorHandler: {
                error: () => ({ action: ErrorAction.Continue }),
                closed: () => ({ action: CloseAction.Restart }),
              },
            },
            connectionProvider: { get: () => Promise.resolve({ reader, writer }) },
          });
          languageClient.start();
          reader.onClose(() => languageClient.stop());
        }
      };
    }
  }

  private initDiffEditor() {
    if (this.code) {
      this.editor = monaco.editor.createDiffEditor(this.editorElement.nativeElement, {
        readOnly: true,
        theme: "vs-dark",
        fontSize: 11,
        automaticLayout: true,
      });
      const currentWorkflowVersionCode = this.workflowActionService
        .getTempWorkflow()
        ?.content.operators?.filter(
          operator =>
            operator.operatorID ===
            this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0]
        )?.[0].operatorProperties.code;
      this.editor.setModel({
        original: monaco.editor.createModel(this.code.toString(), "python"),
        modified: monaco.editor.createModel(currentWorkflowVersionCode, "python"),
      });
    }
  }

  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.operatorID);
  }
}
