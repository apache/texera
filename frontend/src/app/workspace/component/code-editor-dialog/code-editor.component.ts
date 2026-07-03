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
  AfterViewInit,
  Component,
  ComponentRef,
  ElementRef,
  HostListener,
  OnDestroy,
  Type,
  ViewChild,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { WorkflowVersionService } from "../../../dashboard/service/user/workflow-version/workflow-version.service";
import type { Text as YText } from "yjs";
import { getWebsocketUrl } from "src/app/common/util/url";
import { MonacoBinding } from "y-monaco";
import { from, Subject, take } from "rxjs";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { AnnotationSuggestionComponent } from "./annotation-suggestion.component";
import * as monaco from "monaco-editor";
import {
  MonacoVscodeApiWrapper,
  type MonacoVscodeApiConfig,
  getEnhancedMonacoEnvironment,
} from "monaco-languageclient/vscodeApiWrapper";
import { LanguageClientWrapper, type LanguageClientConfig } from "monaco-languageclient/lcwrapper";
import { EditorApp, type EditorAppConfig } from "monaco-languageclient/editorApp";
import { isDefined } from "../../../common/util/predicate";
import { filter } from "rxjs/operators";
import { BreakpointConditionInputComponent } from "./breakpoint-condition-input/breakpoint-condition-input.component";
import { CodeDebuggerComponent } from "./code-debugger.component";
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NgFor, NgComponentOutlet, NgIf } from "@angular/common";

type MonacoEditor = monaco.editor.IStandaloneCodeEditor;

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
  imports: [
    CdkDrag,
    CdkDragHandle,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NgFor,
    NgComponentOutlet,
    NgIf,
    AnnotationSuggestionComponent,
  ],
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

  private static apiWrapperStartPromise?: Promise<void>;
  private editorApp?: EditorApp;
  private languageClientWrapper?: LanguageClientWrapper;
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

  // Operator-type → editor language. The R types are kept on the frontend
  // even though Texera's R UDF backend now ships as a separate plugin —
  // when that plugin is installed, the workflow may still surface `RUDF` /
  // `RUDFSource` operators and the editor needs to open them in R mode.
  private static readonly PYTHON_OPERATOR_TYPES: ReadonlySet<string> = new Set([
    "PythonUDFV2",
    "PythonUDFSourceV2",
    "DualInputPortsPythonUDFV2",
  ]);
  private static readonly R_OPERATOR_TYPES: ReadonlySet<string> = new Set(["RUDFSource", "RUDF"]);

  constructor(
    private sanitizer: DomSanitizer,
    private workflowActionService: WorkflowActionService,
    private workflowVersionService: WorkflowVersionService,
    public coeditorPresenceService: CoeditorPresenceService,
    private aiAssistantService: AIAssistantService,
    private config: GuiConfigService
  ) {
    this.currentOperatorId = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()[0];
    const operatorType = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId).operatorType;
    if (CodeEditorComponent.PYTHON_OPERATOR_TYPES.has(operatorType)) {
      this.language = "python";
    } else if (CodeEditorComponent.R_OPERATOR_TYPES.has(operatorType)) {
      this.language = "r";
    } else {
      this.language = "java";
    }
    this.languageTitle = `${this.language[0].toUpperCase()}${this.language.slice(1)} UDF`;
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

    this.monacoBinding?.destroy();
    this.languageClientWrapper?.dispose().catch(() => {});
    this.languageClientWrapper = undefined;
    this.editorApp?.dispose().catch(() => {});
    this.editorApp = undefined;

    this.workflowVersionStreamSubject.next();
    this.workflowVersionStreamSubject.complete();
  }

  /**
   * Specify the co-editor's cursor style. This step is missing from MonacoBinding.
   *
   * `coeditor.clientId` and `coeditor.color` come from yjs awareness state,
   * which any peer can write to. Both are interpolated into a `<style>` tag
   * passed through `bypassSecurityTrustHtml`, so anything that escapes the
   * tag would land in the page as raw HTML. Validate both to a tight
   * allow-list (digits-only id, hex / `rgb(a)` / `hsl(a)` colour) and bail
   * out otherwise; nothing else should reach the sanitiser.
   * @param coeditor
   */
  public getCoeditorCursorStyles(coeditor: Coeditor) {
    if (!CodeEditorComponent.SAFE_CLIENT_ID.test(coeditor.clientId)) {
      return this.sanitizer.bypassSecurityTrustHtml("");
    }
    if (!coeditor.color || !CodeEditorComponent.SAFE_CSS_COLOR.test(coeditor.color)) {
      return this.sanitizer.bypassSecurityTrustHtml("");
    }
    const id = coeditor.clientId;
    const color = coeditor.color;
    const selectionBg = color.replace("0.8", "0.5");
    return this.sanitizer.bypassSecurityTrustHtml(
      "<style>" +
        `.yRemoteSelection-${id} { background-color: ${selectionBg}}` +
        `.yRemoteSelectionHead-${id}::after { border-color: ${color}}` +
        `.yRemoteSelectionHead-${id} { border-color: ${color}}` +
        "</style>"
    );
  }

  // Allow-lists for the two awareness-derived values that flow into a `<style>`
  // tag in `getCoeditorCursorStyles`. yjs serialises clientIDs as the decimal
  // form of a 32-bit integer, and the colours we generate elsewhere only use
  // these notations — anything outside these patterns is rejected.
  private static readonly SAFE_CLIENT_ID = /^\d{1,10}$/;
  private static readonly SAFE_CSS_COLOR = /^(?:#[0-9a-fA-F]{3,8}|rgba?\([\d.,\s]+\)|hsla?\([\d.,%\s]+\))$/;

  /**
   * Lazily start the global monaco-vscode-api wrapper. The vscode API services are
   * a process-wide singleton in v10; calling start() twice would throw, so we share
   * a single Promise across every CodeEditorComponent instance.
   */
  private static async ensureVscodeApiStarted(): Promise<void> {
    CodeEditorComponent.apiWrapperStartPromise ??= (async () => {
      try {
        const apiConfig: MonacoVscodeApiConfig = {
          $type: "extended",
          viewsConfig: { $type: "EditorService" },
          userConfiguration: {
            json: JSON.stringify({ "workbench.colorTheme": "Default Dark Modern" }),
          },
          // Wire workers via thin trampolines in `./workers/`. Webpack 5 only
          // treats `new Worker(new URL("./relative", import.meta.url))` as a
          // worker entry point and bundles the dep tree into a chunk; bare
          // package URLs `new URL("@codingame/...", import.meta.url)` become
          // static assets whose relative imports 404 at runtime. Esbuild
          // (used by @angular/build:unit-test) also requires a real on-disk
          // file for the URL spec — trampolines satisfy both bundlers.
          monacoWorkerFactory: () => {
            const env = getEnhancedMonacoEnvironment();
            env.getWorker = (_workerId: string, label: string): Worker => {
              switch (label) {
                case "editorWorkerService":
                  return new Worker(new URL("./workers/editor.worker", import.meta.url), { type: "module" });
                case "extensionHostWorkerMain":
                  return new Worker(new URL("./workers/extension-host.worker", import.meta.url), { type: "module" });
                case "TextMateWorker":
                  return new Worker(new URL("./workers/textmate.worker", import.meta.url), { type: "module" });
                default:
                  throw new Error(`No worker configured for label: ${label}`);
              }
            };
          },
        };
        await new MonacoVscodeApiWrapper(apiConfig).start();

        // Load AND fully activate the default language extensions. Each
        // module exports a `whenReady()` that resolves after its TextMate
        // grammar / configuration files are registered with the host —
        // without waiting, the editor opens with every token rendered as
        // the default `mtk1` class (no syntax colours). Dynamic `import(...)`
        // is used so the Angular build pipeline doesn't tree-shake the
        // side-effect imports.
        const extensions = await Promise.all([
          import("@codingame/monaco-vscode-python-default-extension"),
          import("@codingame/monaco-vscode-java-default-extension"),
        ]);
        await Promise.all(extensions.map(ext => ext.whenReady?.()));
      } catch (err) {
        // Clear the cached promise so a later editor open can retry; without
        // this the rejected promise would be returned forever and every
        // subsequent open would fail with the same error.
        CodeEditorComponent.apiWrapperStartPromise = undefined;
        throw err;
      }
    })();
    return CodeEditorComponent.apiWrapperStartPromise;
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
    const editorAppConfig: EditorAppConfig = {
      codeResources: {
        modified: {
          text: this.code?.toString() ?? "",
          uri: `in-memory-${this.currentOperatorId}${fileSuffix}`,
        },
      },
    };

    const languageServerWebsocketUrl = getWebsocketUrl(
      "/python-language-server",
      this.config.env.pythonLanguageServerPort
    );

    const startEditor = async (): Promise<MonacoEditor | undefined> => {
      await CodeEditorComponent.ensureVscodeApiStarted();
      this.editorApp = new EditorApp(editorAppConfig);
      await this.editorApp.start(this.editorElement.nativeElement);

      // Configure the python language client as a best-effort step — a
      // missing or unreachable language server should not block the editor
      // from being usable. The timeout / catch is scoped tightly around
      // `languageClientWrapper.start()` so the editor mount above is always
      // awaited to completion (codingame v25 first-load init can take
      // multiple seconds and easily exceed the LSP timeout).
      if (this.language === "python") {
        const lcConfig: LanguageClientConfig = {
          languageId: this.language,
          connection: {
            options: { $type: "WebSocketUrl", url: languageServerWebsocketUrl },
          },
          clientOptions: { documentSelector: [this.language] },
        };
        this.languageClientWrapper = new LanguageClientWrapper(lcConfig);
        try {
          await Promise.race([
            this.languageClientWrapper.start(),
            new Promise<never>((_, reject) =>
              setTimeout(
                () => reject(new Error("Language server connection timed out")),
                LANGUAGE_SERVER_CONNECTION_TIMEOUT_MS
              )
            ),
          ]);
        } catch {
          // Editor stays usable without the LSP.
        }
      }
      return this.editorApp.getEditor();
    };

    from(startEditor())
      .pipe(filter(isDefined), untilDestroyed(this))
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
        // The TextMate grammar registers asynchronously (the host fetches the
        // .tmLanguage.json from the codingame extension and spins up a worker).
        // Even after the editor mounts, the model may have already been painted
        // with the no-grammar tokens (everything as the default `mtk1` class).
        // Force a re-tokenize so the syntax colours show up on first paint.
        const model = editor.getModel();
        const tokenization = (
          model as unknown as {
            tokenization?: { forceTokenization?: (line: number) => void };
          }
        )?.tokenization;
        if (model && tokenization?.forceTokenization) {
          for (let line = 1; line <= model.getLineCount(); line++) {
            tokenization.forceTokenization(line);
          }
        }
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
    const editorAppConfig: EditorAppConfig = {
      codeResources: {
        modified: {
          text: latestVersionCode,
          uri: `in-memory-${this.currentOperatorId}${fileSuffix}`,
        },
        original: {
          text: oldVersionCode,
          uri: `in-memory-${this.currentOperatorId}-version${fileSuffix}`,
        },
      },
      useDiffEditor: true,
      diffEditorOptions: {
        readOnly: true,
      },
    };

    const startDiffEditor = async () => {
      await CodeEditorComponent.ensureVscodeApiStarted();
      this.editorApp = new EditorApp(editorAppConfig);
      await this.editorApp.start(this.editorElement.nativeElement);
    };
    from(startDiffEditor()).pipe(untilDestroyed(this)).subscribe();
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
    this.insertTypeAnnotations(this.editorApp!.getEditor()!, this.currentRange, this.currentSuggestion);
    // Only for "Add All Type Annotation"
    if (this.isMultipleVariables && this.userResponseSubject) {
      this.userResponseSubject.next();
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

  private insertTypeAnnotations(editor: MonacoEditor, range: monaco.Range, annotations: string) {
    const offset = editor.getModel()?.getOffsetAt(new monaco.Position(range.endLineNumber, range.endColumn)) ?? 0;
    this.code?.insert(offset, annotations);
  }

  @HostListener("window:resize")
  onWindowResize() {
    this.adjustEditorSize();
  }

  private adjustEditorSize(): void {
    const container = this.containerElement.nativeElement;
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const rect = container.getBoundingClientRect();
    if (rect.right > viewportWidth) {
      container.style.width = `${viewportWidth - rect.left}px`;
    }
    if (rect.bottom > viewportHeight) {
      container.style.height = `${viewportHeight - rect.top}px`;
    }
    this.editorApp?.getEditor()?.layout();
  }
  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.currentOperatorId);
  }
}
