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
import { catchError, from, of, Subject, take, timeout } from "rxjs";
import { CoeditorPresenceService } from "../../service/workflow-graph/model/coeditor-presence.service";
import { DomSanitizer, SafeStyle } from "@angular/platform-browser";
import { Coeditor } from "../../../common/type/user";
import { YType } from "../../types/shared-editing.interface";
import { FormControl, FormsModule } from "@angular/forms";
import { AIAssistantService, TypeAnnotationResponse } from "../../service/ai-assistant/ai-assistant.service";
import { UdfContext, UdfCopilotService } from "../../service/udf-copilot/udf-copilot.service";
import { UdfCopilotPanelComponent } from "./udf-copilot-panel.component";
import { UdfContextPanelComponent } from "./udf-context-panel.component";
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
import { GuiConfigService } from "src/app/common/service/gui-config.service";
import { CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NgFor, NgComponentOutlet, NgIf } from "@angular/common";
import { FormlyRepeatDndComponent } from "../../../common/formly/repeat-dnd/repeat-dnd.component";

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
    FormlyRepeatDndComponent,
    UdfCopilotPanelComponent,
    UdfContextPanelComponent,
    FormsModule,
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

  private editorWrapper: MonacoEditorLanguageClientWrapper = new MonacoEditorLanguageClientWrapper();
  private monacoBinding?: MonacoBinding;
  private udfCopilotDisposables: monaco.IDisposable[] = [];

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

  public showCopilotPanel: boolean = false;
  public showContextPanel: boolean = false;

  // Auto-detected differences between what the UDF code writes
  // (`tuple_["x"] = ...`) and what's declared in Extra Output Columns:
  //   - add:    code writes a column not declared and not in upstream
  //   - remove: declared column not written in code anymore
  //   - update: declared with one type but code writes a different type
  public schemaActions: {
    kind: "add" | "remove" | "update";
    name: string;
    type?: string;
    oldType?: string;
  }[] = [];
  private schemaScanTimer?: number;

  // AI-driven schema recommendation (from /sync-schema endpoint). When set,
  // the banner shows the AI's full schema replacement instead of (or in
  // addition to) the regex-based actions.
  public aiSchemaSuggestion?: {
    retainInputColumns: boolean;
    outputColumns: { attributeName: string; attributeType: string }[];
    explanation: string;
  };
  public aiSchemaLoading = false;

  // Cmd+K rewrite overlay state
  public showRewriteOverlay: boolean = false;
  public rewriteMode: "rewrite" | "fix" = "rewrite";
  public rewriteState: "prompt" | "loading" | "preview" = "prompt";
  public rewriteInstruction: string = "";
  public rewriteNewCode: string = "";
  public rewriteOldCode: string = "";
  public rewriteOverlayTop: number = 0;
  public rewriteOverlayLeft: number = 0;
  private rewriteSelection: monaco.Selection | undefined;
  private pendingFix?: { errorMessage: string; range: monaco.Range };

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
    private config: GuiConfigService,
    private udfCopilotService: UdfCopilotService
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

    for (const d of this.udfCopilotDisposables) {
      try {
        d.dispose();
      } catch {}
    }
    this.udfCopilotDisposables = [];

    if (this.schemaScanTimer !== undefined) {
      window.clearTimeout(this.schemaScanTimer);
      this.schemaScanTimer = undefined;
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
    const languageServerWebsocketUrl = getWebsocketUrl(
      "/python-language-server",
      this.config.env.pythonLanguageServerPort
    );
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
        this.setupUdfCopilot(editor);
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

  /**
   * Register the UDF Copilot integrations on this editor instance.
   *
   * Currently registers schema-aware inline completions (ghost text). Must be
   * called from inside the editorWrapper.getEditor() subscribe callback —
   * monaco-editor-wrapper can recreate the editor on language-server reconnect,
   * and any disposables we hold from a previous instance would be stale.
   */
  private setupUdfCopilot(editor: MonacoEditor) {
    if (this.language !== "python") return;

    // Warm the upstream sample-row cache so chat / Cmd+K / fix all see a real
    // data row in context by the time the user actually invokes them.
    this.udfCopilotService.prefetchUpstreamSample(this.currentOperatorId);

    // Watch the Y-text for schema mismatches between code and the property
    // panel. Debounce 500ms so we don't scan on every keystroke.
    if (this.code) {
      const scan = () => {
        if (this.schemaScanTimer !== undefined) window.clearTimeout(this.schemaScanTimer);
        this.schemaScanTimer = window.setTimeout(() => this.scanSchemaMismatches(), 500);
      };
      this.code.observe(scan);
      this.scanSchemaMismatches(); // initial scan
    }

    // If "Fix with AI" was clicked before the editor opened, consume the
    // pending fix now and auto-open the overlay. consumePendingFix clears the
    // stored message so re-opening the editor later does NOT re-trigger this.
    const pendingError = this.udfCopilotService.consumePendingFix(this.currentOperatorId);
    if (pendingError) {
      setTimeout(() => {
        this.openRewriteOverlay(editor, "fix");
        this.rewriteInstruction = pendingError;
      }, 150);
    }

    // If the editor is already open when "Fix with AI" is clicked, handle it live.
    this.udfCopilotService.fixTrigger$
      .pipe(
        filter(ev => ev.operatorId === this.currentOperatorId),
        untilDestroyed(this)
      )
      .subscribe(ev => {
        // Drop the one-shot — we're handling this live, no need to replay later.
        this.udfCopilotService.consumePendingFix(this.currentOperatorId);
        this.openRewriteOverlay(editor, "fix");
        this.rewriteInstruction = ev.errorMessage;
      });

    const PREFIX_MAX = 4000;
    const SUFFIX_MAX = 1000;
    const COMPLETION_DEBOUNCE_MS = 250;

    const disposable = monaco.languages.registerInlineCompletionsProvider("python", {
      provideInlineCompletions: async (model, position, _ctx, token) => {
        if (token.isCancellationRequested) return { items: [] };

        // Defer to the column-name dropdown when the cursor is inside a
        // bracket-string accessor — that has its own provider with all columns.
        const lineUpToCursor = model
          .getLineContent(position.lineNumber)
          .slice(0, position.column - 1);
        if (/\[\s*['"][^'"]*$/.test(lineUpToCursor)) return { items: [] };

        const offset = model.getOffsetAt(position);
        const fullText = model.getValue();
        const prefix = fullText.slice(Math.max(0, offset - PREFIX_MAX), offset);
        const suffix = fullText.slice(offset, Math.min(fullText.length, offset + SUFFIX_MAX));

        // Debounce per-invocation: skip the backend call if Monaco cancels us
        // before the delay elapses. Each invocation owns its own timer, so
        // earlier in-flight invocations don't share state with later ones.
        const debounced = await new Promise<boolean>(resolve => {
          const timer = setTimeout(() => resolve(true), COMPLETION_DEBOUNCE_MS);
          token.onCancellationRequested(() => {
            clearTimeout(timer);
            resolve(false);
          });
        });
        if (!debounced || token.isCancellationRequested) return { items: [] };

        try {
          const context = this.udfCopilotService.buildContext(this.currentOperatorId);
          const res = await this.udfCopilotService.completeAsync({ prefix, suffix, context });
          if (token.isCancellationRequested) return { items: [] };
          const text = (res.text ?? "").replace(/\r/g, "");
          if (!text) return { items: [] };

          return {
            items: [
              {
                insertText: text,
                range: new monaco.Range(
                  position.lineNumber,
                  position.column,
                  position.lineNumber,
                  position.column
                ),
              },
            ],
          };
        } catch {
          return { items: [] };
        }
      },
      freeInlineCompletions: () => {
        // Nothing to free — items are plain objects.
      },
    });

    this.udfCopilotDisposables.push(disposable);

    // Schema-aware column-name completion: when the cursor sits inside a
    // bracket-string accessor (tuple_["..."] / df["..."] / etc.), drop down
    // every upstream column as a completion item. Local & instant — no LLM.
    const columnCompletion = monaco.languages.registerCompletionItemProvider("python", {
      triggerCharacters: ['"', "'", "["],
      provideCompletionItems: (model, position) => {
        const lineUpToCursor = model
          .getLineContent(position.lineNumber)
          .slice(0, position.column - 1);
        const m = lineUpToCursor.match(/\[\s*['"]([^'"]*)$/);
        if (!m) return { suggestions: [] };

        const ctx = this.udfCopilotService.buildContext(this.currentOperatorId);
        const cols = ctx.upstreamSchema ?? [];
        if (cols.length === 0) return { suggestions: [] };

        const partial = m[1];
        const replaceRange = new monaco.Range(
          position.lineNumber,
          position.column - partial.length,
          position.lineNumber,
          position.column
        );

        return {
          suggestions: cols.map(col => ({
            label: col.name,
            kind: monaco.languages.CompletionItemKind.Field,
            insertText: col.name,
            detail: col.type,
            documentation: `Upstream column (${col.type})`,
            range: replaceRange,
            sortText: `0_${col.name}`,
          })),
        };
      },
    });
    this.udfCopilotDisposables.push(columnCompletion);

    // Cmd+K (Ctrl+K on win/linux) inline rewrite.
    const cmdK = editor.addAction({
      id: "udf-copilot-rewrite",
      label: "UDF Copilot: Rewrite Selection (Cmd+K)",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyK],
      contextMenuGroupId: "1_modification",
      contextMenuOrder: 1.05,
      run: () => this.openRewriteOverlay(editor, "rewrite"),
    });
    this.udfCopilotDisposables.push(cmdK);

    // Always-visible "Fix Error with AI" — user pastes the error message;
    // does not depend on Pyright markers being present.
    const fixFromError = editor.addAction({
      id: "udf-copilot-fix-from-error",
      label: "UDF Copilot: Fix Error with AI",
      contextMenuGroupId: "1_modification",
      contextMenuOrder: 1.06,
      run: () => this.openRewriteOverlay(editor, "fix"),
    });
    this.udfCopilotDisposables.push(fixFromError);

    // Quick Fix on pyright markers — registered as a hidden action (no
    // contextMenuGroupId) invoked by the code-action provider's command.
    const fixAction = editor.addAction({
      id: "udf-copilot-fix-action",
      label: "UDF Copilot: Quick Fix (from marker)",
      run: () => this.runPendingFix(),
    });
    this.udfCopilotDisposables.push(fixAction);

    const codeActionProvider = monaco.languages.registerCodeActionProvider("python", {
      provideCodeActions: (_model, _range, context) => {
        const errorMarkers = context.markers.filter(
          m =>
            m.severity === monaco.MarkerSeverity.Error ||
            m.severity === monaco.MarkerSeverity.Warning
        );
        if (errorMarkers.length === 0) {
          return { actions: [], dispose: () => {} };
        }
        const marker = errorMarkers[0];
        // Stash so the editor action can read it; provideCodeActions is sync,
        // but the action runs async later.
        this.pendingFix = {
          errorMessage: marker.message,
          range: new monaco.Range(
            marker.startLineNumber,
            marker.startColumn,
            marker.endLineNumber,
            marker.endColumn
          ),
        };
        const previewMsg = marker.message.length > 50 ? marker.message.slice(0, 50) + "…" : marker.message;
        return {
          actions: [
            {
              title: `UDF Copilot: Fix "${previewMsg}"`,
              kind: "quickfix",
              diagnostics: errorMarkers,
              isPreferred: true,
              command: {
                id: "udf-copilot-fix-action",
                title: "Fix with UDF Copilot",
              },
            },
          ],
          dispose: () => {},
        };
      },
    });
    this.udfCopilotDisposables.push(codeActionProvider);
  }

  private runPendingFix(): void {
    if (!this.pendingFix) return;
    const { errorMessage, range } = this.pendingFix;
    this.pendingFix = undefined;

    const editor = this.editorWrapper.getEditor();
    const model = editor?.getModel();
    if (!model || !this.code) return;

    const code = model.getValue();
    const context = this.udfCopilotService.buildContext(this.currentOperatorId);

    this.udfCopilotService
      .fix({
        errorMessage,
        code,
        range: {
          startLine: range.startLineNumber,
          startColumn: range.startColumn,
          endLine: range.endLineNumber,
          endColumn: range.endColumn,
        },
        context,
      })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: res => {
          if (res.newCode && this.code) {
            const oldLen = this.code.length;
            this.code.delete(0, oldLen);
            this.code.insert(0, res.newCode);
            this.formatAfterAccept();
          }
        },
        error: () => {
          // silently swallow — Quick Fix is best-effort
        },
      });
  }

  /**
   * Open the rewrite/fix overlay near the current selection.
   * mode='rewrite' (Cmd+K): transforms selected code per instruction.
   * mode='fix': user describes the error; AI replaces the full file.
   */
  private openRewriteOverlay(editor: MonacoEditor, mode: "rewrite" | "fix" = "rewrite"): void {
    const model = editor.getModel();
    if (!model) return;

    let selection = editor.getSelection();
    if (!selection || selection.isEmpty()) {
      const pos = editor.getPosition();
      if (pos) {
        const lineLen = model.getLineMaxColumn(pos.lineNumber);
        selection = new monaco.Selection(pos.lineNumber, 1, pos.lineNumber, lineLen);
      } else if (mode === "fix") {
        // Editor lost focus (e.g. user clicked result panel). Fix mode doesn't
        // use the selection, so fall back to line 1.
        selection = new monaco.Selection(1, 1, 1, 1);
      } else {
        return;
      }
    }

    this.rewriteMode = mode;
    this.rewriteSelection = selection;
    this.rewriteOldCode = model.getValueInRange(selection);
    this.rewriteInstruction = "";
    this.rewriteNewCode = "";
    this.rewriteState = "prompt";

    const editorRect = this.editorElement?.nativeElement?.getBoundingClientRect();
    const visiblePos = editor.getScrolledVisiblePosition(selection.getStartPosition());
    if (visiblePos && editorRect) {
      this.rewriteOverlayTop = editorRect.top + visiblePos.top + 20;
      this.rewriteOverlayLeft = editorRect.left + 12;
    } else if (editorRect) {
      // Editor lost focus (e.g. triggered from result panel) — anchor to top of editor.
      this.rewriteOverlayTop = editorRect.top + 40;
      this.rewriteOverlayLeft = editorRect.left + 12;
    }

    this.showRewriteOverlay = true;
  }

  public submitRewrite(): void {
    const instruction = this.rewriteInstruction.trim();
    if (!instruction || this.rewriteState === "loading") return;

    const editor = this.editorWrapper.getEditor();
    const allCode = editor?.getModel()?.getValue() ?? "";
    const context = this.udfCopilotService.buildContext(this.currentOperatorId);

    this.rewriteState = "loading";

    const handleResult = (newCode: string) => {
      const trimmed = (newCode ?? "").trim();
      // Fix mode replaces the whole file — empty is almost certainly the AI
      // giving up, not a request to clear everything.
      if (!trimmed && this.rewriteMode === "fix") {
        this.rewriteInstruction =
          (this.rewriteInstruction ? this.rewriteInstruction + "\n\n" : "") +
          "(AI returned no code. Try rephrasing.)";
        this.rewriteState = "prompt";
        return;
      }
      // Rewrite mode: empty is a legitimate "delete the selection" outcome.
      this.rewriteNewCode = trimmed;
      this.rewriteState = "preview";
    };

    if (this.rewriteMode === "fix") {
      this.udfCopilotService
        .fix({ errorMessage: instruction, code: allCode, context })
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => handleResult(res.newCode),
          error: () => {
            this.rewriteState = "prompt";
          },
        });
    } else {
      this.udfCopilotService
        .rewrite({
          selectedCode: this.rewriteOldCode,
          allCode,
          instruction,
          context,
        })
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => handleResult(res.newCode),
          error: () => {
            this.rewriteState = "prompt";
          },
        });
    }
  }

  public acceptRewrite(): void {
    if (!this.code) {
      this.cancelRewrite();
      return;
    }

    if (this.rewriteMode === "fix") {
      // Fix mode replaces the entire file. Empty here is treated as an error
      // upstream (we never enter preview), so this is always non-empty.
      if (!this.rewriteNewCode) {
        this.cancelRewrite();
        return;
      }
      const oldLen = this.code.length;
      this.code.delete(0, oldLen);
      this.code.insert(0, this.rewriteNewCode);
      this.formatAfterAccept();
      this.cancelRewrite();
      return;
    }

    // Rewrite mode — empty newCode is a legitimate "delete the selection".
    if (!this.rewriteSelection) {
      this.cancelRewrite();
      return;
    }
    const editor = this.editorWrapper.getEditor();
    const model = editor?.getModel();
    if (!model) {
      this.cancelRewrite();
      return;
    }

    const startOffset = model.getOffsetAt(this.rewriteSelection.getStartPosition());
    const endOffset = model.getOffsetAt(this.rewriteSelection.getEndPosition());

    // Re-indent the AI's output to match the indent of the original selection.
    // Only applied when the selection starts at column 1 (i.e. covers full
    // lines) — for mid-line selections the AI's output is inserted verbatim.
    let toInsert = this.rewriteNewCode;
    if (toInsert && this.rewriteSelection.startColumn === 1) {
      toInsert = this.reindentToMatch(this.rewriteOldCode, toInsert);
    }

    this.code.delete(startOffset, endOffset - startOffset);
    if (toInsert) {
      this.code.insert(startOffset, toInsert);
    }
    this.formatAfterAccept();
    this.cancelRewrite();
  }

  public cancelRewrite(): void {
    this.showRewriteOverlay = false;
    this.rewriteMode = "rewrite";
    this.rewriteState = "prompt";
    this.rewriteInstruction = "";
    this.rewriteNewCode = "";
    this.rewriteOldCode = "";
    this.rewriteSelection = undefined;
  }

  public onRewriteKeyDown(ev: KeyboardEvent): void {
    if (ev.key === "Enter" && !ev.shiftKey) {
      ev.preventDefault();
      this.submitRewrite();
    } else if (ev.key === "Escape") {
      ev.preventDefault();
      this.cancelRewrite();
    }
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
    this.editorWrapper.getEditor()?.layout();
  }
  onFocus() {
    this.workflowActionService.getJointGraphWrapper().highlightOperators(this.currentOperatorId);
  }

  public toggleCopilotPanel(): void {
    this.showCopilotPanel = !this.showCopilotPanel;
    // Monaco needs an explicit layout() after its container width changes.
    setTimeout(() => this.editorWrapper.getEditor()?.layout(), 0);
  }

  public toggleContextPanel(): void {
    this.showContextPanel = !this.showContextPanel;
    setTimeout(() => this.editorWrapper.getEditor()?.layout(), 0);
  }

  /**
   * Scan the UDF code for `tuple_["col"] = value` writes and compare against
   * what's declared in Extra Output Columns (plus what flows in from upstream).
   * Computes a list of add/remove/update actions to bring them in sync.
   */
  private scanSchemaMismatches(): void {
    if (!this.code || this.language !== "python") {
      this.schemaActions = [];
      return;
    }
    const text = this.code.toString();
    const writes = new Map<string, string>();
    // Scan line by line so we can skip commented-out lines (a real "delete"
    // of a column write often takes the form of commenting it out, not
    // removing the text entirely).
    for (const rawLine of text.split("\n")) {
      // Strip inline trailing comments (`# ...`) but keep the code part.
      // Conservative: only strip when # is preceded by whitespace, to avoid
      // breaking things like `tuple_["#col"] = 1`.
      const codeOnly = rawLine.replace(/\s+#.*$/, "");
      // Skip pure-comment lines.
      if (/^\s*#/.test(codeOnly)) continue;
      // Require `=` NOT followed by another `=` so `tuple_["x"] == 1`
      // (comparison) doesn't get flagged as a write.
      const m = codeOnly.match(/tuple_\[["']([^"']+)["']\]\s*=(?!=)\s*(.+)/);
      if (m) writes.set(m[1], this.inferAttributeType(m[2].trim()));
    }

    // Currently declared extra-output columns from the property panel.
    let declared = new Map<string, string>();
    try {
      const op = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      const out = (op?.operatorProperties as any)?.outputColumns ?? [];
      declared = new Map(
        out
          .filter((c: any) => c?.attributeName)
          .map((c: any) => [c.attributeName, c.attributeType ?? "string"])
      );
    } catch {}

    // Upstream-inherited columns flow through automatically — writing one of
    // these doesn't require declaring it.
    let upstream = new Set<string>();
    try {
      const ctx = this.udfCopilotService.buildContext(this.currentOperatorId);
      upstream = new Set((ctx.upstreamSchema ?? []).map(c => c.name));
    } catch {}

    const actions: typeof this.schemaActions = [];

    // ADD: written in code, neither declared nor upstream.
    for (const [name, type] of writes) {
      if (!declared.has(name) && !upstream.has(name)) {
        actions.push({ kind: "add", name, type });
      }
    }

    // REMOVE: declared but no longer written in code (and not flowing from
    // upstream — those are inherited anyway).
    for (const [name] of declared) {
      if (!writes.has(name) && !upstream.has(name)) {
        actions.push({ kind: "remove", name });
      }
    }

    // UPDATE: declared AND written, but types disagree. Type inference from
    // a literal RHS is best-effort, so this is a suggestion not a guarantee.
    for (const [name, codeType] of writes) {
      if (declared.has(name) && declared.get(name) !== codeType) {
        actions.push({ kind: "update", name, type: codeType, oldType: declared.get(name) });
      }
    }

    this.schemaActions = actions;
  }

  private inferAttributeType(rhs: string): string {
    const t = rhs.trim();
    if (/^['"]/.test(t)) return "string";
    if (/^(True|False)\b/.test(t)) return "boolean";
    if (/^-?\d+\.\d/.test(t)) return "double";
    if (/^-?\d+(?:[^.\d]|$)/.test(t)) return "integer";
    if (/^(str|float|int|bool)\s*\(/.test(t)) {
      if (t.startsWith("str")) return "string";
      if (t.startsWith("float")) return "double";
      if (t.startsWith("int")) return "integer";
      if (t.startsWith("bool")) return "boolean";
    }
    return "string";
  }

  /** Apply every pending add/remove/update to the operator's outputColumns. */
  public applySchemaActions(): void {
    if (this.schemaActions.length === 0) return;
    try {
      const op = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      const existing = ((op?.operatorProperties as any)?.outputColumns ?? []) as {
        attributeName: string;
        attributeType: string;
      }[];

      const removeNames = new Set(
        this.schemaActions.filter(a => a.kind === "remove").map(a => a.name)
      );
      const typeUpdates = new Map(
        this.schemaActions.filter(a => a.kind === "update").map(a => [a.name, a.type!])
      );
      const adds = this.schemaActions
        .filter(a => a.kind === "add")
        .map(a => ({ attributeName: a.name, attributeType: a.type! }));

      const kept = existing
        .filter(c => !removeNames.has(c.attributeName))
        .map(c =>
          typeUpdates.has(c.attributeName)
            ? { attributeName: c.attributeName, attributeType: typeUpdates.get(c.attributeName)! }
            : c
        );

      const updated = {
        ...(op?.operatorProperties ?? {}),
        outputColumns: [...kept, ...adds],
      };
      this.workflowActionService.setOperatorProperty(this.currentOperatorId, updated);
      this.schemaActions = [];
    } catch {
      // best-effort: leave the banner up so the user can retry
    }
  }

  public dismissSchemaBanner(): void {
    this.schemaActions = [];
    this.aiSchemaSuggestion = undefined;
  }

  /**
   * Ask the AI to analyze the UDF code + current property values and propose
   * the correct outputColumns + retainInputColumns. Surfaces nuances that
   * the regex scan can't (dropping upstream columns, restructured yields,
   * Table API DataFrames built from scratch, etc.).
   */
  public analyzeSchemaWithAI(): void {
    if (this.aiSchemaLoading) return;
    const editor = this.editorWrapper.getEditor();
    const code = editor?.getModel()?.getValue() ?? "";
    if (!code) return;

    let currentOutputColumns: { attributeName: string; attributeType: string }[] = [];
    let currentRetainInputColumns = true;
    try {
      const op = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      const props = (op?.operatorProperties as any) ?? {};
      currentOutputColumns = props.outputColumns ?? [];
      if (typeof props.retainInputColumns === "boolean") {
        currentRetainInputColumns = props.retainInputColumns;
      }
    } catch {}

    const context = this.udfCopilotService.buildContext(this.currentOperatorId);
    this.aiSchemaLoading = true;

    this.udfCopilotService
      .syncSchema({ code, currentOutputColumns, currentRetainInputColumns, context })
      .pipe(untilDestroyed(this))
      .subscribe({
        next: res => {
          this.aiSchemaSuggestion = res;
          this.aiSchemaLoading = false;
        },
        error: () => {
          this.aiSchemaLoading = false;
        },
      });
  }

  /** Apply the AI's suggested outputColumns + retainInputColumns. */
  public applyAiSchemaSuggestion(): void {
    if (!this.aiSchemaSuggestion) return;
    try {
      const op = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorId);
      const updated = {
        ...(op?.operatorProperties ?? {}),
        outputColumns: this.aiSchemaSuggestion.outputColumns,
        retainInputColumns: this.aiSchemaSuggestion.retainInputColumns,
      };
      this.workflowActionService.setOperatorProperty(this.currentOperatorId, updated);
      this.aiSchemaSuggestion = undefined;
      this.schemaActions = [];
    } catch {
      // leave the suggestion visible so the user can retry
    }
  }

  /** UI helper — short prefix glyph per action kind. */
  public actionGlyph(kind: "add" | "remove" | "update"): string {
    if (kind === "add") return "+";
    if (kind === "remove") return "−";
    return "~";
  }

  // Stable function references so Angular doesn't see the panel's inputs
  // changing on every change-detection tick. Bound as arrow-function fields.
  public readonly copilotCodeProvider = (): string => {
    try {
      return this.editorWrapper.getEditor()?.getModel()?.getValue() ?? "";
    } catch {
      return "";
    }
  };

  public readonly copilotContextProvider = (): UdfContext => {
    try {
      return this.udfCopilotService.buildContext(this.currentOperatorId);
    } catch {
      return {};
    }
  };

  /**
   * Apply assistant-suggested code by atomically replacing the full Y-text
   * buffer. Going through Y-text (not editor.executeEdits) is required so
   * co-editors and the Monaco model stay in sync.
   */
  public applyCopilotCode(newCode: string): void {
    if (!this.code) return;
    const oldLen = this.code.length;
    this.code.delete(0, oldLen);
    this.code.insert(0, newCode);
    this.formatAfterAccept();
  }

  /**
   * Best-effort formatting after AI-generated code lands in the editor.
   * Step 1: normalize leading tabs to 4 spaces (Python rejects mixed indent
   *         at runtime; this is the cheap reliable fix).
   * Step 2: trigger Monaco's formatDocument — no-op if no formatter is
   *         registered for Python, otherwise applies the language server's
   *         own formatter.
   */
  private formatAfterAccept(): void {
    if (this.code) {
      const text = this.code.toString();
      const normalized = text
        .split("\n")
        .map(line => {
          const m = line.match(/^([\t ]*)(.*)$/);
          if (!m) return line;
          const leading = m[1].replace(/\t/g, "    ");
          return leading + m[2];
        })
        .join("\n");
      if (normalized !== text) {
        this.code.delete(0, this.code.length);
        this.code.insert(0, normalized);
      }
    }

    setTimeout(() => {
      try {
        this.editorWrapper.getEditor()?.getAction("editor.action.formatDocument")?.run();
      } catch {
        // Pyright may not register a formatter — that's fine, ignore.
      }
    }, 100);
  }

  /**
   * Re-indent `newCode` so its first non-empty line has the same leading
   * indent as `oldCode`'s first non-empty line. Preserves relative indent
   * within `newCode` by applying a uniform delta to every non-empty line.
   *
   * Solves the Cmd+K rewrite case where the AI returns code that's
   * "logically right" but unindented relative to where it gets pasted.
   */
  private reindentToMatch(oldCode: string, newCode: string): string {
    const indentOf = (s: string): number => {
      const first = s.split("\n").find(l => l.trim() !== "");
      if (!first) return 0;
      const m = first.match(/^( *)/);
      return m ? m[1].length : 0;
    };
    const minIndentOf = (s: string): number => {
      let min = Infinity;
      for (const line of s.split("\n")) {
        if (line.trim() === "") continue;
        const m = line.match(/^( *)/);
        const w = m ? m[1].length : 0;
        if (w < min) min = w;
      }
      return isFinite(min) ? min : 0;
    };

    const targetIndent = indentOf(oldCode);
    const sourceMin = minIndentOf(newCode);
    const delta = targetIndent - sourceMin;
    if (delta === 0) return newCode;

    return newCode
      .split("\n")
      .map(line => {
        if (line.trim() === "") return line;
        if (delta > 0) return " ".repeat(delta) + line;
        // delta < 0 — remove up to |delta| leading spaces.
        const m = line.match(/^( *)/);
        const take = Math.min(-delta, m ? m[1].length : 0);
        return line.slice(take);
      })
      .join("\n");
  }
}
