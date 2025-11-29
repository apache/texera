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
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  OnDestroy,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import * as monaco from "monaco-editor";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { YText } from "yjs/dist/src/types/YText";
import { YType } from "../../types/shared-editing.interface";
import { OperatorPredicate } from "../../types/workflow-common.interface";

/**
 * InlineCodePanelComponent displays a small read-only code preview for Python UDF operators.
 * It shows the code content from the operator's properties and updates in real-time.
 * The panel header shows the operator's custom display name and can be closed.
 */
@UntilDestroy()
@Component({
  selector: "texera-inline-code-panel",
  templateUrl: "./inline-code-panel.component.html",
  styleUrls: ["./inline-code-panel.component.scss"],
})
export class InlineCodePanelComponent implements AfterViewInit, OnDestroy, OnChanges {
  @Input() operatorId!: string;
  @Input() displayName: string = "Code";
  @Input() isDiffMode: boolean = false;
  @Input() originalCode?: string;

  @Output() closePanel = new EventEmitter<string>();

  @ViewChild("editorContainer", { static: true }) editorContainer!: ElementRef;

  private editor?: monaco.editor.IStandaloneCodeEditor;
  private diffEditor?: monaco.editor.IStandaloneDiffEditor;
  private codeYText?: YText;
  private observer?: () => void;

  constructor(private workflowActionService: WorkflowActionService) {}

  ngAfterViewInit(): void {
    this.initializeEditor();
  }

  ngOnChanges(changes: SimpleChanges): void {
    // Re-initialize editor if operatorId, diffMode or originalCode changes
    if (changes["operatorId"] || changes["isDiffMode"] || changes["originalCode"]) {
      this.disposeEditor();
      this.initializeEditor();
    }
  }

  ngOnDestroy(): void {
    this.disposeEditor();
  }

  onClose(): void {
    this.closePanel.emit(this.operatorId);
  }

  private disposeEditor(): void {
    if (this.observer && this.codeYText) {
      this.codeYText.unobserve(this.observer);
    }
    if (this.editor) {
      this.editor.dispose();
      this.editor = undefined;
    }
    if (this.diffEditor) {
      this.diffEditor.dispose();
      this.diffEditor = undefined;
    }
  }

  private initializeEditor(): void {
    if (!this.operatorId) {
      return;
    }

    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
    if (!operator) {
      return;
    }

    const language = this.getLanguageFromOperator(operator);
    const code = this.getCodeFromOperator(operator);

    if (this.isDiffMode && this.originalCode !== undefined) {
      this.initializeDiffEditor(code, language);
    } else {
      this.initializeNormalEditor(code, language);
      this.setupYTextObserver();
    }
  }

  private initializeNormalEditor(code: string, language: string): void {
    this.editor = monaco.editor.create(this.editorContainer.nativeElement, {
      value: code,
      language: language,
      readOnly: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      lineNumbers: "on",
      lineNumbersMinChars: 3,
      folding: false,
      renderLineHighlight: "none",
      overviewRulerBorder: false,
      hideCursorInOverviewRuler: true,
      scrollbar: {
        vertical: "auto",
        horizontal: "auto",
        verticalScrollbarSize: 8,
        horizontalScrollbarSize: 8,
      },
      fontSize: 11,
      lineHeight: 16,
      padding: { top: 4, bottom: 4 },
      automaticLayout: true,
      wordWrap: "off",
      theme: "vs",
    });
  }

  private initializeDiffEditor(modifiedCode: string, language: string): void {
    this.diffEditor = monaco.editor.createDiffEditor(this.editorContainer.nativeElement, {
      readOnly: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      lineNumbers: "on",
      lineNumbersMinChars: 3,
      folding: false,
      renderLineHighlight: "none",
      overviewRulerBorder: false,
      scrollbar: {
        vertical: "auto",
        horizontal: "auto",
        verticalScrollbarSize: 8,
        horizontalScrollbarSize: 8,
      },
      fontSize: 11,
      lineHeight: 16,
      automaticLayout: true,
      renderSideBySide: false,
      renderIndicators: true,
    });

    const originalModel = monaco.editor.createModel(this.originalCode || "", language);
    const modifiedModel = monaco.editor.createModel(modifiedCode, language);

    this.diffEditor.setModel({
      original: originalModel,
      modified: modifiedModel,
    });
  }

  private setupYTextObserver(): void {
    try {
      const operatorProperties = this.workflowActionService
        .getTexeraGraph()
        .getSharedOperatorType(this.operatorId)
        .get("operatorProperties") as YType<Readonly<{ [key: string]: any }>>;

      this.codeYText = operatorProperties.get("code") as YText;

      if (this.codeYText) {
        this.observer = () => {
          if (this.editor) {
            const newCode = this.codeYText?.toString() || "";
            const currentCode = this.editor.getValue();
            if (newCode !== currentCode) {
              this.editor.setValue(newCode);
            }
          }
        };
        this.codeYText.observe(this.observer);
      }
    } catch {
      // Operator may not have code property
    }
  }

  private getLanguageFromOperator(operator: OperatorPredicate): string {
    const operatorType = operator.operatorType;
    if (operatorType === "RUDFSource" || operatorType === "RUDF") {
      return "r";
    } else if (
      operatorType === "PythonUDFV2" ||
      operatorType === "PythonUDFSourceV2" ||
      operatorType === "DualInputPortsPythonUDFV2"
    ) {
      return "python";
    } else {
      return "java";
    }
  }

  private getCodeFromOperator(operator: OperatorPredicate): string {
    const properties = operator.operatorProperties as { code?: string };
    return properties.code || "";
  }
}
