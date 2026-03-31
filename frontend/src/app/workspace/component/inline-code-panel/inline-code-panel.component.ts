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
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { YText } from "yjs/dist/src/types/YText";
import { YType } from "../../types/shared-editing.interface";
import { OperatorPredicate } from "../../types/workflow-common.interface";

/**
 * InlineCodePanelComponent displays a small read-only code preview for Python UDF operators.
 * It shows the code content from the operator's properties and updates in real-time.
 * The panel header shows the operator's custom display name and can be closed.
 *
 * NOTE: This component intentionally uses a simple <pre><code> element instead of Monaco editor
 * to avoid conflicts with the MonacoEditorLanguageClientWrapper used by code-editor-dialog.
 * The wrapper's initServices() can only be called once globally, and using multiple Monaco
 * instances with different initialization methods causes blank editors.
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

  @ViewChild("codeContainer", { static: true }) codeContainer!: ElementRef;

  public codeContent: string = "";
  public language: string = "python";

  private codeYText?: YText;
  private observer?: () => void;

  constructor(private workflowActionService: WorkflowActionService) {}

  ngAfterViewInit(): void {
    this.initializeCodeDisplay();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["operatorId"] || changes["isDiffMode"] || changes["originalCode"]) {
      this.disposeObserver();
      this.initializeCodeDisplay();
    }
  }

  ngOnDestroy(): void {
    this.disposeObserver();
  }

  onClose(): void {
    this.closePanel.emit(this.operatorId);
  }

  private disposeObserver(): void {
    if (this.observer && this.codeYText) {
      this.codeYText.unobserve(this.observer);
      this.observer = undefined;
    }
  }

  private initializeCodeDisplay(): void {
    if (!this.operatorId) {
      return;
    }

    const operator = this.workflowActionService.getTexeraGraph().getOperator(this.operatorId);
    if (!operator) {
      return;
    }

    this.language = this.getLanguageFromOperator(operator);
    this.codeContent = this.getCodeFromOperator(operator);
    this.setupYTextObserver();
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
          const newCode = this.codeYText?.toString() || "";
          if (newCode !== this.codeContent) {
            this.codeContent = newCode;
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
      operatorType === "DualInputPortsPythonUDFV2" ||
      operatorType === "PythonTableUDF" ||
      operatorType === "DataProcessing" ||
      operatorType === "DataLoading"
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
