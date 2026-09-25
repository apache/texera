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

import { Component, Input } from "@angular/core";
import { AsyncPipe, NgFor, NgIf } from "@angular/common";
import { Observable } from "rxjs";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { AiWorkflowFixerService, FixConfidence, FixState, UNSUPPORTED_MESSAGE } from "./ai-workflow-fixer.service";

/**
 * Result-panel frame that asks the LLM to fix the failed operator.
 *
 * The analysis is started by an explicit button: ResultPanelComponent rebuilds
 * every frame on each re-render (clearResultPanel), so analysing on init would
 * fire a model request per re-render. For the same reason all state lives in
 * AiWorkflowFixerService and this component keeps none.
 */
@Component({
  selector: "texera-ai-fix-frame",
  templateUrl: "./ai-fix-frame.component.html",
  styleUrls: ["./ai-fix-frame.component.scss"],
  imports: [
    NgIf,
    NgFor,
    AsyncPipe,
    NzAlertComponent,
    NzSpinComponent,
    NzTagComponent,
    NzButtonComponent,
    NzWaveDirective,
    NzIconDirective,
  ],
})
export class AiFixFrameComponent {
  @Input() operatorId?: string;

  public readonly state$: Observable<FixState>;
  public readonly unsupportedMessage = UNSUPPORTED_MESSAGE;

  constructor(
    private aiWorkflowFixerService: AiWorkflowFixerService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowConsoleService: WorkflowConsoleService
  ) {
    this.state$ = aiWorkflowFixerService.getState$();
  }

  /** Collects the operator's error, code, schema and config, then asks for a fix. */
  public onAnalyze(): void {
    const operatorId = this.operatorId;
    if (!operatorId) {
      return;
    }
    const errorMessage = this.rawErrorMessage(operatorId);
    if (!errorMessage) {
      return;
    }
    const properties = this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorProperties ?? {};
    void this.aiWorkflowFixerService.analyzeError(
      operatorId,
      errorMessage,
      this.workflowCompilingService.getPortInputSchema(operatorId, 0),
      properties.code as string | undefined,
      properties
    );
  }

  public onApply(): void {
    this.aiWorkflowFixerService.applyFix();
  }

  public onDiscard(): void {
    this.aiWorkflowFixerService.discardFix();
  }

  public confidenceColor(confidence: FixConfidence): string {
    if (confidence === "high") {
      return "green";
    }
    return confidence === "medium" ? "gold" : "red";
  }

  /** Splits a snippet so the template can prefix each line with - or +. */
  public splitLines(snippet: string): string[] {
    return snippet.split("\n");
  }

  /**
   * The untrimmed error text for this operator.
   *
   * Engine failures arrive as fatal errors, but a Python UDF exception never
   * becomes one: it is delivered as an ERROR console message whose `message`
   * holds the whole traceback, which is what the classifier needs.
   */
  private rawErrorMessage(operatorId: string): string | undefined {
    const fatal = this.executeWorkflowService.getErrorMessages().find(err => err.operatorId === operatorId);
    if (fatal) {
      return [fatal.message, fatal.details].filter(part => part).join("\n");
    }
    const consoleError = (this.workflowConsoleService.getConsoleMessages(operatorId) ?? [])
      .filter(msg => msg.msgType.name === "ERROR")
      .pop();
    return consoleError ? consoleError.message || consoleError.title : undefined;
  }
}
