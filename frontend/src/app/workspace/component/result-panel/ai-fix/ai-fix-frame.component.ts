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
import { BehaviorSubject, Observable, combineLatest } from "rxjs";
import { map } from "rxjs/operators";
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
import {
  AiWorkflowFixerService,
  FixConfidence,
  FixState,
  IDLE_STATE,
  OperatorError,
  UNSUPPORTED_MESSAGE,
} from "./ai-workflow-fixer.service";

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
  private readonly operatorIdSubject = new BehaviorSubject<string | undefined>(undefined);

  @Input()
  set operatorId(value: string | undefined) {
    this.operatorIdSubject.next(value);
  }

  get operatorId(): string | undefined {
    return this.operatorIdSubject.value;
  }

  public readonly state$: Observable<FixState>;
  public readonly unsupportedMessage = UNSUPPORTED_MESSAGE;

  constructor(
    private aiWorkflowFixerService: AiWorkflowFixerService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowConsoleService: WorkflowConsoleService
  ) {
    // The service is a root singleton holding one suggestion, but a failed pipeline can
    // put an AI Fix tab on several operators at once. Showing another operator's
    // suggestion here would let Apply patch an operator the user is not looking at, so a
    // frame only ever renders state that names its own operator.
    this.state$ = combineLatest([aiWorkflowFixerService.getState$(), this.operatorIdSubject]).pipe(
      map(([state, operatorId]) =>
        state.operatorId === operatorId && state.workflowId === this.workflowActionService.getOpenWorkflowId()
          ? state
          : IDLE_STATE
      )
    );
  }

  /** Collects the operator's error, code, schema and config, then asks for a fix. */
  public onAnalyze(): void {
    const operatorId = this.operatorId;
    if (!operatorId) {
      return;
    }
    const error = this.operatorError(operatorId);
    if (!error) {
      return;
    }
    const properties = this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorProperties ?? {};
    void this.aiWorkflowFixerService.analyzeError(
      operatorId,
      error,
      this.workflowCompilingService.getOperatorInputSchemaMap(operatorId),
      properties.code as string | undefined
    );
  }

  public onApply(): void {
    // Awaited by the service's own state stream, not here: the template renders `applying`
    // and then whatever the apply settles on. `void` says the promise is deliberately dropped.
    void this.aiWorkflowFixerService.applyFix();
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
   * This operator's error, split the way its source already splits it.
   *
   * Engine failures arrive as fatal errors, but a Python UDF exception never becomes one: it
   * is delivered as an ERROR console message while the execution stays `Running`. Either way
   * the exception is in a field of its own -- `message` on a fatal error, `title` on a console
   * message -- so the classifier is handed it directly instead of searching the joined text.
   */
  private operatorError(operatorId: string): OperatorError | undefined {
    const fatal = this.executeWorkflowService.getErrorMessages().find(err => err.operatorId === operatorId);
    if (fatal) {
      return { header: fatal.message, full: [fatal.message, fatal.details].filter(part => part).join("\n") };
    }
    const consoleError = (this.workflowConsoleService.getConsoleMessages(operatorId) ?? [])
      .filter(msg => msg.msgType.name === "ERROR")
      .pop();
    if (!consoleError) {
      return undefined;
    }
    // `message` holds only the frames for a JVM operator and the whole traceback for a Python
    // one, so the model is sent both parts with the exception first in either case.
    return {
      header: consoleError.title,
      full: [consoleError.title, consoleError.message].filter(part => part).join("\n"),
    };
  }
}
