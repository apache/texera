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

import { Component, ElementRef, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from "@angular/core";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { render } from "sass";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowSuggestionService } from "../../../service/workflow-suggestion/workflow-suggestion.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-error-frame",
  templateUrl: "./error-frame.component.html",
  styleUrls: ["./error-frame.component.scss"],
})
export class ErrorFrameComponent implements OnInit {
  @Input() operatorId?: string;
  // display error message:
  categoryToErrorMapping: ReadonlyMap<string, ReadonlyArray<WorkflowFatalError>> = new Map();

  // Whether suggestion service is currently loading
  isLoading = false;

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowSuggestionService: WorkflowSuggestionService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.renderError();

    // Listen to loading state to spin icon
    this.workflowSuggestionService
      .getLoadingStream()
      .pipe(untilDestroyed(this))
      .subscribe(v => (this.isLoading = v));
  }

  onClickGotoButton(target: string) {
    this.workflowActionService.highlightOperators(false, target);
  }

  renderError(): void {
    // first fetch the error messages from the execution state store
    let errorMessages = this.executeWorkflowService.getErrorMessages();
    const compilationErrorMap = this.workflowCompilingService.getWorkflowCompilationErrors();
    // then fetch error from the compilation state store
    errorMessages = errorMessages.concat(Object.values(compilationErrorMap));
    if (this.operatorId) {
      errorMessages = errorMessages.filter(err => err.operatorId === this.operatorId);
    }
    this.categoryToErrorMapping = errorMessages.reduce((acc, obj) => {
      const key = obj.type.name;
      if (!acc.has(key)) {
        acc.set(key, []);
      }
      acc.get(key)!.push(obj);
      return acc;
    }, new Map<string, WorkflowFatalError[]>());
  }

  /**
   * Ask workflow copilot to provide a fix for this error.
   */
  onClickFixError(error: WorkflowFatalError) {
    const focusingIDs = error.operatorId && error.operatorId !== "unknown operator" ? [error.operatorId] : [];
    this.notificationService.info("Asking copilot to fix this error...");
    // Fire suggestion request
    this.workflowSuggestionService
      .getSuggestions(
        this.workflowActionService.getWorkflow(),
        this.workflowCompilingService.getWorkflowCompilationStateInfo(),
        this.executeWorkflowService.getExecutionState(),
        "Please fix this error",
        focusingIDs,
        {}
      )
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.notificationService.success("Received fix suggestions from the copilot");
      });

    // Switch to Suggestions tab so the user sees loading spinner
    setTimeout(() => {
      const suggestionTab = document.querySelector(".ant-tabs-tab[aria-controls*=\"Suggestions\"]") as HTMLElement;
      if (suggestionTab) {
        suggestionTab.click();
      }
    }, 50);
  }
}
