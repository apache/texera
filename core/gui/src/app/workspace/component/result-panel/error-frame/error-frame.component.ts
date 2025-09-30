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
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { render } from "sass";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";

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

  constructor(
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService
  ) {}

  ngOnInit(): void {
    this.renderError();
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
}
