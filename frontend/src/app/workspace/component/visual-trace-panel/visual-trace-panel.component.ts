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

import { NgClass, NgFor, NgIf } from "@angular/common";
import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { VisualTraceService } from "../../service/visual-trace/visual-trace.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { VisualTrace, VisualTraceStep, VisualTraceStepKind } from "../../types/visual-trace.interface";

@UntilDestroy()
@Component({
  selector: "texera-visual-trace-panel",
  templateUrl: "./visual-trace-panel.component.html",
  styleUrls: ["./visual-trace-panel.component.scss"],
  imports: [NgIf, NgFor, NgClass, NzIconDirective],
})
export class VisualTracePanelComponent implements OnInit {
  public trace?: VisualTrace;

  constructor(
    private readonly visualTraceService: VisualTraceService,
    private readonly workflowActionService: WorkflowActionService
  ) {}

  ngOnInit(): void {
    this.visualTraceService.trace$.pipe(untilDestroyed(this)).subscribe(trace => {
      this.trace = trace;
    });
  }

  public close(): void {
    this.visualTraceService.closeTrace();
  }

  public focusOperator(step: VisualTraceStep): void {
    if (!step.operatorId || !this.workflowActionService.getTexeraGraph().hasOperator(step.operatorId)) {
      return;
    }
    this.workflowActionService.highlightOperators(false, step.operatorId);
  }

  public getStepLabel(step: VisualTraceStep): string {
    if (step.operatorLabel) {
      return step.operatorLabel;
    }
    if (!step.operatorId || !this.workflowActionService.getTexeraGraph().hasOperator(step.operatorId)) {
      return this.getKindLabel(step.kind);
    }
    const operator = this.workflowActionService.getTexeraGraph().getOperator(step.operatorId);
    return operator.customDisplayName ?? operator.operatorType;
  }

  public getKindLabel(kind?: VisualTraceStepKind): string {
    switch (kind) {
      case "source":
        return "Source";
      case "match":
        return "Match";
      case "compute":
        return "Compute";
      case "render":
        return "Render";
      default:
        return "Step";
    }
  }
}
