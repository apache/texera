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

import { NgFor } from "@angular/common";
import { Component, Inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { isWorkflowColor, WORKFLOW_COLOR_PRESETS, WorkflowColor } from "../../../types/workflow-color";

@Component({
  selector: "texera-operator-color-picker",
  templateUrl: "./operator-color-picker.component.html",
  styleUrls: ["./operator-color-picker.component.scss"],
  imports: [NgFor],
})
export class OperatorColorPickerComponent {
  public readonly presets = WORKFLOW_COLOR_PRESETS;

  constructor(
    @Inject(NZ_MODAL_DATA) public readonly data: { operatorIDs: readonly string[] },
    private readonly workflowActionService: WorkflowActionService
  ) {}

  public get canEdit(): boolean {
    return (
      this.workflowActionService.checkWorkflowModificationEnabled() &&
      !this.workflowActionService.getWorkflowMetadata().readonly &&
      this.data.operatorIDs.length > 0 &&
      this.data.operatorIDs.every(id => this.workflowActionService.getTexeraGraph().hasOperator(id))
    );
  }

  public isSelected(color: WorkflowColor | undefined): boolean {
    const graph = this.workflowActionService.getTexeraGraph();
    return (
      this.data.operatorIDs.length > 0 &&
      this.data.operatorIDs.every(id => {
        if (!graph.hasOperator(id)) return false;
        const value = graph.getOperator(id).color;
        return (isWorkflowColor(value) ? value : undefined) === color;
      })
    );
  }

  public setColor(color: WorkflowColor | undefined): void {
    if (this.canEdit) this.workflowActionService.setOperatorsColor(this.data.operatorIDs, color);
  }
}
