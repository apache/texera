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

import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { AfterContentInit, Component, Input } from "@angular/core";
import { OperatorSchema } from "../../../../types/operator-schema.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Point } from "../../../../types/workflow-common.interface";

@UntilDestroy()
@Component({
  selector: "texera-operator-label",
  templateUrl: "operator-label.component.html",
  styleUrls: ["operator-label.component.scss"],
})
export class OperatorLabelComponent implements AfterContentInit {
  @Input() operator?: OperatorSchema;
  public draggable = true;

  constructor(
    private dragDropService: DragDropService,
    private workflowActionService: WorkflowActionService
  ) {}

  ngAfterContentInit(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => {
        this.draggable = canModify;
      });
  }

  dragStarted() {
    if (this.draggable) {
      this.dragDropService.dragStarted(this.operator!.operatorType);
    }
  }

  dragDropped(dropPoint: Point) {
    this.dragDropService.dragDropped(dropPoint);
  }
}
