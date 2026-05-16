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

import { AfterContentInit, Component, Input } from "@angular/core";
import { CdkDrag, CdkDragPreview, CdkDropList } from "@angular/cdk/drag-drop";
import { NgClass, NgIf } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Point } from "../../../../types/workflow-common.interface";
import { DragDropService } from "../../../../service/drag-drop/drag-drop.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { CustomOperator } from "../../../../../dashboard/type/custom-operator.interface";
import { CustomOperatorFactoryService } from "../../../../../dashboard/service/user/custom-operator/custom-operator-factory.service";

@UntilDestroy()
@Component({
  selector: "texera-custom-operator-label",
  templateUrl: "./custom-operator-label.component.html",
  styleUrls: ["./custom-operator-label.component.scss"],
  imports: [CdkDropList, CdkDrag, CdkDragPreview, NgClass, NgIf],
})
export class CustomOperatorLabelComponent implements AfterContentInit {
  @Input() operator?: CustomOperator;

  public draggable = true;

  constructor(
    private dragDropService: DragDropService,
    private workflowActionService: WorkflowActionService,
    private customOperatorFactory: CustomOperatorFactoryService
  ) {}

  ngAfterContentInit(): void {
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.draggable = canModify));
  }

  public dragStarted(): void {
    if (!this.draggable || !this.operator) return;
    const predicate = this.customOperatorFactory.buildPredicate(this.operator);
    if (!predicate) return;
    this.dragDropService.dragStartedCustom(predicate);
  }

  public dragDropped(dropPoint: Point): void {
    this.dragDropService.dragDropped(dropPoint);
  }
}
