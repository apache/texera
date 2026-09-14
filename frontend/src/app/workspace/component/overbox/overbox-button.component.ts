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

import { Component, Input, inject } from "@angular/core";
import { NzButtonComponent, NzButtonSize } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OverboxDialogComponent } from "./overbox-dialog.component";

@Component({
  selector: "texera-overbox-button",
  imports: [NzButtonComponent, NzIconDirective],
  template: `<button
    nz-button
    [nzSize]="size"
    [disabled]="disabled"
    (click)="create()"
    title="create a section box">
    <span
      nz-icon
      nzType="border"></span>
  </button>`,
  styles: [
    `
      :host {
        display: inline-flex;
      }
      button {
        align-items: center;
        display: inline-flex;
        justify-content: center;
        padding: 0;
        width: 32px;
      }
    `,
  ],
})
export class OverboxButtonComponent {
  @Input() disabled = false;
  @Input() size: NzButtonSize = "default";
  private readonly actions = inject(WorkflowActionService);
  private readonly modal = inject(NzModalService);
  create(): void {
    if (this.disabled || !this.actions.checkWorkflowModificationEnabled()) return;
    this.modal.create({
      nzTitle: "Create section box",
      nzContent: OverboxDialogComponent,
      nzData: { operatorIDs: [...this.actions.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()] },
      nzFooter: null,
    });
  }
}
