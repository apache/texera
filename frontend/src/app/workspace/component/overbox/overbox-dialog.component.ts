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

import { Component, inject } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzFormControlComponent, NzFormDirective, NzFormItemComponent, NzFormLabelComponent } from "ng-zorro-antd/form";
import { OverboxService } from "../../service/overbox/overbox.service";
import { normalizeOverboxColor, OVERBOX_COLORS } from "../../service/overbox/overbox-colors";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { CreateSnippetModalComponent } from "../workflow-editor/create-snippet-modal/create-snippet-modal.component";

@Component({
  selector: "texera-overbox-dialog",
  imports: [
    FormsModule,
    NgFor,
    NgIf,
    NzButtonComponent,
    NzInputDirective,
    NzFormControlComponent,
    NzFormDirective,
    NzFormItemComponent,
    NzFormLabelComponent,
  ],
  templateUrl: "overbox-dialog.component.html",
  styleUrls: ["overbox-dialog.component.scss"],
})
export class OverboxDialogComponent {
  readonly data = inject(NZ_MODAL_DATA) as { id?: string; operatorIDs?: readonly string[] };
  readonly modal = inject(NzModalRef);
  private readonly service = inject(OverboxService);
  private readonly actions = inject(WorkflowActionService);
  private readonly modalService = inject(NzModalService);
  name = "";
  color = "#1677ff";
  error = "";
  readonly colors = OVERBOX_COLORS;
  constructor() {
    const frame = this.data.id ? this.actions.getTexeraGraph().getCommentBox(this.data.id).overbox : undefined;
    if (frame) {
      this.name = frame.name;
      this.color = normalizeOverboxColor(frame.color);
    }
  }
  save(): void {
    try {
      this.persistSection();
      this.modal.close(true);
    } catch (error) {
      this.error = error instanceof Error ? error.message : "Unable to save section.";
    }
  }

  private persistSection(): string {
    if (this.data.id) {
      this.service.update(this.data.id, this.name, this.color);
      return this.data.id;
    }
    return this.service.create(this.name, this.color, this.data.operatorIDs ?? []);
  }

  saveAsReusable(): void {
    try {
      const sectionBoxID = this.persistSection();
      const name = this.name.trim();
      this.modal.close(true);
      this.modalService.create({
        nzTitle: name,
        nzContent: CreateSnippetModalComponent,
        nzData: { sectionBoxID, name },
        nzFooter: null,
      });
    } catch (error) {
      this.error = error instanceof Error ? error.message : "Unable to save section.";
    }
  }
}
