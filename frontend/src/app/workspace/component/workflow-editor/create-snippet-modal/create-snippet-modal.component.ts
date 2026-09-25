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
import { FormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective, NzAutosizeDirective } from "ng-zorro-antd/input";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";

export interface CreateSnippetModalData {
  sectionBoxID: string;
  name: string;
}

@Component({
  selector: "texera-create-snippet-modal",
  templateUrl: "./create-snippet-modal.component.html",
  styleUrls: ["./create-snippet-modal.component.scss"],
  imports: [FormsModule, NzButtonComponent, NzInputDirective, NzAutosizeDirective],
})
export class CreateSnippetModalComponent {
  public description = "";
  public readonly data: CreateSnippetModalData = inject(NZ_MODAL_DATA);

  constructor(
    private readonly modalRef: NzModalRef,
    private readonly snippetService: WorkflowSnippetService,
    private readonly notificationService: NotificationService
  ) {}

  public save(): void {
    try {
      this.snippetService.createFromSectionBox(this.data.sectionBoxID, this.description);
      this.notificationService.success(`Reusable section box "${this.data.name}" saved.`);
      this.modalRef.close(true);
    } catch (error) {
      this.notificationService.error(error instanceof Error ? error.message : "Unable to save the reusable section.");
    }
  }

  public cancel(): void {
    this.modalRef.close(false);
  }
}
