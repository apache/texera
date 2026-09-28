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

import { Component } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowSnippet } from "../../../types/workflow-snippet.interface";
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { ReusableSectionInsertionService } from "../../../service/workflow-snippet/reusable-section-insertion.service";
import { ReusableSectionDetailComponent } from "./reusable-section-detail.component";

@UntilDestroy()
@Component({
  selector: "texera-my-snippets",
  templateUrl: "./my-snippets.component.html",
  styleUrls: ["./my-snippets.component.scss"],
  imports: [NgFor, NgIf, NzButtonComponent, NzModalModule],
})
export class MySnippetsComponent {
  public snippets: readonly WorkflowSnippet[] = [];
  public canModify = true;

  constructor(
    private readonly snippetService: WorkflowSnippetService,
    private readonly workflowActionService: WorkflowActionService,
    private readonly insertion: ReusableSectionInsertionService,
    private readonly modalService: NzModalService
  ) {
    this.snippetService.snippets$.pipe(untilDestroyed(this)).subscribe(snippets => (this.snippets = snippets));
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(untilDestroyed(this))
      .subscribe(canModify => (this.canModify = canModify));
  }

  public insert(snippet: WorkflowSnippet): void {
    this.insertion.insert(snippet);
  }

  public open(snippet: WorkflowSnippet): void {
    this.modalService.create({
      nzTitle: snippet.name,
      nzContent: ReusableSectionDetailComponent,
      nzData: { reusableSectionID: snippet.id },
      nzFooter: null,
    });
  }
}
