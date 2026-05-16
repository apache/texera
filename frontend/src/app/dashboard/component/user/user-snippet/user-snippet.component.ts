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

import { Component, OnInit } from "@angular/core";
import { CommonModule } from "@angular/common";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { NzTooltipModule } from "ng-zorro-antd/tooltip";
import { NzTagModule } from "ng-zorro-antd/tag";
import { WorkflowSnippetService } from "../../../service/user/workflow-snippet/workflow-snippet.service";
import { WorkflowSnippet } from "../../../type/workflow-snippet.interface";
import { UserService } from "../../../../common/service/user/user.service";
import { SnippetBuilderDialogComponent } from "./snippet-builder/snippet-builder-dialog.component";

@UntilDestroy()
@Component({
  selector: "texera-user-snippet",
  templateUrl: "./user-snippet.component.html",
  styleUrls: ["./user-snippet.component.scss"],
  imports: [
    CommonModule,
    NzButtonModule,
    NzIconModule,
    NzCardModule,
    NzEmptyModule,
    NzPopconfirmModule,
    NzTooltipModule,
    NzTagModule,
  ],
})
export class UserSnippetComponent implements OnInit {
  public snippets: WorkflowSnippet[] = [];

  constructor(
    private workflowSnippetService: WorkflowSnippetService,
    private modalService: NzModalService,
    private userService: UserService
  ) {}

  public openCreate(): void {
    const author = this.userService.getCurrentUser()?.name ?? "you";
    this.modalService.create({
      nzTitle: "Create snippet from operator catalog",
      nzContent: SnippetBuilderDialogComponent,
      nzData: { author },
      nzFooter: null,
      nzWidth: 920,
    });
  }

  ngOnInit(): void {
    this.workflowSnippetService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(snippets => (this.snippets = snippets));
  }

  public openEdit(snippet: WorkflowSnippet): void {
    const author = this.userService.getCurrentUser()?.name ?? "you";
    this.modalService.create({
      nzTitle: `Edit snippet — ${snippet.name}`,
      nzContent: SnippetBuilderDialogComponent,
      nzData: { author, editing: snippet },
      nzFooter: null,
      nzWidth: 920,
    });
  }

  public delete(snippet: WorkflowSnippet): void {
    this.workflowSnippetService.delete(snippet.id);
  }

  public previewLabels(snippet: WorkflowSnippet): string[] {
    return snippet.operators.map(o => o.customDisplayName ?? o.operatorType);
  }
}
