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
import { CommonModule } from "@angular/common";
import { RouterLink } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzIconModule } from "ng-zorro-antd/icon";
import { WorkflowSnippetService } from "../../../../dashboard/service/user/workflow-snippet/workflow-snippet.service";
import { WorkflowSnippet } from "../../../../dashboard/type/workflow-snippet.interface";
import { SnippetLabelComponent } from "./snippet-label.component";

@UntilDestroy()
@Component({
  selector: "texera-snippets-section",
  templateUrl: "./snippets-section.component.html",
  styleUrls: ["./snippets-section.component.scss"],
  imports: [CommonModule, RouterLink, NzCollapseModule, NzEmptyModule, NzIconModule, SnippetLabelComponent],
})
export class SnippetsSectionComponent {
  public groupedSnippets: { category: string; items: WorkflowSnippet[] }[] = [];
  public total = 0;

  constructor(private workflowSnippetService: WorkflowSnippetService) {
    this.workflowSnippetService
      .list$()
      .pipe(untilDestroyed(this))
      .subscribe(snippets => {
        this.total = snippets.length;
        const map = new Map<string, WorkflowSnippet[]>();
        for (const snippet of snippets) {
          const cat = snippet.category || "My Snippets";
          const list = map.get(cat) ?? [];
          list.push(snippet);
          map.set(cat, list);
        }
        this.groupedSnippets = Array.from(map.entries()).map(([category, items]) => ({
          category,
          items: items.slice().sort((a, b) => a.name.localeCompare(b.name)),
        }));
      });
  }
}
