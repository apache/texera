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
import { NgFor } from "@angular/common";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { ReusableSectionInsertionService } from "../../../service/workflow-snippet/reusable-section-insertion.service";
import { WorkflowSnippetService } from "../../../service/workflow-snippet/workflow-snippet.service";
import { WorkflowSnippet } from "../../../types/workflow-snippet.interface";

import { buildSectionPreview, PreviewNode, PreviewLink } from "./reusable-section-preview";

@Component({
  selector: "texera-reusable-section-detail",
  imports: [NgFor, NzButtonComponent],
  templateUrl: "./reusable-section-detail.component.html",
  styleUrls: ["./reusable-section-detail.component.scss"],
})
export class ReusableSectionDetailComponent {
  private readonly data = inject(NZ_MODAL_DATA) as { reusableSectionID: string };
  private readonly service = inject(WorkflowSnippetService);
  private readonly insertion = inject(ReusableSectionInsertionService);
  private readonly modal = inject(NzModalRef);

  readonly section: WorkflowSnippet;
  readonly nodes: readonly PreviewNode[];
  readonly links: readonly PreviewLink[];
  readonly viewBox: string;

  constructor() {
    const section = this.service.getSnippets().find(item => item.id === this.data.reusableSectionID);
    if (!section) throw new Error("The selected reusable section no longer exists.");
    this.section = section;
    const preview = buildSectionPreview(section.fragment);
    this.nodes = preview.nodes;
    this.links = preview.links;
    this.viewBox = preview.viewBox;
  }

  insert(): void {
    if (this.insertion.insert(this.section)) this.modal.close(true);
  }

  delete(): void {
    this.service.delete(this.section.id);
    this.modal.close(true);
  }
}
