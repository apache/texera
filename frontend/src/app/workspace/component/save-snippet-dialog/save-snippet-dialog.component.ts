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
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalRef, NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzCheckboxModule } from "ng-zorro-antd/checkbox";
import {
  DEFAULT_SNIPPET_CATEGORY,
  SNIPPET_ICON_CHOICES,
  SnippetLink,
  SnippetOperator,
} from "../../../dashboard/type/workflow-snippet.interface";

export interface SaveSnippetDialogData {
  operators: SnippetOperator[];
  links: SnippetLink[];
}

export interface SaveSnippetDialogResult {
  name: string;
  description: string;
  icon: string;
  category: string;
  isPublic: boolean;
}

@Component({
  selector: "texera-save-snippet-dialog",
  templateUrl: "./save-snippet-dialog.component.html",
  styleUrls: ["./save-snippet-dialog.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzInputModule,
    NzButtonModule,
    NzFormModule,
    NzSelectModule,
    NzCheckboxModule,
  ],
})
export class SaveSnippetDialogComponent {
  protected readonly modal = inject(NzModalRef<SaveSnippetDialogComponent, SaveSnippetDialogResult | null>);
  protected readonly data = inject<SaveSnippetDialogData>(NZ_MODAL_DATA);

  public readonly iconChoices = SNIPPET_ICON_CHOICES;
  public name = "";
  public description = "";
  public icon = "📦";
  public category = DEFAULT_SNIPPET_CATEGORY;
  public isPublic = false;

  public get operatorCount(): number {
    return this.data.operators.length;
  }

  public get linkCount(): number {
    return this.data.links.length;
  }

  public get previewLabels(): string[] {
    return this.data.operators.map(o => o.customDisplayName ?? o.operatorType);
  }

  public save(): void {
    if (!this.name.trim()) return;
    this.modal.close({
      name: this.name.trim(),
      description: this.description.trim(),
      icon: this.icon,
      category: this.category.trim() || DEFAULT_SNIPPET_CATEGORY,
      isPublic: this.isPublic,
    });
  }

  public cancel(): void {
    this.modal.close(null);
  }
}
