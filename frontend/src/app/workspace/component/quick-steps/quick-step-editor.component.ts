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
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzIconModule } from "ng-zorro-antd/icon";
import { QuickStepService } from "../../../dashboard/service/user/quick-step/quick-step.service";
import { WorkflowSnippetService } from "../../../dashboard/service/user/workflow-snippet/workflow-snippet.service";
import {
  QUICK_STEP_ACTION_TEMPLATES,
  QUICK_STEP_ICON_CHOICES,
  QuickStepAction,
  QuickStepActionType,
} from "../../../dashboard/type/quick-step.interface";

export interface QuickStepEditorData {
  author: string;
}

@Component({
  selector: "texera-quick-step-editor",
  templateUrl: "./quick-step-editor.component.html",
  styleUrls: ["./quick-step-editor.component.scss"],
  imports: [CommonModule, FormsModule, NzInputModule, NzButtonModule, NzSelectModule, NzIconModule],
})
export class QuickStepEditorComponent {
  protected readonly modal = inject(NzModalRef<QuickStepEditorComponent, unknown>);
  protected readonly data = inject<QuickStepEditorData>(NZ_MODAL_DATA);

  public readonly iconChoices = QUICK_STEP_ICON_CHOICES;
  public readonly actionTemplates = QUICK_STEP_ACTION_TEMPLATES;

  public name = "";
  public description = "";
  public icon = "⚡";
  public steps: QuickStepAction[] = [];

  constructor(
    private quickStepService: QuickStepService,
    private snippetService: WorkflowSnippetService
  ) {}

  public get snippetNames(): string[] {
    return this.snippetService.list().map(s => s.name);
  }

  public addStep(actionType: QuickStepActionType): void {
    const template = this.actionTemplates.find(t => t.type === actionType);
    if (!template) return;
    this.steps.push({
      order: this.steps.length + 1,
      action: actionType,
      label: template.defaultLabel,
      simulatedDurationMs: template.defaultDurationMs,
      config: {},
    });
  }

  public moveUp(index: number): void {
    if (index === 0) return;
    const reordered = this.steps.slice();
    [reordered[index - 1], reordered[index]] = [reordered[index], reordered[index - 1]];
    this.steps = reordered.map((s, idx) => ({ ...s, order: idx + 1 }));
  }

  public moveDown(index: number): void {
    if (index === this.steps.length - 1) return;
    const reordered = this.steps.slice();
    [reordered[index + 1], reordered[index]] = [reordered[index], reordered[index + 1]];
    this.steps = reordered.map((s, idx) => ({ ...s, order: idx + 1 }));
  }

  public remove(index: number): void {
    this.steps.splice(index, 1);
    this.steps = this.steps.map((s, idx) => ({ ...s, order: idx + 1 }));
  }

  public save(): void {
    if (!this.canSave()) return;
    this.quickStepService.create({
      name: this.name.trim(),
      description: this.description.trim(),
      icon: this.icon,
      steps: this.steps,
      author: this.data.author,
      isPublic: false,
    });
    this.modal.close();
  }

  public canSave(): boolean {
    return !!this.name.trim() && this.steps.length > 0;
  }

  public cancel(): void {
    this.modal.close();
  }
}
