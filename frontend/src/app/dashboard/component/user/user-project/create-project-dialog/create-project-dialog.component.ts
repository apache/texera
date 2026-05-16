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
import { NgFor, NgClass, NgIf, NgStyle } from "@angular/common";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzFormItemComponent, NzFormLabelComponent, NzFormControlComponent } from "ng-zorro-antd/form";

export interface CreateProjectDialogResult {
  name: string;
  description: string;
  icon: string;
  color: string;
}

export interface CreateProjectDialogData {
  mode: "create" | "edit";
  initialName?: string;
  initialDescription?: string;
  initialIcon?: string;
  initialColor?: string;
}

const PRESET_COLORS = ["ff85c0", "ff8c50", "bae637", "36cfc9", "9254de", "808080"];
const SUGGESTED_ICONS = ["📁", "🧬", "🤖", "📊", "🧪", "🔬", "💡", "🚀", "🎯", "📈"];

@Component({
  selector: "texera-create-project-dialog",
  templateUrl: "./create-project-dialog.component.html",
  styleUrls: ["./create-project-dialog.component.scss"],
  imports: [
    FormsModule,
    NgFor,
    NgClass,
    NgIf,
    NgStyle,
    NzButtonComponent,
    NzInputDirective,
    NzFormItemComponent,
    NzFormLabelComponent,
    NzFormControlComponent,
  ],
})
export class CreateProjectDialogComponent {
  readonly nzModalData: CreateProjectDialogData = inject(NZ_MODAL_DATA);
  readonly presetColors = PRESET_COLORS;
  readonly suggestedIcons = SUGGESTED_ICONS;

  name = "";
  description = "";
  icon = "📁";
  color = PRESET_COLORS[0];

  constructor(private modalRef: NzModalRef) {
    this.name = this.nzModalData.initialName ?? "";
    this.description = this.nzModalData.initialDescription ?? "";
    this.icon = this.nzModalData.initialIcon || "📁";
    this.color = this.nzModalData.initialColor || PRESET_COLORS[0];
  }

  get isEdit(): boolean {
    return this.nzModalData.mode === "edit";
  }

  get canSave(): boolean {
    return this.name.trim().length > 0;
  }

  selectIcon(emoji: string): void {
    this.icon = emoji;
  }

  selectColor(hex: string): void {
    this.color = hex;
  }

  save(): void {
    if (!this.canSave) {
      return;
    }
    const result: CreateProjectDialogResult = {
      name: this.name.trim(),
      description: this.description.trim(),
      icon: this.icon || "📁",
      color: this.color,
    };
    this.modalRef.close(result);
  }

  cancel(): void {
    this.modalRef.close(null);
  }
}
