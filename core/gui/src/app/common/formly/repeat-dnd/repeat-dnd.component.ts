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
import { FieldArrayType } from "@ngx-formly/core";
import { CdkDragDrop, moveItemInArray } from "@angular/cdk/drag-drop";

@Component({
  selector: "formly-repeat-section-dnd",
  // HIGHLIGHT: The template below defines the entire component's view.
  template: `
    <div cdkDropList (cdkDropListDropped)="onDrop($event)">
      <div *ngFor="let field of field.fieldGroup; let i = index" cdkDrag class="dnd-row">
        <div class="drag-handle" cdkDragHandle>
          <i nz-icon nzType="drag" nzTheme="outline"></i>
        </div>
        <formly-field class="dnd-field" [field]="field"></formly-field>
        <button
          nz-button
          nzType="default"
          (click)="remove(i)"
          class="dnd-remove-button"
          [disabled]="field.templateOptions?.disabled"
        >
          <i nz-icon nzType="delete" nzTheme="outline"></i>
        </button>
      </div>
    </div>
    <!-- HIGHLIGHT: This is the "Add" button. If this line is missing, the button will not appear. -->
    <!-- The (click)="add()" method is provided by the 'FieldArrayType' class that we extend. -->
    <button nz-button nzType="default" (click)="add()" [disabled]="field.templateOptions?.disabled">
      {{ field.templateOptions?.addText || "Add" }}
    </button>
  `,
  styles: [
    `
      .dnd-row {
        display: flex;
        align-items: center;
        margin-bottom: 12px;
        padding: 8px;
        border: 1px solid #f0f0f0;
        border-radius: 4px;
        background-color: #fafafa;
      }
      .drag-handle {
        cursor: move;
        margin: 0 10px;
        color: #888;
      }
      .drag-handle:hover {
        color: #333;
      }
      .dnd-field {
        flex-grow: 1;
      }
      .dnd-remove-button {
        margin-left: 10px;
      }
    `,
  ],
})
export class FormlyRepeatDndComponent extends FieldArrayType {
  onDrop(event: CdkDragDrop<string[]>) {
    // If there's no model or the item wasn't moved, do nothing.
    if (!this.model || event.previousIndex === event.currentIndex) {
      return;
    }

    // 1. Reorder the data model. This is the source of truth for the backend.
    moveItemInArray(this.model, event.previousIndex, event.currentIndex);

    // 2. Reorder the Formly field configurations. This keeps the UI definition in sync with the data.
    //    The non-null assertion (!) is safe because this is a FieldArrayType.
    moveItemInArray(this.field.fieldGroup!, event.previousIndex, event.currentIndex);

    // 3. Reorder the actual Angular FormArray controls. This keeps the live form state in sync.
    const control = this.formControl.at(event.previousIndex);
    this.formControl.removeAt(event.previousIndex);
    this.formControl.insert(event.currentIndex, control);

    // 4. Notify the parent to save the changes. The parent should NOT redraw the form.
    if (this.props.reorder) {
      this.props.reorder();
    }
  }
}
