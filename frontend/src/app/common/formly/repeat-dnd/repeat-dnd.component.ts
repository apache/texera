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
import { FieldArrayType, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { CdkDragDrop, moveItemInArray, CdkDropList, CdkDrag, CdkDragHandle } from "@angular/cdk/drag-drop";
import { NgFor } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";

@Component({
  selector: "texera-formly-repeat-section-dnd",
  templateUrl: "./repeat-dnd.component.html",
  styleUrls: ["./repeat-dnd.component.css"],
  imports: [
    CdkDropList,
    NgFor,
    CdkDrag,
    CdkDragHandle,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    FormlyModule,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
  ],
})
export class FormlyRepeatDndComponent extends FieldArrayType {
  /**
   * Move the dropped row, in step: the data model (the source of truth for the backend), the formly
   * field configs (the UI definition) and the FormArray controls (the live form state), then the
   * parent's optional reorder callback (the operator property panel's, which saves without redrawing
   * the form; the Form View's card has none, its form's valueChanges persists the move). Nothing
   * moves for no model or a drop on the same position.
   */
  onDrop(event: CdkDragDrop<string[]>) {
    const from = event.previousIndex;
    const to = event.currentIndex;
    if (!this.model || from === to) {
      return;
    }
    moveItemInArray(this.model, from, to);
    moveItemInArray(this.field.fieldGroup!, from, to);
    // formly keys a row by its position when it builds the rows, and writes a cell's value into
    // the model by that key path; so the rows are re-keyed to where they now are, as formly's own
    // remove() does, or a value typed into a moved row would land on the row that used to be there.
    this.field.fieldGroup!.forEach((row, index) => this.rekey(row, `${index}`));
    const control = this.formControl.at(from);
    this.formControl.removeAt(from);
    this.formControl.insert(to, control);
    if (this.props.reorder) {
      this.props.reorder();
    }
  }

  /** formly's updateArrayElementKey, which it keeps private: the row's own key, else its keyed children's. */
  private rekey(row: FormlyFieldConfig, key: string): void {
    if (row.key !== undefined && row.key !== null && row.key !== "") {
      row.key = key;
      return;
    }
    row.fieldGroup?.forEach(child => this.rekey(child, key));
  }
}
