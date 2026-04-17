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
  selector: "texera-formly-repeat-section-dnd",
  templateUrl: "./repeat-dnd.component.html",
  styleUrls: ["./repeat-dnd.component.css"],
})
export class FormlyRepeatDndComponent extends FieldArrayType {
  onDrop(event: CdkDragDrop<string[]>) {
    if (!this.model || event.previousIndex === event.currentIndex) {
      return;
    }

    // 1. Reorder the data model. This is the source of truth for the backend.
    moveItemInArray(this.model, event.previousIndex, event.currentIndex);

    // 2. Reorder the Formly field configurations. This keeps the UI definition in sync with the data.
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
