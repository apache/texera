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

import { ChangeDetectionStrategy, Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyModule } from "@ngx-formly/core";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzSelectModule } from "ng-zorro-antd/select";
import { matchingValueRule } from "../../../common/formly/formly-utils";

/**
 * A field whose accepted values depend on what a sibling field holds: a chosen-from-a-set
 * parameter renders as a dropdown, a numeric one as a number input, and anything the rules do
 * not cover stays a plain text box.
 *
 * The value stays a string whichever control is showing. Operators that read one of these put
 * the text through a converter of their own, so handing them a JSON number instead would only
 * move the coercion somewhere less visible.
 */
@Component({
  selector: "texera-constrained-value",
  standalone: true,
  imports: [CommonModule, FormsModule, FormlyModule, NzInputModule, NzSelectModule],
  changeDetection: ChangeDetectionStrategy.Default,
  template: `
    <nz-select
      *ngIf="acceptedValues.length > 0; else freeInput"
      [ngModel]="current"
      (ngModelChange)="write($event)"
      [nzDisabled]="to.disabled ?? false"
      nzAllowClear>
      <nz-option
        *ngFor="let accepted of acceptedValues"
        [nzValue]="accepted"
        [nzLabel]="accepted"></nz-option>
    </nz-select>

    <ng-template #freeInput>
      <input
        nz-input
        [type]="inputType"
        [disabled]="to.disabled ?? false"
        [ngModel]="current"
        (ngModelChange)="write($event)" />
    </ng-template>
  `,
})
export class ConstrainedValueComponent extends FieldType<FieldTypeConfig> {
  /** The branch of the rules that the sibling's current value selects, if any. */
  private get rule() {
    return matchingValueRule(this.props.valueRules, this.field?.parent?.model);
  }

  get acceptedValues(): ReadonlyArray<string> {
    return this.rule?.enum ?? [];
  }

  /** A number input where the rules call for a number, so a keyboard offers digits and the
   * browser refuses most of what the converter would reject.
   */
  get inputType(): string {
    return this.rule?.type === undefined ? "text" : "number";
  }

  get current(): string {
    return this.formControl.value ?? "";
  }

  /** Writes the control as a string whatever the control was. `nz-select` clears to null and a
   * number input yields a number once its value parses, and both reach an operator expecting
   * text.
   */
  write(raw: unknown): void {
    this.formControl.setValue(raw === null || raw === undefined ? "" : String(raw));
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
  }
}
