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
import { FieldType, FieldTypeConfig, FormlyAttributes } from "@ngx-formly/core";
import { NgFor } from "@angular/common";
import { ReactiveFormsModule } from "@angular/forms";
import { NzInputDirective } from "ng-zorro-antd/input";
import {
  NzAutocompleteComponent,
  NzAutocompleteOptionComponent,
  NzAutocompleteTriggerDirective,
} from "ng-zorro-antd/auto-complete";
import { matchingValueRule } from "../../../common/formly/formly-utils";

/**
 * The control of a primitive property (string, integer, number, boolean) of an operator inside a control
 * block. A text input, so that a loop-variable reference such as "$K" can be typed where the plain
 * control (a number box, a check box) could not take it, with an autocomplete offering "$" + name for
 * every variable the enclosing Loop Starts declare (`props.loopVariableOptions`). The field's parser,
 * set by applyLoopVariableField, stores numeric or true/false text as the property's primitive and a
 * reference as the literal string, which the backend binds to the loop variable at run time. A field with
 * value rules (`props.valueRules`) keeps its text as typed, and the autocomplete also offers the values
 * accepted by the branch of the rules its row selects, which the rules' own dropdown would have listed.
 */
@Component({
  templateUrl: "loop-variable-input.component.html",
  imports: [
    NgFor,
    ReactiveFormsModule,
    FormlyAttributes,
    NzInputDirective,
    NzAutocompleteTriggerDirective,
    NzAutocompleteComponent,
    NzAutocompleteOptionComponent,
  ],
})
export class LoopVariableInputComponent extends FieldType<FieldTypeConfig> {
  /**
   * The options to offer: every "$name" option and then every value the field's rules accept while the
   * box is empty, the "$name" options matching the typed prefix once it starts with "$", and the accepted
   * values matching it, ignoring case, while a plain value is typed (none for a field without rules).
   */
  get suggestions(): string[] {
    const references: ReadonlyArray<string> = this.props["loopVariableOptions"] ?? [];
    const accepted: ReadonlyArray<string> =
      matchingValueRule(this.props["valueRules"], this.field.parent?.model)?.enum ?? [];
    const text = String(this.formControl.value ?? "");
    if (text === "") {
      return [...references, ...accepted];
    }
    const prefix = text.toLowerCase();
    return (text.startsWith("$") ? references : accepted).filter(option => option.toLowerCase().startsWith(prefix));
  }
}
