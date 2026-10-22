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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgFor, NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzInputDirective } from "ng-zorro-antd/input";
import {
  CUSTOM_DELIMITER,
  DelimiterMode,
  DelimiterPreset,
  delimiterPresets,
  matchDelimiterPreset,
  unescapeCharDelimiter,
} from "./delimiter-presets";

/**
 * Delimiter picker: common delimiters are chosen by name, so nobody has to wonder whether a tab is
 * typed as a tab key press or as `\t`; anything else is typed into a box that opens under "Custom".
 * The stored value is unchanged from the plain text field this replaces -- the delimiter itself --
 * so existing workflows load as they were. `props.delimiterMode` ("char" or "regex") picks the
 * presets and how a custom value is checked (see delimiterError, registered with this type).
 *
 * The Custom box is bound by hand rather than through formlyAttributes, which would copy the schema's
 * `maxLength: 1` onto it as a `maxlength` attribute and block the second key of a typed `\t`. The
 * length is still enforced by the schema's validator.
 */
@UntilDestroy()
@Component({
  selector: "texera-delimiter-type",
  templateUrl: "./delimiter.type.html",
  styleUrls: ["./delimiter.type.scss"],
  imports: [NgFor, NgIf, FormsModule, NzSelectModule, NzInputDirective],
})
export class DelimiterTypeComponent extends FieldType<FieldTypeConfig> implements OnInit {
  readonly CUSTOM = CUSTOM_DELIMITER;

  // The value last written from the Custom box, so typing a value that happens to equal a preset does
  // not snap the select back to that preset mid-edit. A value that differs came from elsewhere (undo, a
  // co-editor, the agent), and the select follows it. Undefined while Custom is not in use.
  private customValue: unknown = undefined;

  ngOnInit(): void {
    // Any write this widget did not make itself ends the Custom edit, even when it lands back on the
    // custom value (undo, then redo).
    this.formControl.valueChanges.pipe(untilDestroyed(this)).subscribe(value => {
      if (!this.isCustomValue(value)) {
        this.customValue = undefined;
      }
    });
  }

  // Formly stores a cleared optional field as undefined rather than "", so every empty value is the
  // same empty custom value.
  private isCustomValue(value: unknown): boolean {
    if (this.customValue === undefined) {
      return false;
    }
    const isEmpty = (v: unknown) => v === undefined || v === null || v === "";
    return value === this.customValue || (isEmpty(value) && isEmpty(this.customValue));
  }

  get mode(): DelimiterMode {
    return (this.props as { delimiterMode?: DelimiterMode } | undefined)?.delimiterMode === "regex" ? "regex" : "char";
  }

  get fieldLabel(): string {
    return (this.props?.label as string | undefined) ?? "Delimiter";
  }

  get presets(): readonly DelimiterPreset[] {
    return delimiterPresets(this.mode);
  }

  /** The select's value: the matching preset, "Custom", or nothing while the property is unset. */
  get selected(): string | null {
    const value = this.formControl?.value;
    const inCustom = this.isCustomValue(value);
    const preset = matchDelimiterPreset(value, this.mode);
    if (!inCustom && preset) {
      return preset.value;
    }
    if (inCustom || (typeof value === "string" && value.length > 0)) {
      return CUSTOM_DELIMITER;
    }
    return null;
  }

  get customPlaceholder(): string {
    return this.mode === "regex" ? "Regular expression, e.g. [,;]\\s*" : "Any single character";
  }

  onSelect(choice: string): void {
    if (choice === CUSTOM_DELIMITER) {
      // Start from what is there when it is already custom; a preset's value would only be in the way.
      if (matchDelimiterPreset(this.formControl.value, this.mode)) {
        this.formControl.setValue("");
      }
      this.customValue = this.formControl.value ?? "";
    } else {
      this.customValue = undefined;
      this.formControl.setValue(choice);
    }
    this.formControl.markAsDirty();
    this.formControl.markAsTouched();
  }

  onCustomInput(event: Event): void {
    const typed = (event.target as HTMLInputElement).value;
    const value = this.mode === "char" ? unescapeCharDelimiter(typed) : typed;
    // A typed escape that names a preset (`\t`) is that preset, so the select shows it by name.
    this.customValue = value !== typed && matchDelimiterPreset(value, this.mode) ? undefined : value;
    this.formControl.setValue(value);
    this.formControl.markAsDirty();
  }
}
