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
import { NgFor, NgIf } from "@angular/common";
import { FieldArrayType, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";

type UiUdfParameterColumn = Readonly<{ label: string; key: string; parentKey?: string; disabled: boolean }>;

/** Renders inferred Python UDF UI parameters with editable values and locked name/type columns. */
@Component({
  selector: "texera-ui-udf-parameters",
  templateUrl: "./ui-udf-parameters.component.html",
  styleUrls: ["./ui-udf-parameters.component.scss"],
  imports: [NgIf, NgFor, FormlyModule],
})
export class UiUdfParametersComponent extends FieldArrayType<FormlyFieldConfig> {
  private readonly disabledStateConfigured = new WeakMap<FormlyFieldConfig, boolean>();

  readonly fieldColumns: UiUdfParameterColumn[] = [
    { label: "Value", key: "value", disabled: false },
    { label: "Name", key: "attributeName", parentKey: "attribute", disabled: true },
    { label: "Type", key: "attributeType", parentKey: "attribute", disabled: true },
  ];

  override onPopulate(field: FormlyFieldConfig): void {
    this.configureRowTemplate(this.getFieldArrayTemplate(field));
    super.onPopulate(field);
    field.fieldGroup?.forEach(rowField => this.configureRowFields(rowField));
  }

  /** Finds the Formly field config that backs one visible column in a parameter row. */
  getColumnField(rowField: FormlyFieldConfig, column: UiUdfParameterColumn): FormlyFieldConfig | undefined {
    return this.getChildField(column.parentKey ? this.getChildField(rowField, column.parentKey) : rowField, column.key);
  }

  private getFieldArrayTemplate(field: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return typeof field.fieldArray === "function" ? undefined : field.fieldArray;
  }

  private configureRowTemplate(rowField: FormlyFieldConfig | undefined): void {
    this.configureRowColumns(rowField, this.setDisabledMetadata.bind(this));
  }

  private configureRowFields(rowField: FormlyFieldConfig | undefined): void {
    this.configureRowColumns(rowField, this.configureDisabledState.bind(this));
  }

  private configureRowColumns(
    rowField: FormlyFieldConfig | undefined,
    configureColumn: (field: FormlyFieldConfig | undefined, disabled: boolean) => void
  ): void {
    if (!rowField) return;

    this.fieldColumns.forEach(column => configureColumn(this.getColumnField(rowField, column), column.disabled));
  }

  private getChildField(rowField: FormlyFieldConfig | undefined, key: string): FormlyFieldConfig | undefined {
    return rowField?.fieldGroup?.find(fieldConfig => fieldConfig.key === key);
  }

  /** Sets Formly disabled metadata and keeps controls created later in sync through an onInit hook. */
  private configureDisabledState(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;

    this.setDisabledMetadata(field, disabled);

    if (this.disabledStateConfigured.get(field) === disabled) {
      this.applyDisabledState(field, disabled);
      return;
    }

    const previousOnInit = field.hooks?.onInit;
    field.hooks = {
      ...(field.hooks ?? {}),
      onInit: initializedField => {
        previousOnInit?.(initializedField);
        this.applyDisabledState(initializedField, disabled);
      },
    };

    this.disabledStateConfigured.set(field, disabled);
    this.applyDisabledState(field, disabled);
  }

  private setDisabledMetadata(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;

    field.props = { ...(field.props ?? {}), disabled };

    // Keep deprecated templateOptions in sync for existing Formly wrappers that still read it.
    (field as any).templateOptions = { ...((field as any).templateOptions ?? {}), disabled };
  }

  private applyDisabledState(field: FormlyFieldConfig, disabled: boolean): void {
    if (disabled) field.formControl?.disable({ emitEvent: false });
    else field.formControl?.enable({ emitEvent: false });
  }

  trackByParameterName = (index: number, parameter: any): string | number => {
    return parameter?.attribute?.attributeName ?? index;
  };
}
