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

import { FormControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { UiUdfParametersComponent } from "./ui-udf-parameters.component";

describe("UiUdfParametersComponent", () => {
  let component: UiUdfParametersComponent;

  beforeEach(() => {
    component = new UiUdfParametersComponent();
  });

  it("should disable name and type fields while leaving value editable", () => {
    const valueControl = new FormControl({ value: "42", disabled: true });
    const nameControl = new FormControl("threshold");
    const typeControl = new FormControl("double");

    const rowField = rowConfig([
      { key: "value", formControl: valueControl },
      { key: "attributeName", formControl: nameControl },
      { key: "attributeType", formControl: typeControl },
    ]);

    (component as any).field = { model: [{}], fieldGroup: [rowField] } as FormlyFieldConfig;

    component.onPopulate((component as any).field);

    // templateOptions is deprecated, but some existing Formly wrappers still read it.
    [
      {
        column: component.fieldColumns[0],
        field: component.getColumnField(rowField, component.fieldColumns[0]),
        control: valueControl,
      },
      {
        column: component.fieldColumns[1],
        field: component.getColumnField(rowField, component.fieldColumns[1]),
        control: nameControl,
      },
      {
        column: component.fieldColumns[2],
        field: component.getColumnField(rowField, component.fieldColumns[2]),
        control: typeControl,
      },
    ].forEach(({ column, field, control }) => {
      expect(component.getColumnField(rowField, column)).toBe(field);
      const disabled = column.disabled;
      expect((field as FormlyFieldConfig).props?.disabled).toBe(disabled);
      expect((field as any).templateOptions?.disabled).toBe(disabled);
      expect((control as FormControl).disabled).toBe(disabled);
    });
  });

  it("should apply disabled state to rows generated from the field array template", () => {
    const field: FormlyFieldConfig = {
      model: [{ value: "42", attribute: { attributeName: "threshold", attributeType: "double" } }],
      fieldArray: rowConfig([{ key: "value" }, { key: "attributeName" }, { key: "attributeType" }]),
      fieldGroup: [],
    };

    component.onPopulate(field);

    const generatedRow = field.fieldGroup?.[0] as FormlyFieldConfig;
    const valueControl = new FormControl({ value: "42", disabled: true });
    const nameControl = new FormControl("threshold");
    const typeControl = new FormControl("double");

    [
      { column: component.fieldColumns[0], control: valueControl },
      { column: component.fieldColumns[1], control: nameControl },
      { column: component.fieldColumns[2], control: typeControl },
    ].forEach(({ column, control }) => {
      const columnField = component.getColumnField(generatedRow, column) as FormlyFieldConfig;
      Object.assign(columnField, { formControl: control });
      columnField.hooks?.onInit?.(columnField);

      expect(columnField.props?.disabled).toBe(column.disabled);
      expect((columnField as any).templateOptions?.disabled).toBe(column.disabled);
      expect(control.disabled).toBe(column.disabled);
    });
  });
});

function rowConfig(fields: ReadonlyArray<{ key: string; formControl?: FormControl }>): FormlyFieldConfig {
  const [valueField, nameField, typeField] = fields.map(field => ({
    key: field.key,
    formControl: field.formControl,
  }));

  return {
    fieldGroup: [
      valueField,
      {
        key: "attribute",
        fieldGroup: [nameField, typeField],
      },
    ],
  };
}
