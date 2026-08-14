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
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormlyFieldConfig } from "@ngx-formly/core";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UiUdfParametersEditError } from "../../service/code-editor/ui-udf-parameters-parser.service";
import { UiUdfParametersSyncService } from "../../service/code-editor/ui-udf-parameters-sync.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { UiUdfParametersComponent } from "./ui-udf-parameters.component";

describe("UiUdfParametersComponent", () => {
  const operatorId = "operator-1";

  let fixture: ComponentFixture<UiUdfParametersComponent>;
  let component: UiUdfParametersComponent;
  let workflowActionServiceMock: {
    checkWorkflowModificationEnabled: Mock;
    getJointGraphWrapper: Mock;
  };
  let syncServiceMock: { addParameter: Mock };
  let notificationServiceMock: { error: Mock };

  beforeEach(async () => {
    workflowActionServiceMock = {
      checkWorkflowModificationEnabled: vitest.fn().mockReturnValue(true),
      getJointGraphWrapper: vitest.fn().mockReturnValue({
        getCurrentHighlightedOperatorIDs: () => [operatorId],
      }),
    };
    syncServiceMock = { addParameter: vitest.fn() };
    notificationServiceMock = { error: vitest.fn() };

    await TestBed.configureTestingModule({
      imports: [UiUdfParametersComponent],
      providers: [
        { provide: WorkflowActionService, useValue: workflowActionServiceMock },
        { provide: UiUdfParametersSyncService, useValue: syncServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(UiUdfParametersComponent);
    component = fixture.componentInstance;
  });

  it("should render the add control and draft row before existing parameters", () => {
    (component as any).field = {
      model: [{ value: "42", attribute: { attributeName: "threshold", attributeType: "double" } }],
      fieldGroup: [{}],
    } as FormlyFieldConfig;

    fixture.detectChanges();

    const addButton = fixture.nativeElement.querySelector(".add-parameter-button") as HTMLElement;
    const parameterList = fixture.nativeElement.querySelector(".ui-udf-parameter-list") as HTMLElement;
    expect(addButton.compareDocumentPosition(parameterList) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    component.draftVisible = true;
    fixture.detectChanges();

    const draftRow = fixture.nativeElement.querySelector(".ui-udf-parameter-row.draft") as HTMLElement;
    const existingRow = fixture.nativeElement.querySelector(
      ".ui-udf-parameter-row:not(.header):not(.draft)"
    ) as HTMLElement;
    expect(draftRow.compareDocumentPosition(existingRow) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
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

  it("should add a parameter for the highlighted operator and close the draft row", () => {
    component.draftVisible = true;

    component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

    expect(syncServiceMock.addParameter).toHaveBeenCalledWith(operatorId, "threshold", "double");
    expect(component.draftVisible).toBe(false);
    expect(notificationServiceMock.error).not.toHaveBeenCalled();
  });

  it("should surface edit errors and keep the draft row open", () => {
    component.draftVisible = true;
    syncServiceMock.addParameter.mockImplementation(() => {
      throw new UiUdfParametersEditError("UiParameter name 'threshold' is declared already.");
    });

    component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

    expect(notificationServiceMock.error).toHaveBeenCalledWith(
      "Could not add UDF parameter: UiParameter name 'threshold' is declared already."
    );
    expect(component.draftVisible).toBe(true);
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
