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
import { By } from "@angular/platform-browser";
import { FormlyFieldConfig } from "@ngx-formly/core";
import type { Mock } from "vitest";
import { vi as vitest } from "vitest";
import { NotificationService } from "../../../common/service/notification/notification.service";
import {
  UiUdfParametersEditError,
  UiUdfParametersParseError,
} from "../../service/code-editor/ui-udf-parameters-parser.service";
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

  describe("branch coverage", () => {
    const columnKeys = [{ key: "value" }, { key: "attributeName" }, { key: "attributeType" }];

    it("surfaces parse errors the same way as edit errors", () => {
      component.draftVisible = true;
      syncServiceMock.addParameter.mockImplementation(() => {
        throw new UiUdfParametersParseError("could not parse the UDF code");
      });

      component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

      expect(notificationServiceMock.error).toHaveBeenCalledWith(
        "Could not add UDF parameter: could not parse the UDF code"
      );
      expect(component.draftVisible).toBe(true);
    });

    it("rethrows an error that is neither an edit nor a parse error", () => {
      syncServiceMock.addParameter.mockImplementation(() => {
        throw new Error("unexpected");
      });

      expect(() => component.addParameter({ value: "threshold" } as HTMLInputElement, "double")).toThrowError(
        "unexpected"
      );
      expect(notificationServiceMock.error).not.toHaveBeenCalled();
    });

    it("skips the row template when fieldArray is a factory function", () => {
      const field: FormlyFieldConfig = {
        model: [],
        fieldArray: () => rowConfig(columnKeys),
        fieldGroup: [],
      };

      expect(() => component.onPopulate(field)).not.toThrow();
    });

    it("ignores columns that the row template does not declare", () => {
      // getColumnField returns undefined for every column here, which exercises the
      // `if (!field) return` guards in both the metadata and disabled-state helpers.
      // The generated row carries none of the expected keys, so every lookup returns
      // undefined in both the template pass and the per-row pass.
      const field: FormlyFieldConfig = {
        model: [{ value: "42" }],
        fieldArray: { fieldGroup: [] },
        fieldGroup: [],
      };

      expect(() => component.onPopulate(field)).not.toThrow();
    });

    it("tracks parameter rows by attribute name, falling back to the index", () => {
      expect(component.trackByParameterName(3, { attribute: { attributeName: "threshold" } })).toBe("threshold");
      expect(component.trackByParameterName(3, undefined)).toBe(3);
      expect(component.trackByParameterName(4, { attribute: {} })).toBe(4);
    });

    it("reapplies the disabled state when the same row is populated again", () => {
      const rowField = rowConfig(columnKeys);
      const field: FormlyFieldConfig = {
        model: [{ value: "42", attribute: { attributeName: "threshold", attributeType: "double" } }],
        fieldArray: rowConfig(columnKeys),
        fieldGroup: [rowField],
      };

      component.onPopulate(field);
      const columnField = component.getColumnField(rowField, component.fieldColumns[0]) as FormlyFieldConfig;
      const hookAfterFirstPopulate = columnField.hooks?.onInit;

      // The second pass sees the same field object already configured for this
      // disabled value, so it only re-applies the state instead of re-wrapping the hook.
      component.onPopulate(field);

      expect(columnField.hooks?.onInit).toBe(hookAfterFirstPopulate);
      expect(columnField.props?.disabled).toBe(component.fieldColumns[0].disabled);
    });
  });

  // Drives the template's own event handlers and structural branches through the
  // rendered DOM: the add / confirm / cancel buttons, the draft input's keyboard
  // shortcuts, and the three shapes the parameter list switches on (a model row
  // with a backing field group, a model row without one, and no model at all).
  describe("template interactions", () => {
    const columnKeys = [{ key: "value" }, { key: "attributeName" }, { key: "attributeType" }];

    function setField(field: Partial<FormlyFieldConfig>): void {
      (component as any).field = field as FormlyFieldConfig;
    }

    function query<T extends HTMLElement>(selector: string): T | null {
      return fixture.nativeElement.querySelector(selector) as T | null;
    }

    function draftNameInput(): HTMLInputElement {
      const input = query<HTMLInputElement>(".ui-udf-parameter-row.draft input");
      expect(input).toBeTruthy();
      return input!;
    }

    // Selects a NON-default type in the draft row. "string" is addParameterTypeOptions[0]
    // and therefore the select's untouched value, so asserting on it cannot tell reading
    // the select apart from hard-coding the first option.
    function chooseDraftType(type: string): void {
      const typeSelect = query<HTMLSelectElement>(".ui-udf-parameter-row.draft select");
      expect(typeSelect).toBeTruthy();
      typeSelect!.value = type;
      expect(typeSelect!.value).toBe(type);
    }

    function bodyRows(): HTMLElement[] {
      return Array.from(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row:not(.header):not(.draft)"));
    }

    it("opens the draft row when the add-parameter button is clicked", () => {
      setField({ model: [], fieldGroup: [] });
      fixture.detectChanges();

      // An empty model with no draft keeps the list collapsed, so only the add button shows.
      expect(query(".ui-udf-parameter-list")).toBeNull();
      const addButton = query(".add-parameter-button");
      expect(addButton).toBeTruthy();

      addButton!.click();
      fixture.detectChanges();

      expect(component.draftVisible).toBe(true);
      expect(query(".ui-udf-parameter-row.draft")).toBeTruthy();
      // The add button hides itself while the draft row is open.
      expect(query(".add-parameter-button")).toBeNull();
    });

    it("adds the parameter with the type chosen in the draft select when the confirm button is clicked", () => {
      setField({ model: [], fieldGroup: [] });
      component.draftVisible = true;
      fixture.detectChanges();

      draftNameInput().value = "threshold";
      chooseDraftType("integer");
      const confirmButton = query('button[title="Add parameter"]');
      expect(confirmButton).toBeTruthy();

      confirmButton!.click();

      // "integer" is not the select's default, so the type can only have come from reading
      // the select -- which also makes the <option [value]="parameterType"> binding load-bearing.
      expect(syncServiceMock.addParameter).toHaveBeenCalledWith(operatorId, "threshold", "integer");
      expect(component.draftVisible).toBe(false);
    });

    it("closes the draft row without adding anything when the cancel button is clicked", () => {
      setField({ model: [], fieldGroup: [] });
      component.draftVisible = true;
      fixture.detectChanges();

      draftNameInput().value = "threshold";
      const cancelButton = query('button[title="Cancel"]');
      expect(cancelButton).toBeTruthy();

      cancelButton!.click();
      fixture.detectChanges();

      expect(component.draftVisible).toBe(false);
      expect(query(".ui-udf-parameter-row.draft")).toBeNull();
      expect(syncServiceMock.addParameter).not.toHaveBeenCalled();
    });

    it("adds the parameter on Enter and closes the draft row on Escape", () => {
      setField({ model: [], fieldGroup: [] });
      component.draftVisible = true;
      fixture.detectChanges();

      const input = draftNameInput();
      input.value = "threshold";
      chooseDraftType("double");
      input.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter" }));

      expect(syncServiceMock.addParameter).toHaveBeenCalledWith(operatorId, "threshold", "double");
      expect(component.draftVisible).toBe(false);

      component.draftVisible = true;
      fixture.detectChanges();
      draftNameInput().dispatchEvent(new KeyboardEvent("keyup", { key: "Escape" }));
      fixture.detectChanges();

      expect(component.draftVisible).toBe(false);
      expect(query(".ui-udf-parameter-row.draft")).toBeNull();
      // Escape must not add a second parameter.
      expect(syncServiceMock.addParameter).toHaveBeenCalledTimes(1);
    });

    it("renders one formly-field per column for a model row backed by a field group", () => {
      const rowField = rowConfig(columnKeys);
      setField({
        model: [{ value: "42", attribute: { attributeName: "threshold", attributeType: "double" } }],
        fieldGroup: [rowField],
      });

      fixture.detectChanges();

      const rows = bodyRows();
      expect(rows.length).toBe(1);
      const cells = Array.from(rows[0].querySelectorAll(".field-cell"));
      expect(cells.length).toBe(3);
      // Exactly one formly-field per visible column, each bound to the column's own
      // config rather than to the row config, so the keys spell out the column order.
      // The keys are spelled out as literals: comparing against component.fieldColumns
      // would put the production array on both sides of the assertion.
      expect(cells.map(cell => cell.querySelectorAll("formly-field").length)).toEqual([1, 1, 1]);
      const renderedFields = fixture.debugElement
        .queryAll(By.css("formly-field"))
        .map(node => (node.componentInstance as { field: FormlyFieldConfig }).field);
      expect(renderedFields.map(field => field.key)).toEqual(["value", "attributeName", "attributeType"]);
    });

    it("binds each rendered row to its own backing field group", () => {
      const firstRow = rowConfig(columnKeys);
      const secondRow = rowConfig(columnKeys);
      setField({
        model: [{ value: "1" }, { value: "2" }],
        fieldGroup: [firstRow, secondRow],
      });

      fixture.detectChanges();

      expect(bodyRows().length).toBe(2);
      const renderedFields = fixture.debugElement
        .queryAll(By.css("formly-field"))
        .map(node => (node.componentInstance as { field: FormlyFieldConfig }).field);
      const expected = [firstRow, secondRow].flatMap(rowField =>
        component.fieldColumns.map(column => component.getColumnField(rowField, column))
      );
      expect(renderedFields.length).toBe(6);
      // Object identity, not key equality: both rows declare the same three keys, so only
      // identity shows that the second row renders the SECOND row's configs. A row index
      // that always resolved to fieldGroup[0] would make every row edit the first parameter.
      renderedFields.forEach((field, index) => expect(field).toBe(expected[index]));
    });

    it("renders no cells for a model row whose backing field group is missing", () => {
      setField({ model: [{ value: "42" }] });

      fixture.detectChanges();

      const rows = bodyRows();
      expect(rows.length).toBe(1);
      expect(rows[0].querySelectorAll(".field-cell").length).toBe(0);
      expect(fixture.nativeElement.querySelectorAll("formly-field").length).toBe(0);
    });

    it("renders the header and draft rows alone when the field carries no model", () => {
      setField({ fieldGroup: [] });
      component.draftVisible = true;

      fixture.detectChanges();

      // The draft row alone keeps the list open even though `model` is undefined.
      expect(query(".ui-udf-parameter-list")).toBeTruthy();
      expect(query(".ui-udf-parameter-row.header")).toBeTruthy();
      expect(query(".ui-udf-parameter-row.draft")).toBeTruthy();
      expect(bodyRows().length).toBe(0);
      // Spelled out as literals rather than read back off fieldColumns: these are the
      // human-facing headings, and `label` is used nowhere else in the component.
      expect(
        Array.from(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .col-title")).map(node =>
          ((node as HTMLElement).textContent ?? "").trim()
        )
      ).toEqual(["Value", "Name", "Type"]);
    });

    it("hides the add-parameter button while the workflow may not be modified", () => {
      workflowActionServiceMock.checkWorkflowModificationEnabled.mockReturnValue(false);
      setField({ model: [], fieldGroup: [] });

      fixture.detectChanges();

      expect(component.workflowModificationEnabled).toBe(false);
      // Offering "Add parameter" on a read-only or running workflow would push a code edit
      // through the sync service onto a graph that must not be modified.
      expect(query(".add-parameter-button")).toBeNull();

      workflowActionServiceMock.checkWorkflowModificationEnabled.mockReturnValue(true);
      fixture.detectChanges();

      // Re-enabling modification brings it back, so the guard is pinned in both directions.
      expect(query(".add-parameter-button")).toBeTruthy();
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
