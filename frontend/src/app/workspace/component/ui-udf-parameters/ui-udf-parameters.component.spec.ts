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

import { FormControl, FormGroup, UntypedFormArray } from "@angular/forms";
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

  it("should edit a row that names a resource with that resource's browser, and leave others alone", () => {
    const columns = () =>
      rowConfig([
        { key: "value", formControl: new FormControl("") },
        { key: "attributeName", formControl: new FormControl("SOURCE") },
        { key: "attributeType", formControl: new FormControl("string") },
      ]);
    const resourceRow = columns();
    const plainRow = columns();
    const unknownRow = columns();

    component.onPopulate({
      model: [{ inputType: "model" }, {}, { inputType: "workflow" }],
      fieldGroup: [resourceRow, plainRow, unknownRow],
    } as FormlyFieldConfig);

    const valueOf = (row: FormlyFieldConfig) => component.getColumnField(row, component.fieldColumns[0]);
    expect(valueOf(resourceRow)?.type).toBe("resourcevalue");
    expect(valueOf(resourceRow)?.props?.resource).toBe("model");
    expect(valueOf(plainRow)?.type).toBeUndefined();
    expect(valueOf(unknownRow)?.type).toBeUndefined();
  });

  it("configures no value editor for a row formly has not built", () => {
    expect(() => (component as any).configureValueEditor(undefined, "model")).not.toThrow();
  });

  it("should find the rows before Formly narrows the field's model to them", () => {
    const rows = [{ inputType: "dataset", attribute: { attributeName: "DATA" } }];
    const operatorProperties = { code: "", uiParameters: rows };
    const field: FormlyFieldConfig = {
      key: "uiParameters",
      props: {},
      formControl: new UntypedFormArray([]),
      fieldArray: rowConfig([{ key: "value" }, { key: "attributeName" }, { key: "attributeType" }]),
      fieldGroup: [],
    };
    // Formly hands this field the whole operator's properties and narrows `model` to the
    // parameter array while the base class populates, so the first read sees the properties.
    let narrowed = false;
    Object.defineProperty(field, "model", {
      get: () => {
        const model = narrowed ? rows : operatorProperties;
        narrowed = true;
        return model;
      },
      configurable: true,
    });

    component.onPopulate(field);

    expect(component.getColumnField(field.fieldGroup![0], component.fieldColumns[0])?.type).toBe("resourcevalue");
  });

  it("should rebuild a row whose resource changed, so its cell renders the editor it now needs", () => {
    const columnKeys = [{ key: "value" }, { key: "attributeName" }, { key: "attributeType" }];
    const rows: object[] = [{ attribute: { attributeName: "DATA" } }, { attribute: { attributeName: "count" } }];
    const field: FormlyFieldConfig = {
      model: rows,
      fieldArray: rowConfig(columnKeys),
      fieldGroup: [],
    };
    const valueOf = (index: number) => component.getColumnField(field.fieldGroup![index], component.fieldColumns[0]);

    component.onPopulate(field);
    const plainRow = field.fieldGroup![0];
    expect(valueOf(0)?.type).toBeUndefined();

    // The code now declares DATA with value=Resource.DATASET.
    rows[0] = { inputType: "dataset", attribute: { attributeName: "DATA" } };
    component.onPopulate(field);
    expect(field.fieldGroup![0]).not.toBe(plainRow);
    expect(valueOf(0)?.type).toBe("resourcevalue");
    expect(valueOf(0)?.props?.resource).toBe("dataset");

    // Populating again with nothing changed keeps the rows.
    const resourceRow = field.fieldGroup![0];
    component.onPopulate(field);
    expect(field.fieldGroup![0]).toBe(resourceRow);

    // And back to free text: the browser must not linger on the row.
    rows[0] = { attribute: { attributeName: "DATA" } };
    component.onPopulate(field);
    expect(field.fieldGroup![0]).not.toBe(resourceRow);
    expect(valueOf(0)?.type).toBeUndefined();
    expect(valueOf(0)?.props?.["resource"]).toBeUndefined();
    expect(field.fieldGroup).toHaveLength(2);
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

  // The Form View's per-sub-field overrides (#8438) land on a row's sub-fields as props.label and
  // hide; this widget hides the formly labels and draws fixed headers, which papered over both (#8763).
  describe("on the Form View (props.operatorID set by the form)", () => {
    const model = [{ value: "42", attribute: { attributeName: "threshold", attributeType: "double" } }];

    it("is on the panel until formly hands it a field, and on the form once the field names its operator", () => {
      (component as any).field = undefined;
      expect(component.onFormView).toBe(false);
      // The template may read the columns before formly hands the widget its field.
      expect(component.columns).toBe(component.fieldColumns);
      (component as any).field = { props: { operatorID: "op-form" } };
      expect(component.onFormView).toBe(true);
    });

    /** A row as the form's walk decorates it for a reader: the name renamed, the type hidden. */
    function decoratedRow(): FormlyFieldConfig {
      return {
        fieldGroup: [
          { key: "value" },
          {
            key: "attribute",
            fieldGroup: [
              // The form puts a saved rename on the label and, kept apart, on authorName.
              { key: "attributeName", props: { label: "Parameter", authorName: "Parameter" } },
              { key: "attributeType", hide: true },
            ],
          },
        ],
      };
    }
    /** Populates the widget as formly's build does, and reads the headers as the template does. */
    function populate(field: FormlyFieldConfig): string[] {
      (component as any).field = field;
      component.onPopulate(field);
      return component.columns.map(column => component.columnLabel(column));
    }
    const headers = () =>
      Array.from(
        fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .col-title") as NodeListOf<HTMLElement>
      ).map(cell => cell.textContent?.trim());

    it("shows a renamed column under its new name and leaves a hidden one out, in the header and the cells", () => {
      // formly clones the template into the rows, so a row carries what the walk put on the template.
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: decoratedRow(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter"]);

      fixture.detectChanges();
      expect(headers()).toEqual(["Value", "Parameter"]);
      expect(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row:not(.header) .field-cell")).toHaveLength(2);
    });

    // The draft row names and types the parameter being added, so it draws all three cells. With a
    // column hidden, a filtered header left the name box under the wrong heading (Copilot on #8846).
    it("shows the three fixed columns while the draft row is open, so its cells line up with the header", () => {
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: decoratedRow(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter"]);

      component.draftVisible = true;
      fixture.detectChanges();

      expect(headers()).toEqual(["Value", "Parameter", "Type"]);
      const cellsIn = (selector: string) => fixture.nativeElement.querySelectorAll(selector).length;
      expect(cellsIn(".ui-udf-parameter-row.draft .field-cell")).toBe(3);
      expect(cellsIn(".ui-udf-parameter-row:not(.header):not(.draft) .field-cell")).toBe(3);

      // Closing the draft gives the author's hide back.
      component.draftVisible = false;
      fixture.detectChanges();
      expect(headers()).toEqual(["Value", "Parameter"]);
    });

    it("reads the first row when formly builds the rows on demand", () => {
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: () => decoratedRow(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter"]);
    });

    it("leaves a column out when the group it sits in is hidden, not only when it is itself", () => {
      // formly hides the children with the group but marks only the group.
      const row = decoratedRow();
      const attribute = row.fieldGroup![1];
      attribute.hide = true;
      attribute.fieldGroup![1].hide = undefined;
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: () => row,
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value"]);
    });

    it("shows the author's name for a column while it is being renamed, and follows the typing", () => {
      // While authoring, the label wrapper blanks props.label and keeps the name in props.authorName,
      // which the name box updates on every keystroke without a rebuild.
      const row = decoratedRow();
      const name = row.fieldGroup![1].fieldGroup![0];
      name.props = { label: "", authorName: "Parameter (draft)" };
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: () => row,
        fieldGroup: [row],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter (draft)"]);

      name.props["authorName"] = "Threshold";
      fixture.detectChanges();
      expect(headers()).toEqual(["Value", "Threshold"]);
    });

    it("keeps the fixed headers while there are no rows yet: nothing to read them from, and the list is not shown", () => {
      const field = {
        model: [],
        props: { operatorID: "op-form" },
        fieldArray: () => decoratedRow(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Name", "Type"]);
    });

    it("keeps the fixed headers on the operator property panel, whatever the sub-fields carry", () => {
      const field = { model, fieldArray: decoratedRow(), fieldGroup: [] } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Name", "Type"]);
    });

    it("keeps the panel's fixed header for a column the author did not rename, whatever the schema calls it", () => {
      // formly puts the schema's own title on every sub-field's label; the panel never shows it, so
      // neither does the form: only the author's name (authorName) replaces a fixed header.
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: {
          fieldGroup: [
            { key: "value", props: { label: "Value" } },
            {
              key: "attribute",
              fieldGroup: [
                { key: "attributeName", props: { label: "Attribute Name" } },
                { key: "attributeType", props: { label: "Attribute Type" } },
              ],
            },
          ],
        },
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Name", "Type"]);
    });

    it("reads the columns live, so a hide that lands on the row after populate takes the column out", () => {
      // The form's hide reaches a row's sub-field after formly has built the row; a list fixed at
      // populate kept the hidden column.
      const row = decoratedRow();
      (row.fieldGroup![1].fieldGroup![1] as FormlyFieldConfig).hide = undefined;
      const field = { model, props: { operatorID: "op-form" }, fieldArray: row, fieldGroup: [] } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter", "Type"]);

      const builtType = component.getColumnField(field.fieldGroup![0], component.fieldColumns[2])!;
      builtType.hide = true;

      expect(component.columns.map(column => component.columnLabel(column))).toEqual(["Value", "Parameter"]);
    });

    it("hands back the same column list while nothing changed, so the header does not redraw on every check", () => {
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: decoratedRow(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      populate(field);

      const first = component.columns;
      expect(component.columns).toBe(first);
    });

    it("locks the value cells of a read-only reader's card", () => {
      // The Form View marks a reader's field props.disabled; the widget's own "the value is editable"
      // used to re-enable the control after formly had disabled it.
      const value = new FormControl("42");
      const row = rowConfig([{ key: "value", formControl: value }, { key: "attributeName" }, { key: "attributeType" }]);
      const field = {
        model,
        props: { operatorID: "op-form", disabled: true },
        fieldArray: () => row,
        fieldGroup: [row],
      } as FormlyFieldConfig;

      component.onPopulate(field);

      expect(value.disabled).toBe(true);
    });

    it("offers Add parameter on the Form View as on the panel, and adds to the card's operator", () => {
      // Nothing is highlighted on the form (or another step is), so the field's own operator is the
      // one the declaration goes into; the highlighted operator is the panel's rule only.
      (component as any).field = { model: [], fieldGroup: [], props: { operatorID: "op-form" } };
      fixture.detectChanges();
      expect(fixture.nativeElement.querySelector(".add-parameter-button")).not.toBeNull();

      component.draftVisible = true;
      component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

      expect(syncServiceMock.addParameter).toHaveBeenCalledWith("op-form", "threshold", "double");
      expect(component.draftVisible).toBe(false);
    });

    it("offers Add parameter on a writer's card while the canvas's modification flag is off, as the Form View holds it outside edit mode", () => {
      workflowActionServiceMock.checkWorkflowModificationEnabled.mockReturnValue(false);
      (component as any).field = {
        model: [],
        fieldGroup: [],
        props: { operatorID: "op-form" },
        formControl: new FormGroup({}),
      };
      fixture.detectChanges();

      expect(component.editable).toBe(true);
      expect(fixture.nativeElement.querySelector(".add-parameter-button")).not.toBeNull();

      component.draftVisible = true;
      component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

      expect(syncServiceMock.addParameter).toHaveBeenCalledWith("op-form", "threshold", "double");
    });

    it("hides Add parameter on a reader's card, whose field the form disables, and adds nothing", () => {
      const control = new FormGroup({});
      control.disable();
      (component as any).field = {
        model: [],
        fieldGroup: [],
        props: { operatorID: "op-form", disabled: true },
        formControl: control,
      };
      fixture.detectChanges();

      expect(component.editable).toBe(false);
      expect(fixture.nativeElement.querySelector(".add-parameter-button")).toBeNull();

      component.draftVisible = true;
      component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

      expect(syncServiceMock.addParameter).not.toHaveBeenCalled();
      expect(component.draftVisible).toBe(true);
    });

    // formly disables controls, not arrays: a reader's table with no parameters yet has an enabled
    // array control, so the form's own mark on the field is what says the card is a reader's.
    it("hides Add parameter on a reader's card whose table has no parameters yet, its array control enabled", () => {
      (component as any).field = {
        model: [],
        fieldGroup: [],
        props: { operatorID: "op-form", disabled: true },
        formControl: new FormGroup({}),
      };
      fixture.detectChanges();

      expect(component.editable).toBe(false);
      expect(fixture.nativeElement.querySelector(".add-parameter-button")).toBeNull();

      component.draftVisible = true;
      component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

      expect(syncServiceMock.addParameter).not.toHaveBeenCalled();
    });
  });

  it("adds nothing while the workflow may not be modified, whoever calls", () => {
    workflowActionServiceMock.checkWorkflowModificationEnabled.mockReturnValue(false);
    component.draftVisible = true;

    component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

    expect(syncServiceMock.addParameter).not.toHaveBeenCalled();
    expect(component.draftVisible).toBe(true);
  });

  it("should add a parameter for the highlighted operator and close the draft row", () => {
    component.draftVisible = true;

    component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

    expect(syncServiceMock.addParameter).toHaveBeenCalledWith(operatorId, "threshold", "double");
    expect(component.draftVisible).toBe(false);
    expect(notificationServiceMock.error).not.toHaveBeenCalled();
  });

  it("adds to the highlighted operator as well before formly has handed the widget its field", () => {
    (component as any).field = undefined;
    component.draftVisible = true;

    component.addParameter({ value: "threshold" } as HTMLInputElement, "double");

    expect(syncServiceMock.addParameter).toHaveBeenCalledWith(operatorId, "threshold", "double");
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

      expect(component.editable).toBe(false);
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
