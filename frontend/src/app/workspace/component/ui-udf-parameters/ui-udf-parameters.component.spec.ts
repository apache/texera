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
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
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
  let executeServiceMock: { isExecutionInFlight: Mock };

  beforeEach(async () => {
    workflowActionServiceMock = {
      checkWorkflowModificationEnabled: vitest.fn().mockReturnValue(true),
      getJointGraphWrapper: vitest.fn().mockReturnValue({
        getCurrentHighlightedOperatorIDs: () => [operatorId],
      }),
    };
    syncServiceMock = { addParameter: vitest.fn() };
    notificationServiceMock = { error: vitest.fn() };
    executeServiceMock = { isExecutionInFlight: vitest.fn().mockReturnValue(false) };

    await TestBed.configureTestingModule({
      imports: [UiUdfParametersComponent],
      providers: [
        { provide: WorkflowActionService, useValue: workflowActionServiceMock },
        { provide: UiUdfParametersSyncService, useValue: syncServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: ExecuteWorkflowService, useValue: executeServiceMock },
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

  // The operator property panel enables its whole form group when it turns interactive, and formly
  // mirrors a control's state into props.disabled, so a one-time disable of the Name and Type cells
  // did not survive there (the Form View, which never enables a group wholesale, showed them
  // locked). A locked cell's control is held through Angular's disabled-change hook.
  it("holds a locked cell locked when the host enables its control, and leaves the Value cell to the host", async () => {
    const valueControl = new FormControl("42");
    const nameControl = new FormControl("threshold");
    const typeControl = new FormControl("double");
    const rowField = rowConfig([
      { key: "value", formControl: valueControl },
      { key: "attributeName", formControl: nameControl },
      { key: "attributeType", formControl: typeControl },
    ]);
    component.onPopulate({ model: [{}], fieldGroup: [rowField] } as FormlyFieldConfig);
    expect(nameControl.disabled).toBe(true);
    const nameStatus = vitest.fn();
    nameControl.statusChanges.subscribe(nameStatus);

    nameControl.enable();
    typeControl.enable();
    valueControl.disable();
    valueControl.enable();
    // The lock goes back on in the microtask after the host's enable() has told every hook,
    // the input's value accessor included; see keepLocked.
    expect(nameControl.enabled).toBe(true);
    await Promise.resolve();

    expect(nameControl.disabled).toBe(true);
    expect(typeControl.disabled).toBe(true);
    expect(valueControl.enabled).toBe(true);
    // Announced like the enable() was: the cell's nz-input paints its disabled attribute from it.
    expect(nameStatus.mock.calls.map(call => call[0])).toEqual(["VALID", "DISABLED"]);
  });

  it("locks a cell whose control is not a FormControl without a hold hook, and does not register twice", () => {
    const group = new FormGroup({ inner: new FormControl("x") });
    (component as any).applyDisabledState({ formControl: group }, true);
    expect(group.disabled).toBe(true);

    const nameControl = new FormControl("threshold");
    const register = vitest.spyOn(nameControl, "registerOnDisabledChange");
    (component as any).applyDisabledState({ formControl: nameControl }, true);
    (component as any).applyDisabledState({ formControl: nameControl }, true);
    expect(register).toHaveBeenCalledTimes(1);
  });

  it("releases a cell from the hold once it is configured editable again", async () => {
    const nameControl = new FormControl("threshold");
    const rowField = rowConfig([
      { key: "value", formControl: new FormControl("42") },
      { key: "attributeName", formControl: nameControl },
      { key: "attributeType", formControl: new FormControl("double") },
    ]);
    component.onPopulate({ model: [{}], fieldGroup: [rowField] } as FormlyFieldConfig);
    expect(nameControl.disabled).toBe(true);

    (component as any).applyDisabledState(component.getColumnField(rowField, component.fieldColumns[1]), false);
    nameControl.disable();
    nameControl.enable();
    await Promise.resolve();

    expect(nameControl.enabled).toBe(true);
  });

  it("leaves a locked cell alone when the host has disabled it again before the microtask", async () => {
    const nameControl = new FormControl("threshold");
    const rowField = rowConfig([
      { key: "value", formControl: new FormControl("42") },
      { key: "attributeName", formControl: nameControl },
      { key: "attributeType", formControl: new FormControl("double") },
    ]);
    component.onPopulate({ model: [{}], fieldGroup: [rowField] } as FormlyFieldConfig);
    const disable = vitest.spyOn(nameControl, "disable");

    nameControl.enable();
    nameControl.disable({ emitEvent: false });
    await Promise.resolve();

    expect(disable).toHaveBeenCalledTimes(1);
    expect(nameControl.disabled).toBe(true);
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
      // The template may read the columns, and their headers, before formly hands it the field.
      expect(component.columns).toBe(component.fieldColumns);
      expect(component.columnLabel(component.fieldColumns[1])).toBe("Name");
      expect(component.columnControls(component.fieldColumns[1])).toBeUndefined();
      (component as any).field = { props: { operatorID: "op-form" } };
      expect(component.onFormView).toBe(true);
    });

    /** A row as formly builds it: the cells, which the widget reaches by key. */
    function row(): FormlyFieldConfig {
      return {
        fieldGroup: [
          { key: "value" },
          { key: "attribute", fieldGroup: [{ key: "attributeName" }, { key: "attributeType" }] },
        ],
      };
    }
    /** What the Form View publishes on the input's field: the name renamed, the type hidden. */
    const renamedAndHidden = {
      value: { label: "Value", shown: true },
      "attribute.attributeName": { label: "Parameter", shown: true },
      "attribute.attributeType": { label: "Type", shown: false },
    };
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

    /**
     * A row as the form's walk decorates it for the AUTHOR. `controls` for the row that carries them
     * (each cell with callbacks of its own, so a test can tell which column's fired), `follower`
     * for a later row, which shows the same state and has no callbacks.
     */
    function authoredRow(carriesControls = true, canHide = true): FormlyFieldConfig {
      const controls = (name: string, label: string) => ({
        label: "",
        authoring: carriesControls,
        authorName: name,
        authorHidden: false,
        canHide,
        schemaLabel: label,
        renameField: carriesControls ? vitest.fn() : undefined,
        setFieldHidden: carriesControls ? vitest.fn() : undefined,
      });
      // A rendered cell's props are observed by formly, which runs field.options.detectChanges on a
      // write; the harness has no formly build behind these rows, so give each cell that hook.
      const options = { detectChanges: () => {} } as FormlyFieldConfig["options"];
      return {
        fieldGroup: [
          { key: "value", props: controls("", "Value"), options },
          {
            key: "attribute",
            fieldGroup: [
              { key: "attributeName", props: controls("Parameter", "Attribute Name"), options },
              { key: "attributeType", props: controls("", "Attribute Type"), options },
            ],
          },
        ],
      };
    }
    const headerBoxes = () =>
      Array.from(
        fixture.nativeElement.querySelectorAll(
          ".ui-udf-parameter-row.header .col-title-input"
        ) as NodeListOf<HTMLInputElement>
      );
    const headerEyes = () =>
      Array.from(
        fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .col-eye") as NodeListOf<HTMLButtonElement>
      );

    // The form's label wrapper decorates every row's sub-fields; drawn in the cells under a header
    // that already names the column, the first row's name box and every later row's static label
    // put one name three times over. The header hosts the one pair of controls per column instead.
    /** What the form publishes while authoring: every column shown, whatever the author hid. */
    const allShownForAuthor = {
      value: { label: "", shown: true },
      "attribute.attributeName": { label: "Parameter", shown: true },
      "attribute.attributeType": { label: "", shown: true },
    };

    it("hosts the author's name box and eye for each column in its header, and tells the cells so", () => {
      const row = authoredRow();
      const later = authoredRow(false);
      const field = {
        model: [...model, { value: "7", attribute: { attributeName: "limit", attributeType: "integer" } }],
        props: { operatorID: "op-form", subFieldView: allShownForAuthor },
        fieldArray: () => row,
        fieldGroup: [row, later],
      } as FormlyFieldConfig;
      populate(field);
      fixture.detectChanges();

      // One name box and one eye per column, in the header, seeded from the first row.
      expect(headerBoxes().map(box => box.value)).toEqual(["", "Parameter", ""]);
      expect(headerBoxes().map(box => box.placeholder)).toEqual(["Value", "Name", "Type"]);
      expect(headerEyes()).toHaveLength(3);
      expect(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .col-title")).toHaveLength(0);
      // Every cell of every row is told the label lives in the header, and is named as the heading
      // reads, not as the schema does ("Type", not "Attribute Type").
      const cells = (r: FormlyFieldConfig) => [
        r.fieldGroup![0],
        r.fieldGroup![1].fieldGroup![0],
        r.fieldGroup![1].fieldGroup![1],
      ];
      for (const cell of [...cells(row), ...cells(later)]) {
        expect(cell.props?.["labelHosted"]).toBe(true);
      }
      expect(cells(later).map(cell => cell.props?.["schemaLabel"])).toEqual(["Value", "Name", "Type"]);

      // Typing in the header renames through the first row's own callback for THAT column, and the
      // box follows; the other columns' callbacks are left alone.
      const [value, name, type] = cells(row);
      const box = headerBoxes()[1];
      box.value = "Threshold";
      box.dispatchEvent(new Event("input"));
      expect(name.props?.["renameField"]).toHaveBeenCalledWith("Threshold");
      expect(name.props?.["authorName"]).toBe("Threshold");
      expect(value.props?.["renameField"]).not.toHaveBeenCalled();
      expect(type.props?.["renameField"]).not.toHaveBeenCalled();

      // The eye hides through that column's own callback; the column fades and stays in view (the
      // form publishes every column shown for an author); the eye says so; a second click un-hides.
      headerEyes()[2].click();
      fixture.detectChanges();
      expect(type.props?.["setFieldHidden"]).toHaveBeenCalledWith(true);
      expect(type.props?.["authorHidden"]).toBe(true);
      expect(name.props?.["setFieldHidden"]).not.toHaveBeenCalled();
      expect(component.columns).toHaveLength(3);
      expect(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .field-cell.dimmed")).toHaveLength(1);
      const eye = headerEyes()[2];
      expect(eye.getAttribute("aria-pressed")).toBe("true");
      expect(eye.classList.contains("off")).toBe(true);
      expect(eye.getAttribute("title")).toBe("Hidden from the form");
      eye.click();
      fixture.detectChanges();
      expect(type.props?.["setFieldHidden"]).toHaveBeenLastCalledWith(false);
      expect(type.props?.["authorHidden"]).toBe(false);
      expect(headerEyes()[2].getAttribute("aria-pressed")).toBe("false");
      expect(headerEyes()[2].getAttribute("title")).toBe("Shown on the form");
      expect(fixture.nativeElement.querySelectorAll(".ui-udf-parameter-row.header .field-cell.dimmed")).toHaveLength(0);
    });

    it("offers no eye for a column the form marked as not hideable, and keeps the mark across a populate", () => {
      const row = authoredRow(true, false);
      const field = {
        model,
        props: { operatorID: "op-form" },
        fieldArray: () => row,
        fieldGroup: [row],
      } as FormlyFieldConfig;
      populate(field);
      fixture.detectChanges();
      expect(headerBoxes()).toHaveLength(3);
      expect(headerEyes()).toHaveLength(0);

      // A second populate (every change re-runs it) finds the mark already there and keeps it,
      // along with the accessible name the host gave the cell.
      populate(field);
      expect(row.fieldGroup![0].props?.["labelHosted"]).toBe(true);
      expect(row.fieldGroup![0].props?.["schemaLabel"]).toBe("Value");
    });

    it("keeps a plain text header for a reader's card and on the operator property panel", () => {
      // A reader's rows carry no controls (authoring is off), so the header is the column's name,
      // from what the form published for the card, and no cell is told otherwise.
      const readerRow = row();
      const field = {
        model,
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: () => readerRow,
        fieldGroup: [readerRow],
      } as FormlyFieldConfig;
      populate(field);
      fixture.detectChanges();
      expect(headerBoxes()).toHaveLength(0);
      expect(headers()).toEqual(["Value", "Parameter"]);
      expect(readerRow.fieldGroup![0].props?.["labelHosted"]).toBeUndefined();

      // The panel: the same rows, no operator mark, so nothing is hosted and nothing is marked.
      const panelRow = authoredRow();
      const panel = { model, fieldArray: () => panelRow, fieldGroup: [panelRow] } as FormlyFieldConfig;
      populate(panel);
      fixture.detectChanges();
      expect(headerBoxes()).toHaveLength(0);
      expect(headers()).toEqual(["Value", "Name", "Type"]);
      expect(panelRow.fieldGroup![0].props?.["labelHosted"]).toBeUndefined();
    });

    it("shows a renamed column under its new name and leaves a hidden one out, in the header and the cells", () => {
      // formly clones the template into the rows, so a row carries what the walk put on the template.
      const field = {
        model,
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: row(),
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
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: row(),
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
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: () => row(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter"]);
    });

    it("leaves a column out when the group it sits in is hidden, not only when it is itself", () => {
      // Hiding the group is one decision the author made about the group, so that is the path the
      // form publishes; what sits inside it goes with it.
      const field = {
        model,
        props: {
          operatorID: "op-form",
          subFieldView: { value: { label: "Value", shown: true }, attribute: { label: "Attribute", shown: false } },
        },
        fieldArray: () => row(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value"]);
    });

    it("shows the author's name for a column while it is being renamed, and follows the typing", () => {
      // The form writes a name being typed into the very object it published, so the header follows
      // without a rebuild (see onSubFieldNamed).
      const view = {
        value: { label: "Value", shown: true },
        "attribute.attributeName": { label: "Parameter (draft)", shown: true },
        "attribute.attributeType": { label: "Type", shown: false },
      };
      const field = {
        model,
        props: { operatorID: "op-form", subFieldView: view },
        fieldArray: () => row(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter (draft)"]);

      view["attribute.attributeName"] = { label: "Threshold", shown: true };
      fixture.detectChanges();
      expect(headers()).toEqual(["Value", "Threshold"]);
    });

    // The old reading -- what formly left on the first built row -- had nothing to read before a row
    // existed, so a table whose UDF declares nothing yet showed the un-overridden header.
    it("answers for a table whose UDF declares no parameters yet, which has no rows to read", () => {
      const field = {
        model: [],
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: () => row(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter"]);
    });

    it("keeps the fixed headers on the operator property panel, whatever the sub-fields carry", () => {
      const field = { model, fieldArray: row(), fieldGroup: [] } as FormlyFieldConfig;
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

    it("reads the columns live, so a hide the author makes after populate takes the column out", () => {
      const view: Record<string, { label: string; shown: boolean }> = {
        value: { label: "Value", shown: true },
        "attribute.attributeName": { label: "Parameter", shown: true },
        "attribute.attributeType": { label: "Type", shown: true },
      };
      const field = {
        model,
        props: { operatorID: "op-form", subFieldView: view },
        fieldArray: row(),
        fieldGroup: [],
      } as FormlyFieldConfig;
      expect(populate(field)).toEqual(["Value", "Parameter", "Type"]);

      view["attribute.attributeType"] = { label: "Type", shown: false };

      expect(component.columns.map(column => component.columnLabel(column))).toEqual(["Value", "Parameter"]);
    });

    it("hands back the same column list while nothing changed, so the header does not redraw on every check", () => {
      const field = {
        model,
        props: { operatorID: "op-form", subFieldView: renamedAndHidden },
        fieldArray: row(),
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

    // The rule itself -- a writer's card offers it without the canvas's flag, a reader's does not,
    // nor does a writer's while a run is in flight -- is pinned under real formly in
    // ui-udf-parameters.component.formly.spec.ts; here, what the widget does with it.
    it("adds to the card's operator on a writer's card, without the canvas's flag", () => {
      // Nothing is highlighted on the form (or another step is), so the field's own operator is the
      // one the declaration goes into; the highlighted operator is the panel's rule only. And the
      // flag is off outside edit mode, where a writer's card is still editable.
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
      expect(component.draftVisible).toBe(false);
    });

    // Adding a parameter writes a declaration into the UDF's code, straight into the shared
    // document, and the property panel and the canvas code editor both refuse a code edit while a
    // run is in flight. The form's own lock cannot say so: it is held off outside edit mode for a
    // different reason, so the card asks the execute service (meng). The check sits in the method,
    // not only on the button, and the draft stays open.
    it("adds nothing on a card that is not editable, whoever calls: a reader's, or a writer's while a run is in flight", () => {
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

      executeServiceMock.isExecutionInFlight.mockReturnValue(true);
      (component as any).field = {
        model: [],
        fieldGroup: [],
        props: { operatorID: "op-form" },
        formControl: new FormGroup({}),
      };
      fixture.detectChanges();
      expect(component.editable).toBe(false);
      expect(fixture.nativeElement.querySelector(".add-parameter-button")).toBeNull();
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
