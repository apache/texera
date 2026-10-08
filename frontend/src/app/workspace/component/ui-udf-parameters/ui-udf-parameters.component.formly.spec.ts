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
import { TestBed } from "@angular/core/testing";
import { ReactiveFormsModule, UntypedFormGroup } from "@angular/forms";
import { FieldType, FieldTypeConfig, FormlyFieldConfig, FormlyForm, FormlyModule } from "@ngx-formly/core";
import { NzInputModule } from "ng-zorro-antd/input";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { UiUdfParametersSyncService } from "../../service/code-editor/ui-udf-parameters-sync.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { UiUdfParametersComponent } from "./ui-udf-parameters.component";

/** The cell formly builds a string column into: an nz-input bound to the control, as in the app. */
@Component({
  template: '<input nz-input [formControl]="formControl" />',
  imports: [ReactiveFormsModule, NzInputModule],
})
class InputCellType extends FieldType<FieldTypeConfig> {}

/** A form holding one parameter table, with the builder row template formly-jsonschema produces. */
@Component({
  template:
    '<form [formGroup]="form"><formly-form [form]="form" [model]="model" [fields]="fields"></formly-form></form>',
  imports: [ReactiveFormsModule, FormlyForm],
})
class HostComponent {
  form = new UntypedFormGroup({});
  model = {
    uiParameters: [
      { value: "0.5", attribute: { attributeName: "threshold", attributeType: "double" } },
      { value: "high", attribute: { attributeName: "label", attributeType: "string" } },
    ],
  };
  fields: FormlyFieldConfig[] = [
    {
      key: "uiParameters",
      type: "ui-udf-parameters",
      fieldArray: () => ({
        fieldGroup: [
          { key: "value", type: "plain" },
          {
            key: "attribute",
            fieldGroup: [
              { key: "attributeName", type: "plain" },
              { key: "attributeType", type: "plain" },
            ],
          },
        ],
      }),
    },
  ];
}

/**
 * Real formly, no mocks of it: the widget's own spec drives populate on hand-made rows, which says
 * nothing about what formly builds from a row template or what it does with a field the Form View
 * marked for a reader. This one stands the table up the way the app does -- a form, the builder row
 * template formly-jsonschema produces, nz-input cells -- so the rows are formly's own and the
 * reader's lock is the one formly applied.
 */
describe("UiUdfParametersComponent under real formly", () => {
  /** The canvas's modification flag, which the Form View holds off outside edit mode. */
  let modificationEnabled = true;

  beforeEach(async () => {
    modificationEnabled = true;
    await TestBed.configureTestingModule({
      imports: [
        HostComponent,
        FormlyModule.forRoot({
          types: [
            { name: "ui-udf-parameters", component: UiUdfParametersComponent },
            { name: "plain", component: InputCellType },
          ],
        }),
      ],
      providers: [
        {
          provide: WorkflowActionService,
          useValue: {
            checkWorkflowModificationEnabled: () => modificationEnabled,
            getJointGraphWrapper: () => ({ getCurrentHighlightedOperatorIDs: () => ["op-1"] }),
          },
        },
        { provide: UiUdfParametersSyncService, useValue: { addParameter: vi.fn() } },
        { provide: NotificationService, useValue: { error: vi.fn() } },
      ],
    }).compileComponents();
  });

  /**
   * Renders the host; `topProps` go on the table's field (the Form View puts its marks there), `rows`
   * replace the two parameters the model declares by default.
   */
  function render(topProps?: Record<string, unknown>, rows?: unknown[]) {
    const fixture = TestBed.createComponent(HostComponent);
    if (topProps) {
      fixture.componentInstance.fields[0].props = topProps;
    }
    if (rows) {
      fixture.componentInstance.model = { uiParameters: rows as HostComponent["model"]["uiParameters"] };
    }
    fixture.detectChanges();
    const form = fixture.componentInstance.form;
    const cell = (row: number, path: string) => form.get(`uiParameters.${row}.${path}`)!;
    const locks = () => [
      cell(0, "value").disabled,
      cell(0, "attribute.attributeName").disabled,
      cell(0, "attribute.attributeType").disabled,
      cell(1, "attribute.attributeName").disabled,
    ];
    return { fixture, form, locks };
  }

  it("builds the rows with the Name and Type cells locked and the Value cell editable", () => {
    const { locks } = render();

    expect(locks()).toEqual([false, true, true, true]);
  });

  const addButton = (fixture: { nativeElement: HTMLElement }) =>
    fixture.nativeElement.querySelector(".add-parameter-button");

  it("offers Add parameter on a writer's Form View card without the canvas's flag, and not on a reader's, whose field is disabled", () => {
    modificationEnabled = false;

    const writer = render({ operatorID: "op-form" });
    expect(addButton(writer.fixture)).not.toBeNull();

    const reader = render({ operatorID: "op-form", disabled: true });
    expect(reader.locks()).toEqual([true, true, true, true]);
    expect(addButton(reader.fixture)).toBeNull();
  });

  // formly disables the controls it builds under a disabled field, not the array itself, and an
  // array with no rows reads as enabled; the form's mark on the field is what says "a reader's".
  it("offers Add parameter to a writer whose UDF declares no parameters yet, and not to a reader, though the empty array control is enabled", () => {
    modificationEnabled = false;

    const writer = render({ operatorID: "op-form" }, []);
    expect(writer.form.get("uiParameters")!.enabled).toBe(true);
    expect(addButton(writer.fixture)).not.toBeNull();

    const reader = render({ operatorID: "op-form", disabled: true }, []);
    expect(reader.form.get("uiParameters")!.enabled).toBe(true);
    expect(addButton(reader.fixture)).toBeNull();
  });
});
