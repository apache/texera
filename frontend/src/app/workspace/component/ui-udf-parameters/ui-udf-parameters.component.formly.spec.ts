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

/**
 * The cell formly builds a string column into: an nz-input bound to the control, as in the app.
 * nz-input paints its disabled attribute from the control's status changes, so a lock the control
 * carries silently is not one the reader sees -- which is why these tests read the inputs.
 */
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
 * Real formly, no mocks of it: the operator property panel enables and disables its whole form
 * group (setInteractivity), and formly mirrors a control's state into props.disabled, so the
 * widget's lock on the Name and Type columns has to survive a wholesale enable(). The widget's own
 * spec drives populate on hand-made rows; this one proves the lock under formly's own build.
 */
describe("UiUdfParametersComponent under real formly", () => {
  beforeEach(async () => {
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
            checkWorkflowModificationEnabled: () => true,
            getJointGraphWrapper: () => ({ getCurrentHighlightedOperatorIDs: () => ["op-1"] }),
          },
        },
        { provide: UiUdfParametersSyncService, useValue: { addParameter: vi.fn() } },
        { provide: NotificationService, useValue: { error: vi.fn() } },
      ],
    }).compileComponents();
  });

  function render() {
    const fixture = TestBed.createComponent(HostComponent);
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

  // The lock goes back on in the microtask after the host's enable() (see keepLocked), so the
  // assertions wait one; what the reader sees is read from the inputs, not only the controls.
  const settled = async (fixture: { detectChanges(): void }) => {
    await Promise.resolve();
    fixture.detectChanges();
  };
  const inputsDisabled = (fixture: { nativeElement: HTMLElement }) =>
    Array.from(fixture.nativeElement.querySelectorAll<HTMLInputElement>(".ui-udf-parameter-row:not(.header) input"))
      .slice(0, 3)
      .map(input => input.disabled);

  it("keeps the Name and Type cells locked when the host enables the whole form group, as the panel does", async () => {
    const { fixture, form, locks } = render();

    form.enable();
    await settled(fixture);

    expect(locks()).toEqual([false, true, true, true]);
    expect(inputsDisabled(fixture)).toEqual([false, true, true]);
  });

  it("locks everything for a read-only host and gives only the Value cell back on enable", async () => {
    const { fixture, form, locks } = render();

    form.disable();
    await settled(fixture);
    expect(locks()).toEqual([true, true, true, true]);
    expect(inputsDisabled(fixture)).toEqual([true, true, true]);

    form.enable();
    await settled(fixture);
    expect(locks()).toEqual([false, true, true, true]);
    expect(inputsDisabled(fixture)).toEqual([false, true, true]);
  });
});
