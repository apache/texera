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

import { CdkDragDrop } from "@angular/cdk/drag-drop";
import { Component } from "@angular/core";
import { TestBed } from "@angular/core/testing";
import { FormArray, ReactiveFormsModule, UntypedFormGroup } from "@angular/forms";
import { By } from "@angular/platform-browser";
import { FieldType, FieldTypeConfig, FormlyFieldConfig, FormlyForm, FormlyModule } from "@ngx-formly/core";
import { FormlyRepeatDndComponent } from "./repeat-dnd.component";

/** The plainest cell formly can build a row's sub-field into. */
@Component({
  template: '<input [formControl]="formControl" />',
  imports: [ReactiveFormsModule],
})
class PlainInputType extends FieldType<FieldTypeConfig> {}

/** A form holding one repeated section, as the Form View's card and the operator property panel do. */
@Component({
  template:
    '<form [formGroup]="form"><formly-form [form]="form" [model]="model" [fields]="fields"></formly-form></form>',
  imports: [ReactiveFormsModule, FormlyForm],
})
class CardComponent {
  form = new UntypedFormGroup({});
  model: { attributes: { alias: string }[] } = { attributes: [{ alias: "a" }, { alias: "b" }, { alias: "c" }] };
  fields: FormlyFieldConfig[] = [
    {
      key: "attributes",
      type: "repeat-section-dnd",
      fieldArray: () => ({ fieldGroup: [{ key: "alias", type: "plain" }] }),
    },
  ];
}

/**
 * Real formly, no mocks: formly keys a repeated section's rows by position and writes a cell's value
 * into the model by that key path, so a move that leaves the keys where they were sends a later edit
 * to the wrong row. The widget's own spec drives the drops on flat rows; this one proves the model.
 */
describe("FormlyRepeatDndComponent under real formly", () => {
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        CardComponent,
        FormlyModule.forRoot({
          types: [
            { name: "repeat-section-dnd", component: FormlyRepeatDndComponent },
            { name: "plain", component: PlainInputType },
          ],
        }),
      ],
    }).compileComponents();
  });

  function renderCard() {
    const fixture = TestBed.createComponent(CardComponent);
    fixture.detectChanges();
    const widget = fixture.debugElement.query(By.directive(FormlyRepeatDndComponent))
      .componentInstance as FormlyRepeatDndComponent;
    const aliases = () => fixture.componentInstance.model.attributes.map(row => row.alias);
    const cells = () => Array.from(fixture.nativeElement.querySelectorAll("input") as NodeListOf<HTMLInputElement>);
    return { fixture, widget, aliases, cells };
  }

  it("moves the row in the model and on screen, and keeps the rows' keys in step with their positions", () => {
    const { fixture, widget, aliases, cells } = renderCard();

    widget.onDrop({ previousIndex: 0, currentIndex: 1 } as CdkDragDrop<string[]>);
    fixture.detectChanges();

    expect(aliases()).toEqual(["b", "a", "c"]);
    expect(cells().map(cell => cell.value)).toEqual(["b", "a", "c"]);
    expect(widget.field.fieldGroup!.map(row => row.key)).toEqual(["0", "1", "2"]);
  });

  it("writes a value typed into a moved row onto that row's entry, not onto the row that used to be there", () => {
    const { fixture, widget, aliases } = renderCard();
    widget.onDrop({ previousIndex: 0, currentIndex: 1 } as CdkDragDrop<string[]>);
    fixture.detectChanges();

    // The first row now shows "b"; typing there edits b's entry.
    (fixture.componentInstance.form.get("attributes") as FormArray).at(0).get("alias")!.setValue("typed");

    expect(aliases()).toEqual(["typed", "a", "c"]);
  });
});
