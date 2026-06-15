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
import { FormArray, FormControl } from "@angular/forms";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormlyRepeatDndComponent } from "./repeat-dnd.component";

describe("FormlyRepeatDndComponent", () => {
  let component: FormlyRepeatDndComponent;
  let fixture: ComponentFixture<FormlyRepeatDndComponent>;

  const createDropEvent = (previousIndex: number, currentIndex: number): CdkDragDrop<string[]> =>
    ({ previousIndex, currentIndex }) as CdkDragDrop<string[]>;

  const setComponentState = (reorder = vi.fn()) => {
    const formControl = new FormArray([new FormControl("a"), new FormControl("b"), new FormControl("c")]);

    component.field = {
      model: ["a", "b", "c"],
      fieldGroup: [{ key: "a" }, { key: "b" }, { key: "c" }],
      formControl,
      props: { reorder },
    } as any;

    return reorder;
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FormlyRepeatDndComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(FormlyRepeatDndComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    setComponentState();

    expect(component).toBeTruthy();
  });

  it("should do nothing when previousIndex equals currentIndex", () => {
    const reorder = setComponentState();

    component.onDrop(createDropEvent(1, 1));

    expect(component.model).toEqual(["a", "b", "c"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["a", "b", "c"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["a", "b", "c"]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("should do nothing when model is undefined", () => {
    const reorder = setComponentState();
    component.field = {
      ...component.field,
      model: undefined,
    } as any;

    component.onDrop(createDropEvent(0, 2));

    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["a", "b", "c"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["a", "b", "c"]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("should reorder model, fieldGroup, formControl, and call reorder callback", () => {
    const reorder = setComponentState();

    component.onDrop(createDropEvent(0, 2));

    expect(component.model).toEqual(["b", "c", "a"]);
    expect(component.field.fieldGroup?.map(field => field.key)).toEqual(["b", "c", "a"]);
    expect((component.formControl as FormArray).controls.map(control => control.value)).toEqual(["b", "c", "a"]);
    expect(reorder).toHaveBeenCalledOnce();
  });
});
