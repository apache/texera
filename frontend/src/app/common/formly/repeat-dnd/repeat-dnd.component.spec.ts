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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormArray, FormControl } from "@angular/forms";
import { FormlyRepeatDndComponent } from "./repeat-dnd.component";

describe("FormlyRepeatDndComponent", () => {
  let component: FormlyRepeatDndComponent;
  let fixture: ComponentFixture<FormlyRepeatDndComponent>;
  let reorder: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [FormlyRepeatDndComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(FormlyRepeatDndComponent);
    component = fixture.componentInstance;
    reorder = vi.fn();
  });

  const setupOnDropState = (options?: { model: string[] | undefined }) => {
    const model = options && "model" in options ? options.model : ["a", "b", "c"];
    const fieldGroup = [{ id: "a" }, { id: "b" }, { id: "c" }];
    const formControl = new FormArray([
      new FormControl("a"),
      new FormControl("b"),
      new FormControl("c"),
    ]);

    Object.defineProperty(component, "model", { value: model, writable: true, configurable: true });
    Object.defineProperty(component, "field", {
      value: { fieldGroup, props: { reorder } },
      writable: true,
      configurable: true,
    });
    Object.defineProperty(component, "formControl", { value: formControl, writable: true, configurable: true });

    return { model, fieldGroup, formControl };
  };

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it("does nothing when previousIndex equals currentIndex", () => {
    const { model, fieldGroup, formControl } = setupOnDropState();

    component.onDrop({ previousIndex: 1, currentIndex: 1 } as any);

    expect(model).toEqual(["a", "b", "c"]);
    expect(fieldGroup).toEqual([{ id: "a" }, { id: "b" }, { id: "c" }]);
    expect(formControl.controls.map(control => control.value)).toEqual(["a", "b", "c"]);
    expect(reorder).not.toHaveBeenCalled();
  });

  it("does nothing when model is undefined", () => {
    setupOnDropState({ model: undefined });

    expect(() => component.onDrop({ previousIndex: 0, currentIndex: 2 } as any)).not.toThrow();
    expect(reorder).not.toHaveBeenCalled();
  });

  it("reorders model, fieldGroup, and form controls on drop", () => {
    const { model, fieldGroup, formControl } = setupOnDropState();

    component.onDrop({ previousIndex: 0, currentIndex: 2 } as any);

    expect(model).toEqual(["b", "c", "a"]);
    expect(fieldGroup).toEqual([{ id: "b" }, { id: "c" }, { id: "a" }]);
    expect(formControl.controls.map(control => control.value)).toEqual(["b", "c", "a"]);
    expect(reorder).toHaveBeenCalledTimes(1);
  });
});
