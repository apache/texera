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
import { FormlyFieldConfig } from "@ngx-formly/core";
import { FormControl } from "@angular/forms";
import { ExposePropertyWrapperComponent } from "./expose-property-wrapper.component";

describe("ExposePropertyWrapperComponent", () => {
  let component: ExposePropertyWrapperComponent;
  let fixture: ComponentFixture<ExposePropertyWrapperComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ExposePropertyWrapperComponent],
    }).compileComponents();
    fixture = TestBed.createComponent(ExposePropertyWrapperComponent);
    component = fixture.componentInstance;
  });

  describe("decorate", () => {
    it.each([
      { wrappers: undefined, first: "form-field" },
      { wrappers: ["custom"], first: "custom" },
    ])("preserves $first as the outer wrapper", ({ wrappers, first }) => {
      const config: FormlyFieldConfig = { key: "k", wrappers };
      ExposePropertyWrapperComponent.decorate(config, () => true, vi.fn());
      expect(config.wrappers).toEqual([first, "expose-property-wrapper"]);
    });

    it("carries the exposed state and toggle callback in props, preserving existing props", () => {
      const toggle = vi.fn();
      let exposed = true;
      const config: FormlyFieldConfig = { key: "k", props: { label: "kept" } };
      ExposePropertyWrapperComponent.decorate(config, () => exposed, toggle);
      expect(config.props?.["label"]).toBe("kept");
      expect(config.props?.["isExposed"]()).toBe(true);
      exposed = false;
      expect(config.props?.["isExposed"]()).toBe(false);
      expect(config.props?.["toggleExposed"]).toBe(toggle);
    });
  });

  describe("template", () => {
    const checkbox = (): HTMLInputElement => fixture.nativeElement.querySelector("input[type=checkbox]");

    it.each([
      { label: "Text", expected: "Show Text on the Form View" },
      { label: undefined, expected: "Show this property on the Form View" },
    ])("uses the accessible label '$expected' and follows the form control", ({ label, expected }) => {
      const formControl = new FormControl({ value: "text", disabled: true });
      component.field = { formControl, props: { label, isExposed: () => false } };
      fixture.detectChanges();
      expect(checkbox().getAttribute("aria-label")).toBe(expected);
      expect(checkbox().disabled).toBe(true);
      formControl.enable();
      fixture.detectChanges();
      expect(checkbox().disabled).toBe(false);
    });

    it("reflects binding changes without remounting and drives onToggle on change", () => {
      const toggle = vi.fn();
      let exposed = true;
      component.field = { formControl: new FormControl(), props: { isExposed: () => exposed, toggleExposed: toggle } };
      fixture.detectChanges();

      const box = checkbox();
      expect(box).toBeTruthy();
      expect(box.checked).toBe(true);

      exposed = false;
      fixture.detectChanges();
      expect(box.checked).toBe(false);
      exposed = true;
      fixture.detectChanges();
      expect(box.checked).toBe(true);

      for (const checked of [false, true]) {
        box.checked = checked;
        box.dispatchEvent(new Event("change"));
        expect(toggle).toHaveBeenLastCalledWith(checked);
      }
    });
  });
});
