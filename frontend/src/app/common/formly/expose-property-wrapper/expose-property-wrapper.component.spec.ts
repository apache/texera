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
    it("keeps form-field outermost and appends the wrapper when no wrappers are set", () => {
      const config: FormlyFieldConfig = { key: "k" };
      ExposePropertyWrapperComponent.decorate(config, true, () => {});
      expect(config.wrappers).toEqual(["form-field", "expose-property-wrapper"]);
    });

    it("appends after existing wrappers without dropping them", () => {
      const config: FormlyFieldConfig = { key: "k", wrappers: ["custom"] };
      ExposePropertyWrapperComponent.decorate(config, false, () => {});
      expect(config.wrappers).toEqual(["custom", "expose-property-wrapper"]);
    });

    it("carries the exposed state and toggle callback in props, preserving existing props", () => {
      const toggle = vi.fn();
      const config: FormlyFieldConfig = { key: "k", props: { label: "kept" } };
      ExposePropertyWrapperComponent.decorate(config, true, toggle);
      expect(config.props?.["label"]).toBe("kept");
      expect(config.props?.["exposed"]).toBe(true);
      expect(config.props?.["toggleExposed"]).toBe(toggle);
    });
  });

  describe("onToggle", () => {
    it("forwards the checkbox checked state to toggleExposed", () => {
      const toggle = vi.fn();
      component.field = { props: { exposed: false, toggleExposed: toggle } } as unknown as FormlyFieldConfig;
      component.onToggle({ target: { checked: true } } as unknown as Event);
      expect(toggle).toHaveBeenCalledWith(true);
    });
  });

  describe("template", () => {
    it("renders a checkbox reflecting props.exposed and drives onToggle on change", () => {
      const toggle = vi.fn();
      component.field = { props: { exposed: true, toggleExposed: toggle } } as unknown as FormlyFieldConfig;
      fixture.detectChanges();

      const box = fixture.nativeElement.querySelector("input[type=checkbox]") as HTMLInputElement;
      expect(box).toBeTruthy();
      expect(box.checked).toBe(true);

      box.checked = false;
      box.dispatchEvent(new Event("change"));
      expect(toggle).toHaveBeenCalledWith(false);
    });
  });
});
