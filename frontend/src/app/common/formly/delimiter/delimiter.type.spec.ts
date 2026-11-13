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
import { FormControl } from "@angular/forms";
import { FieldTypeConfig } from "@ngx-formly/core";
import { CUSTOM_DELIMITER, DelimiterMode, delimiterPresets } from "./delimiter-presets";
import { DelimiterTypeComponent } from "./delimiter.type";

describe("DelimiterTypeComponent", () => {
  let component: DelimiterTypeComponent;
  let fixture: ComponentFixture<DelimiterTypeComponent>;

  function setField(initialValue: unknown, delimiterMode: DelimiterMode = "char"): FormControl {
    const formControl = new FormControl(initialValue);
    component.field = { formControl, props: { delimiterMode } } as unknown as FieldTypeConfig;
    return formControl;
  }

  const customBox = (): HTMLInputElement | null => fixture.nativeElement.querySelector("input.delimiter-custom");
  const shownSelection = (): string =>
    (fixture.nativeElement.querySelector(".ant-select-selection-item")?.textContent ?? "").trim();

  function typeInto(box: HTMLInputElement, text: string): void {
    box.value = text;
    box.dispatchEvent(new Event("input"));
    fixture.detectChanges();
  }

  beforeEach(async () => {
    await TestBed.configureTestingModule({ imports: [DelimiterTypeComponent] }).compileComponents();
    fixture = TestBed.createComponent(DelimiterTypeComponent);
    component = fixture.componentInstance;
  });

  describe.each(["char", "regex"] as DelimiterMode[])("in %s mode", mode => {
    it.each(delimiterPresets(mode).map(p => [p.label, p.value]))(
      "stores %s as its value when picked, with no text box",
      async (label, value) => {
        const formControl = setField(",", mode);
        fixture.detectChanges();

        component.onSelect(value);
        fixture.detectChanges();
        await fixture.whenStable();
        fixture.detectChanges();

        expect(formControl.value).toBe(value);
        expect(formControl.dirty).toBe(true);
        expect(formControl.touched).toBe(true);
        expect(component.selected).toBe(value);
        expect(shownSelection()).toBe(label);
        expect(customBox()).toBeNull();
      }
    );

    it("opens an empty text box when Custom is picked from a preset", () => {
      const formControl = setField(",", mode);

      component.onSelect(CUSTOM_DELIMITER);
      fixture.detectChanges();

      expect(formControl.value).toBe("");
      expect(component.selected).toBe(CUSTOM_DELIMITER);
      expect(customBox()).not.toBeNull();
    });

    it("stays on Custom while the typed value happens to equal a preset", () => {
      const formControl = setField(",", mode);
      component.onSelect(CUSTOM_DELIMITER);
      fixture.detectChanges();

      typeInto(customBox()!, ";");

      expect(formControl.value).toBe(";");
      expect(component.selected).toBe(CUSTOM_DELIMITER);
      expect(customBox()).not.toBeNull();
    });

    it("stays on Custom after the custom value is cleared", () => {
      const formControl = setField(",", mode);
      component.onSelect(CUSTOM_DELIMITER);

      formControl.setValue("");

      expect(component.selected).toBe(CUSTOM_DELIMITER);
    });

    it("closes the text box when a preset is picked after Custom", () => {
      const formControl = setField(",", mode);
      component.onSelect(CUSTOM_DELIMITER);
      fixture.detectChanges();

      component.onSelect(";");
      fixture.detectChanges();

      expect(formControl.value).toBe(";");
      expect(customBox()).toBeNull();
    });

    it("selects nothing while the property is unset", () => {
      setField(null, mode);
      expect(component.selected).toBeNull();
      setField("", mode);
      expect(component.selected).toBeNull();
    });

    it("disables the picker with the form, e.g. on a read-only workflow", () => {
      const formControl = setField(",", mode);
      formControl.disable();
      fixture.detectChanges();

      expect(fixture.nativeElement.querySelector("nz-select").classList).toContain("ant-select-disabled");
    });
  });

  it("shows a custom char delimiter with a single-character placeholder", () => {
    setField("#");
    fixture.detectChanges();

    expect(customBox()!.value).toBe("#");
    expect(customBox()!.placeholder).toBe("Any single character");
  });

  it("turns a typed \\t into a tab and shows it as the Tab preset", () => {
    const formControl = setField(",");
    component.onSelect(CUSTOM_DELIMITER);
    fixture.detectChanges();

    typeInto(customBox()!, "\\t");

    expect(formControl.value).toBe("\t");
    expect(component.selected).toBe("\t");
    expect(customBox()).toBeNull();
  });

  it("lets a backslash be typed on its way to an escape, rather than capping it at one character", () => {
    const formControl = setField(",");
    component.onSelect(CUSTOM_DELIMITER);
    fixture.detectChanges();

    typeInto(customBox()!, "\\");
    expect(formControl.value).toBe("\\");
    expect(customBox()!.hasAttribute("maxlength")).toBe(false);
  });

  it("follows the value back to its preset when it changes from outside, e.g. undo or a co-editor", () => {
    const formControl = setField(",");
    fixture.detectChanges();
    component.onSelect(CUSTOM_DELIMITER);
    fixture.detectChanges();
    typeInto(customBox()!, ";");

    formControl.setValue(",");
    fixture.detectChanges();
    formControl.setValue(";");
    fixture.detectChanges();

    expect(component.selected).toBe(";");
    expect(customBox()).toBeNull();
  });

  it("labels the picker and the text box for screen readers", () => {
    component.field = {
      formControl: new FormControl("#"),
      props: { delimiterMode: "char", label: "Delimiter" },
    } as unknown as FieldTypeConfig;
    fixture.detectChanges();

    expect(fixture.nativeElement.querySelector("nz-select").getAttribute("aria-label")).toBe("Delimiter");
    expect(customBox()!.getAttribute("aria-label")).toBe("Custom Delimiter");
  });

  it("lets a custom regex be any length, with an example in the placeholder", () => {
    setField("[,;]\\s*", "regex");
    fixture.detectChanges();

    expect(component.selected).toBe(CUSTOM_DELIMITER);
    expect(customBox()!.value).toBe("[,;]\\s*");
    expect(customBox()!.hasAttribute("maxlength")).toBe(false);
    expect(customBox()!.placeholder).toContain("Regular expression");
  });

  it("keeps a custom value when Custom is picked again", () => {
    const formControl = setField("#");

    component.onSelect(CUSTOM_DELIMITER);

    expect(formControl.value).toBe("#");
  });

  it("shows a stored tab as the Tab preset in both modes", () => {
    setField("\t", "char");
    expect(component.selected).toBe("\t");
    setField("\t", "regex");
    expect(component.selected).toBe("\\t");
  });

  it("opens an older workflow's over-long char delimiter on Custom so it can be fixed", () => {
    setField(",;");
    fixture.detectChanges();

    expect(component.selected).toBe(CUSTOM_DELIMITER);
    expect(customBox()!.value).toBe(",;");
  });

  it("falls back to char mode when the schema names no mode", () => {
    component.field = { formControl: new FormControl(","), props: {} } as unknown as FieldTypeConfig;
    expect(component.mode).toBe("char");
  });
});
