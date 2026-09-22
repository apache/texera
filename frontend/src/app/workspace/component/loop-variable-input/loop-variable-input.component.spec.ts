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
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { FormControl, FormGroup, ReactiveFormsModule } from "@angular/forms";
import { By } from "@angular/platform-browser";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { FieldTypeConfig, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";
import { FormlyJsonschema } from "@ngx-formly/core/json-schema";
import { JSONSchema7 } from "json-schema";
import { LoopVariableInputComponent } from "./loop-variable-input.component";
import {
  applyLoopVariableField,
  LOOP_VARIABLE_INPUT_TYPE,
  primitiveSchemaType,
} from "../../util/loop-variable-field.util";

/** A formly form around the fields a test builds, as the property panel hosts them. */
@Component({
  template: `<form [formGroup]="form">
    <formly-form
      [form]="form"
      [fields]="fields"
      [model]="model"></formly-form>
  </form>`,
  imports: [ReactiveFormsModule, FormlyModule],
})
class FormHostComponent {
  form = new FormGroup({});
  model: Record<string, unknown> = {};
  fields: FormlyFieldConfig[] = [];
}

describe("LoopVariableInputComponent", () => {
  describe("the options it offers", () => {
    let component: LoopVariableInputComponent;

    beforeEach(async () => {
      await TestBed.configureTestingModule({
        imports: [LoopVariableInputComponent, NoopAnimationsModule],
      }).compileComponents();
      component = TestBed.createComponent(LoopVariableInputComponent).componentInstance;
    });

    function holding(value: unknown, loopVariableOptions?: string[]): void {
      component.field = {
        formControl: new FormControl(value),
        props: loopVariableOptions === undefined ? {} : { loopVariableOptions },
      } as FieldTypeConfig;
    }

    it("offers every variable while the box is empty", () => {
      holding("", ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual(["$K", "$prev"]);
      holding(undefined, ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual(["$K", "$prev"]);
    });

    it("offers the variables matching a typed $ prefix, ignoring case", () => {
      holding("$p", ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual(["$prev"]);
      holding("$P", ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual(["$prev"]);
      holding("$", ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual(["$K", "$prev"]);
    });

    it("offers nothing while a plain value is typed, or when the block declares no variable", () => {
      holding("5", ["$K", "$prev"]);
      expect(component.referenceOptions).toEqual([]);
      holding(5, ["$K"]);
      expect(component.referenceOptions).toEqual([]);
      holding("", undefined);
      expect(component.referenceOptions).toEqual([]);
    });
  });

  // Rendered through formly, as the property panel renders it, so that the parser formly runs on every
  // keystroke and the value it writes back into the box are the real ones.
  describe("rendered in a form", () => {
    const schema: JSONSchema7 = {
      type: "object",
      properties: {
        limit: { type: "integer", title: "limit" },
        fraction: { type: "number", title: "fraction" },
        prefix: { type: "string", title: "prefix" },
      },
    };

    let fixture: ComponentFixture<FormHostComponent>;
    let host: FormHostComponent;

    beforeEach(async () => {
      await TestBed.configureTestingModule({
        imports: [
          FormHostComponent,
          NoopAnimationsModule,
          FormlyModule.forRoot({ types: [{ name: LOOP_VARIABLE_INPUT_TYPE, component: LoopVariableInputComponent }] }),
        ],
      }).compileComponents();
      fixture = TestBed.createComponent(FormHostComponent);
      host = fixture.componentInstance;
      // the property panel's mapping: every primitive property of an operator inside a block
      const root = TestBed.inject(FormlyJsonschema).toFieldConfig(schema, {
        map: (field: FormlyFieldConfig, source: JSONSchema7) => {
          const schemaType = primitiveSchemaType(source.type);
          if (schemaType !== undefined) {
            applyLoopVariableField(field, schemaType, ["K"]);
            field.props = { ...field.props, placeholder: `the ${field.key}`, attributes: { style: "color: red" } };
          }
          return field;
        },
      });
      root.type = undefined; // render the properties as a plain group, without the object type
      host.fields = [root];
      fixture.detectChanges();
      await fixture.whenStable();
    });

    function input(key: string): HTMLInputElement {
      return fixture.debugElement.query(By.css(`input[placeholder="the ${key}"]`)).nativeElement;
    }

    /** Types the text one key at a time, letting the box redraw between keys as it does for a user. */
    async function typeInto(key: string, text: string): Promise<void> {
      const box = input(key);
      for (const char of text) {
        box.value += char;
        box.dispatchEvent(new Event("input"));
        fixture.detectChanges();
        await fixture.whenStable();
        await Promise.resolve(); // the autocomplete writes a new value into the box in a microtask
      }
    }

    it("renders one text box per field, carrying the field's attributes", () => {
      const box = input("limit");
      expect(box.getAttribute("nz-input")).not.toBeNull();
      expect(box.getAttribute("style")).toContain("color: red");
      expect(box.id).not.toBe("");
    });

    it("stores a decimal typed one key at a time and keeps the box as typed", async () => {
      await typeInto("fraction", "1.05");
      expect(host.model["fraction"]).toBe(1.05);
      expect(input("fraction").value).toBe("1.05");
    });

    it("keeps a leading minus and a leading zero while they are typed", async () => {
      await typeInto("fraction", "-0.5");
      expect(host.model["fraction"]).toBe(-0.5);
      expect(input("fraction").value).toBe("-0.5");
    });

    it("keeps a numeral still being typed as text, flagged until it is complete", async () => {
      await typeInto("fraction", "7.");
      expect(input("fraction").value).toBe("7.");
      expect(host.model["fraction"]).toBe("7.");
      expect(host.form.get("fraction")?.hasError("type")).toBe(true);

      await typeInto("fraction", "5");
      expect(host.model["fraction"]).toBe(7.5);
      expect(host.form.get("fraction")?.valid).toBe(true);
    });

    it("stores an integer and a reference as typed", async () => {
      await typeInto("limit", "12");
      expect(host.model["limit"]).toBe(12);
      expect(input("limit").value).toBe("12");

      input("limit").value = "";
      await typeInto("limit", "$K");
      expect(host.model["limit"]).toBe("$K");
      expect(input("limit").value).toBe("$K");
    });

    it("gives a string field its empty-string default", () => {
      expect(host.model["prefix"]).toBe("");
    });
  });
});
