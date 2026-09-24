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
import { setValueRules } from "../../../common/formly/formly-utils";
import { ValueRuleSet } from "../../types/custom-json-schema.interface";

/**
 * The shape the sklearn trainers' hyperparameter rows emit: the rules of the value follow the
 * `parameter` chosen beside it, a set of words for one parameter and a bounded number for another.
 */
const valueRules: ValueRuleSet = {
  allOf: [
    { if: { parameter: { valEnum: ["C"] } }, then: { type: "number", exclusiveMinimum: 0, examples: ["1.0"] } },
    {
      if: { parameter: { valEnum: ["kernel"] } },
      then: { enum: ["rbf", "linear", "poly", "sigmoid", "precomputed"] },
    },
  ],
};

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
      expect(component.suggestions).toEqual(["$K", "$prev"]);
      holding(undefined, ["$K", "$prev"]);
      expect(component.suggestions).toEqual(["$K", "$prev"]);
    });

    it("offers the variables matching a typed $ prefix, ignoring case", () => {
      holding("$p", ["$K", "$prev"]);
      expect(component.suggestions).toEqual(["$prev"]);
      holding("$P", ["$K", "$prev"]);
      expect(component.suggestions).toEqual(["$prev"]);
      holding("$", ["$K", "$prev"]);
      expect(component.suggestions).toEqual(["$K", "$prev"]);
    });

    it("offers nothing while a plain value is typed, or when the block declares no variable", () => {
      holding("5", ["$K", "$prev"]);
      expect(component.suggestions).toEqual([]);
      holding(5, ["$K"]);
      expect(component.suggestions).toEqual([]);
      holding("", undefined);
      expect(component.suggestions).toEqual([]);
    });

    describe("on a field with value rules", () => {
      /** Holds `value` in a row whose `parameter` is the one given, as a hyperparameter row does. */
      function holdingInRow(value: unknown, parameter: string, withRules = true): void {
        component.field = {
          formControl: new FormControl(value),
          props: withRules ? { loopVariableOptions: ["$K", "$prev"], valueRules } : { loopVariableOptions: ["$K"] },
          parent: { model: { parameter } },
        } as FieldTypeConfig;
      }

      it("offers the variables and then every value the chosen parameter accepts while the box is empty", () => {
        holdingInRow("", "kernel");
        expect(component.suggestions).toEqual(["$K", "$prev", "rbf", "linear", "poly", "sigmoid", "precomputed"]);
        holdingInRow(undefined, "kernel");
        expect(component.suggestions).toEqual(["$K", "$prev", "rbf", "linear", "poly", "sigmoid", "precomputed"]);
      });

      it("offers the accepted values matching a typed plain prefix, ignoring case", () => {
        holdingInRow("p", "kernel");
        expect(component.suggestions).toEqual(["poly", "precomputed"]);
        holdingInRow("PO", "kernel");
        expect(component.suggestions).toEqual(["poly"]);
        holdingInRow("rbf", "kernel");
        expect(component.suggestions).toEqual(["rbf"]);
        holdingInRow("x", "kernel");
        expect(component.suggestions).toEqual([]);
      });

      it("matches accepted values that carry capitals against a lower-case prefix", () => {
        component.field = {
          formControl: new FormControl("t"),
          props: {
            loopVariableOptions: ["$K"],
            valueRules: {
              allOf: [{ if: { parameter: { valEnum: ["fit_intercept"] } }, then: { enum: ["True", "False"] } }],
            },
          },
          parent: { model: { parameter: "fit_intercept" } },
        } as FieldTypeConfig;
        expect(component.suggestions).toEqual(["True"]);
      });

      it("offers only the matching variables once the text starts with $", () => {
        holdingInRow("$", "kernel");
        expect(component.suggestions).toEqual(["$K", "$prev"]);
        holdingInRow("$p", "kernel");
        expect(component.suggestions).toEqual(["$prev"]);
      });

      it("offers the variables alone where the chosen parameter's rules name no set of values", () => {
        // C takes any number above zero, and a row with no parameter chosen yet has no rules at all
        holdingInRow("", "C");
        expect(component.suggestions).toEqual(["$K", "$prev"]);
        holdingInRow("1", "C");
        expect(component.suggestions).toEqual([]);
        holdingInRow("", "");
        expect(component.suggestions).toEqual(["$K", "$prev"]);
      });

      it("follows the row when the parameter beside it changes", () => {
        holdingInRow("", "C");
        expect(component.suggestions).toEqual(["$K", "$prev"]);
        component.field.parent!.model.parameter = "kernel";
        expect(component.suggestions).toEqual(["$K", "$prev", "rbf", "linear", "poly", "sigmoid", "precomputed"]);
      });

      it("offers no values for a field without value rules, whatever its row holds", () => {
        holdingInRow("", "kernel", false);
        expect(component.suggestions).toEqual(["$K"]);
        holdingInRow("r", "kernel", false);
        expect(component.suggestions).toEqual([]);
      });
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
        // a hyperparameter row's pair, whose value the rules hold to what the parameter takes
        parameter: { type: "string", title: "parameter" },
        value: { type: "string", title: "value" },
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
      // the property panel's mapping: every primitive property of an operator inside a block, the value
      // rules first
      const root = TestBed.inject(FormlyJsonschema).toFieldConfig(schema, {
        map: (field: FormlyFieldConfig, source: JSONSchema7) => {
          if (field.key === "value") {
            setValueRules(field, valueRules);
          }
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

    it("stores a value-rules field's text as typed, judged by the rules the row's parameter selects", async () => {
      await typeInto("parameter", "C");
      await typeInto("value", "-1");
      expect(host.model["value"]).toBe("-1");
      expect(host.form.get("value")?.hasError("valueRules")).toBe(true);

      input("value").value = "";
      await typeInto("value", "1.0");
      expect(host.model["value"]).toBe("1.0");
      expect(input("value").value).toBe("1.0");
      expect(host.form.get("value")?.valid).toBe(true);

      // a reference the rules cannot judge: the loop variable decides the value at run time
      input("value").value = "";
      await typeInto("value", "$K");
      expect(host.model["value"]).toBe("$K");
      expect(host.form.get("value")?.valid).toBe(true);
    });

    it("re-judges a value-rules field when the parameter beside it changes", async () => {
      await typeInto("parameter", "C");
      await typeInto("value", "1.0");
      expect(host.form.get("value")?.valid).toBe(true);

      // 1.0 is a C, and no kernel at all
      input("parameter").value = "";
      await typeInto("parameter", "kernel");
      expect(host.form.get("value")?.hasError("valueRules")).toBe(true);
    });
  });
});
