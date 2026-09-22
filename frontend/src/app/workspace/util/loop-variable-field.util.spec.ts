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

import { AbstractControl, FormControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import {
  applyLoopVariableField,
  coerceOrReference,
  collectReferences,
  isReference,
  LOOP_VARIABLE_INPUT_TYPE,
  loopVariableOptions,
  primitiveSchemaType,
  referenceName,
  referenceValidator,
  valueAtPointer,
} from "./loop-variable-field.util";

describe("isReference / referenceName", () => {
  it("accepts a dollar sign followed by an identifier, as the whole value", () => {
    expect(isReference("$K")).toBe(true);
    expect(isReference("$_k1")).toBe(true);
    expect(referenceName("$K")).toBe("K");
  });

  it("rejects anything else", () => {
    expect(isReference("$")).toBe(false);
    expect(isReference("$1")).toBe(false);
    expect(isReference("K")).toBe(false);
    expect(isReference("$K ")).toBe(false);
    expect(isReference(" $K")).toBe(false);
    expect(isReference("$K.x")).toBe(false);
    expect(isReference("$K-1")).toBe(false);
    expect(isReference(5)).toBe(false);
    expect(isReference(null)).toBe(false);
    expect(isReference(undefined)).toBe(false);
    expect(referenceName("K")).toBeUndefined();
  });
});

describe("coerceOrReference", () => {
  it("keeps a reference as the literal string on every primitive type", () => {
    expect(coerceOrReference("$K", "integer")).toBe("$K");
    expect(coerceOrReference("$K", "number")).toBe("$K");
    expect(coerceOrReference("$K", "boolean")).toBe("$K");
    expect(coerceOrReference("$K", "string")).toBe("$K");
  });

  it("stores a number for numeric text on an integer field, and keeps other text for the validator", () => {
    expect(coerceOrReference("5", "integer")).toBe(5);
    expect(coerceOrReference(" 5 ", "integer")).toBe(5);
    expect(coerceOrReference("-3", "integer")).toBe(-3);
    expect(coerceOrReference("+3", "integer")).toBe(3);
    expect(coerceOrReference("5.5", "integer")).toBe("5.5");
    expect(coerceOrReference("abc", "integer")).toBe("abc");
  });

  it("stores a number for numeric text on a number field", () => {
    expect(coerceOrReference("5.5", "number")).toBe(5.5);
    expect(coerceOrReference("1e3", "number")).toBe(1000);
    expect(coerceOrReference("-0.25", "number")).toBe(-0.25);
    expect(coerceOrReference(".5", "number")).toBe(0.5);
    expect(coerceOrReference("1.0", "number")).toBe(1);
    expect(coerceOrReference("abc", "number")).toBe("abc");
    expect(coerceOrReference("Infinity", "number")).toBe("Infinity");
    expect(coerceOrReference("1e400", "number")).toBe("1e400");
  });

  it("keeps a numeral still being typed as text, so the next key is not lost", () => {
    // Number("7.") is 7: coercing it would redraw the box as "7" and typing 7.5 would store 75
    for (const partial of ["7.", "-", "+", "-0.", ".", "1e", "1e-", "1E+"]) {
      expect(coerceOrReference(partial, "number"), partial).toBe(partial);
    }
    expect(coerceOrReference("-", "integer")).toBe("-");
  });

  it("coerces only decimal numerals", () => {
    expect(coerceOrReference("0x10", "number")).toBe("0x10");
    expect(coerceOrReference("0b1", "number")).toBe("0b1");
    expect(coerceOrReference("1_000", "number")).toBe("1_000");
  });

  it("stores a boolean for true/false text, case-insensitively", () => {
    expect(coerceOrReference("true", "boolean")).toBe(true);
    expect(coerceOrReference("FALSE", "boolean")).toBe(false);
    expect(coerceOrReference(" True ", "boolean")).toBe(true);
    expect(coerceOrReference("yes", "boolean")).toBe("yes");
  });

  it("treats empty text as unset on a non-string field and as the empty string on a string field", () => {
    expect(coerceOrReference("", "integer")).toBeUndefined();
    expect(coerceOrReference("   ", "number")).toBeUndefined();
    expect(coerceOrReference("", "boolean")).toBeUndefined();
    expect(coerceOrReference("", "string")).toBe("");
  });

  it("leaves a string field's text untouched", () => {
    expect(coerceOrReference(" hello ", "string")).toBe(" hello ");
    expect(coerceOrReference("5", "string")).toBe("5");
  });

  it("passes an already-typed value through", () => {
    expect(coerceOrReference(7, "integer")).toBe(7);
    expect(coerceOrReference(true, "boolean")).toBe(true);
    expect(coerceOrReference(null, "integer")).toBeNull();
    expect(coerceOrReference(undefined, "number")).toBeUndefined();
  });
});

describe("referenceValidator", () => {
  const validate = referenceValidator(["K", "prev"]);

  it("accepts a reference to a declared variable", () => {
    expect(validate("$K")).toBeUndefined();
    expect(validate("$prev")).toBeUndefined();
  });

  it("names the undeclared variable in the message", () => {
    expect(validate("$foo")).toBe("$foo is not a variable of an enclosing block");
  });

  it("does not judge values that are not references", () => {
    expect(validate(5)).toBeUndefined();
    expect(validate("foo")).toBeUndefined();
    expect(validate("")).toBeUndefined();
    expect(validate(undefined)).toBeUndefined();
  });

  it("rejects every reference when no variable is declared", () => {
    expect(referenceValidator([])("$K")).toBe("$K is not a variable of an enclosing block");
  });
});

describe("primitiveSchemaType", () => {
  it("returns the four primitive types", () => {
    expect(primitiveSchemaType("string")).toBe("string");
    expect(primitiveSchemaType("integer")).toBe("integer");
    expect(primitiveSchemaType("number")).toBe("number");
    expect(primitiveSchemaType("boolean")).toBe("boolean");
  });

  it("reads the primitive out of a nullable type list", () => {
    expect(primitiveSchemaType(["integer", "null"])).toBe("integer");
    expect(primitiveSchemaType(["null", "number"])).toBe("number");
  });

  it("returns undefined for containers, ambiguous lists and missing types", () => {
    expect(primitiveSchemaType("object")).toBeUndefined();
    expect(primitiveSchemaType("array")).toBeUndefined();
    expect(primitiveSchemaType("null")).toBeUndefined();
    expect(primitiveSchemaType(["string", "integer"])).toBeUndefined();
    expect(primitiveSchemaType(undefined)).toBeUndefined();
  });
});

describe("loopVariableOptions", () => {
  it("prefixes each declared name with a dollar sign", () => {
    expect(loopVariableOptions(["K", "prev"])).toEqual(["$K", "$prev"]);
    expect(loopVariableOptions([])).toEqual([]);
  });
});

describe("collectReferences / valueAtPointer", () => {
  const properties = {
    limit: "$K",
    name: "plain",
    count: 3,
    nested: { list: ["$x", 3, { deep: "$y" }], "a/b": "$z" },
  };

  it("finds every reference with its JSON pointer, in document order", () => {
    expect(collectReferences(properties)).toEqual([
      { pointer: "/limit", name: "K" },
      { pointer: "/nested/list/0", name: "x" },
      { pointer: "/nested/list/2/deep", name: "y" },
      { pointer: "/nested/a~1b", name: "z" },
    ]);
  });

  it("finds nothing in properties without references", () => {
    expect(collectReferences({ a: 1, b: "text", c: [true] })).toEqual([]);
    expect(collectReferences({})).toEqual([]);
    expect(collectReferences(undefined)).toEqual([]);
  });

  it("resolves the pointers it produced, including escaped keys", () => {
    expect(valueAtPointer(properties, "/limit")).toBe("$K");
    expect(valueAtPointer(properties, "/nested/list/2/deep")).toBe("$y");
    expect(valueAtPointer(properties, "/nested/a~1b")).toBe("$z");
    expect(valueAtPointer(properties, "")).toBe(properties);
    expect(valueAtPointer(properties, "/missing/path")).toBeUndefined();
  });
});

describe("applyLoopVariableField", () => {
  /** The `type` validator formly's JSON-schema mapper attaches to an integer field. */
  const integerTypeValidator = () => ({
    schemaType: ["integer"],
    expression: ({ value }: AbstractControl) => value === undefined || Number.isInteger(value),
  });
  const control = (value: unknown) => ({ value }) as AbstractControl;

  it("renders the field as the loop-variable input with one option per declared variable", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer", props: { label: "limit" } };
    applyLoopVariableField(field, "integer", ["K", "prev"]);
    expect(field.type).toBe(LOOP_VARIABLE_INPUT_TYPE);
    expect(field.props?.["loopVariableOptions"]).toEqual(["$K", "$prev"]);
    expect(field.props?.label).toBe("limit");
  });

  it("parses typed text into the field's primitive or keeps a reference", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer" };
    applyLoopVariableField(field, "integer", ["K"]);
    const parse = field.parsers?.[0];
    expect(parse).toBeDefined();
    expect(parse!("5")).toBe(5);
    expect(parse!("$K")).toBe("$K");
    expect(parse!("")).toBeUndefined();
  });

  it("stores the parsed value in the control without writing it back into the box", () => {
    // formly sets the control to what a parser returns and, left to itself, redraws the box with it:
    // "1.0" would then read "1" and the next key would make it "15"
    const field: FormlyFieldConfig = { key: "fraction", type: "number" };
    applyLoopVariableField(field, "number", ["K"]);
    const formControl = new FormControl<unknown>("1.0");
    const setValue = vi.spyOn(formControl, "setValue");
    const parse = field.parsers![0] as (value: unknown, fieldConfig: FormlyFieldConfig) => unknown;

    expect(parse("1.0", { ...field, formControl })).toBe(1);
    expect(formControl.value).toBe(1);
    expect(setValue).toHaveBeenCalledWith(1, { emitModelToViewChange: false });

    // a value the control already holds is not set again
    setValue.mockClear();
    expect(parse(1, { ...field, formControl })).toBe(1);
    expect(setValue).not.toHaveBeenCalled();
  });

  it("keeps the mapper's own parsers on a string field, whose text needs no coercion", () => {
    const original = (v: unknown) => v;
    const field: FormlyFieldConfig = { key: "name", type: "string", parsers: [original] };
    applyLoopVariableField(field, "string", ["K"]);
    expect(field.parsers).toEqual([original]);
  });

  it("gives a string field the empty-string default its plain control has", () => {
    // the "string" formly type contributes defaultValue "", which the retyped field would lose
    const field: FormlyFieldConfig = { key: "name", type: "string" };
    applyLoopVariableField(field, "string", ["K"]);
    expect(field.defaultValue).toBe("");

    const withDefault: FormlyFieldConfig = { key: "name", type: "string", defaultValue: "abc" };
    applyLoopVariableField(withDefault, "string", ["K"]);
    expect(withDefault.defaultValue).toBe("abc");

    const integer: FormlyFieldConfig = { key: "limit", type: "integer" };
    applyLoopVariableField(integer, "integer", ["K"]);
    expect(integer.defaultValue).toBeUndefined();
  });

  it("lets a reference through the schema type check but still rejects other wrong text", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer", validators: { type: integerTypeValidator() } };
    applyLoopVariableField(field, "integer", ["K"]);
    const type = field.validators?.type;
    expect(type.expression(control("$K"), field)).toBe(true);
    expect(type.expression(control(3), field)).toBe(true);
    expect(type.expression(control("abc"), field)).toBe(false);
    expect(type.schemaType).toEqual(["integer"]);
    expect(type.message).toBe("should be an integer or a $variable of an enclosing block");
  });

  it("flags a reference to a variable no enclosing block declares", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer", validators: { type: integerTypeValidator() } };
    applyLoopVariableField(field, "integer", ["K"]);
    const reference = field.validators?.loopVariableReference;
    expect(reference.expression(control("$K"), field)).toBe(true);
    expect(reference.expression(control(3), field)).toBe(true);
    expect(reference.expression(control("$foo"), field)).toBe(false);
    expect(reference.message(undefined, { ...field, formControl: control("$foo") })).toBe(
      "$foo is not a variable of an enclosing block"
    );
    expect(field.validation?.show).toBe(true);
  });

  it("copes with a field that carries no validators at all", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer" };
    applyLoopVariableField(field, "integer", []);
    expect(field.validators?.type.expression(control("$K"), field)).toBe(true);
    expect(field.validators?.loopVariableReference.expression(control("$K"), field)).toBe(false);
  });

  it("checks a boolean field itself, since formly's schema type check lets any value through there", () => {
    // formly's JSON-schema `type` validator has no boolean case and returns true for any value
    const anyValue = { schemaType: ["boolean"], expression: () => true };
    for (const field of [
      { key: "flag", type: "boolean" } as FormlyFieldConfig,
      { key: "flag", type: "boolean", validators: { type: anyValue } } as FormlyFieldConfig,
    ]) {
      applyLoopVariableField(field, "boolean", ["K"]);
      const type = field.validators?.type;
      expect(type.expression(control("maybe"), field)).toBe(false);
      expect(type.expression(control(true), field)).toBe(true);
      expect(type.expression(control(false), field)).toBe(true);
      expect(type.expression(control("$K"), field)).toBe(true);
      expect(type.expression(control(undefined), field)).toBe(true);
      expect(type.message).toBe("should be true or false or a $variable of an enclosing block");
    }
  });
});
