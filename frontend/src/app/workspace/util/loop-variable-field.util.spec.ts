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
import { cloneDeep } from "lodash-es";
import {
  applyLoopVariableField,
  coerceOrReference,
  collectReferences,
  inheritNoLoopVariable,
  isReference,
  LOOP_VARIABLE_INPUT_TYPE,
  loopVariableOptions,
  primitiveSchemaType,
  referenceName,
  unknownReferenceWarning,
  valueAtPointer,
} from "./loop-variable-field.util";
import { setValueRules } from "../../common/formly/formly-utils";
import { CustomJSONSchema7, ValueRuleSet } from "../types/custom-json-schema.interface";

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

describe("unknownReferenceWarning", () => {
  const warn = unknownReferenceWarning(["K", "prev"]);

  it("has nothing to say about a reference to a declared variable", () => {
    expect(warn("$K")).toBeUndefined();
    expect(warn("$prev")).toBeUndefined();
  });

  it("names the undeclared variable and what it means for the run", () => {
    expect(warn("$foo")).toBe(
      "$foo is not a variable of an enclosing block; the run will fail if no Loop Start sets it"
    );
  });

  it("does not judge values that are not references", () => {
    expect(warn(5)).toBeUndefined();
    expect(warn("foo")).toBeUndefined();
    expect(warn("")).toBeUndefined();
    expect(warn(undefined)).toBeUndefined();
    // a malformed reference is the type check's to flag
    expect(warn("$1st")).toBeUndefined();
  });

  it("warns about every reference when no variable is declared", () => {
    expect(unknownReferenceWarning([])("$K")).toBe(
      "$K is not a variable of an enclosing block; the run will fail if no Loop Start sets it"
    );
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

describe("inheritNoLoopVariable", () => {
  /** The schema a `$ref` in `schema` names, as formly's mapper looks it up from the root. */
  const resolve = (schema: CustomJSONSchema7, ref: unknown): any => {
    expect(typeof ref === "string" && ref.startsWith("#/"), String(ref)).toBe(true);
    return valueAtPointer(schema, (ref as string).slice(1));
  };

  it("marks every schema nested under a marked property, and nothing beside it", () => {
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: {
        isDrop: { type: "boolean", noLoopVariable: true },
        columns: {
          type: "array",
          noLoopVariable: true,
          items: { type: "object", properties: { name: { type: "string" }, size: { type: ["integer", "null"] } } },
        },
        limit: { type: "integer" },
      },
    };
    const marked: any = inheritNoLoopVariable(schema);
    expect(marked.properties.isDrop.noLoopVariable).toBe(true);
    expect(marked.properties.columns.noLoopVariable).toBe(true);
    expect(marked.properties.columns.items.noLoopVariable).toBe(true);
    expect(marked.properties.columns.items.properties.name).toEqual({ type: "string", noLoopVariable: true });
    expect(marked.properties.columns.items.properties.size.noLoopVariable).toBe(true);
    expect(marked.properties.limit).toEqual({ type: "integer" });
    expect(marked.noLoopVariable).toBeUndefined();
  });

  it("follows a $ref to a marked copy of what it names, leaving the one an unmarked property shares alone", () => {
    // Projection's `attributes` against a sibling that takes the same rows and may hold a reference
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: {
        attributes: { type: "array", noLoopVariable: true, items: { $ref: "#/definitions/AttributeUnit" } },
        renames: { type: "array", items: { $ref: "#/definitions/AttributeUnit" } },
      },
      definitions: {
        AttributeUnit: {
          type: "object",
          properties: { originalAttribute: { type: "string" }, alias: { type: "string" } },
        },
      },
    };
    const marked: any = inheritNoLoopVariable(schema);

    const markedUnit = resolve(marked, marked.properties.attributes.items.$ref);
    expect(markedUnit.noLoopVariable).toBe(true);
    expect(markedUnit.properties.alias).toEqual({ type: "string", noLoopVariable: true });
    expect(markedUnit.properties.originalAttribute.noLoopVariable).toBe(true);

    expect(marked.properties.renames.items.$ref).toBe("#/definitions/AttributeUnit");
    const sharedUnit = resolve(marked, marked.properties.renames.items.$ref);
    expect(sharedUnit.noLoopVariable).toBeUndefined();
    expect(sharedUnit.properties.alias).toEqual({ type: "string" });
  });

  it("marks what a marked property's own $ref names, since formly keeps only the named schema's keywords", () => {
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: { domain: { $ref: "#/definitions/Domain", title: "domain", noLoopVariable: true } },
      definitions: { Domain: { type: "object", properties: { min: { type: "integer" }, max: { type: "integer" } } } },
    };
    const marked: any = inheritNoLoopVariable(schema);
    expect(marked.properties.domain.title).toBe("domain");
    const domain = resolve(marked, marked.properties.domain.$ref);
    expect(domain.noLoopVariable).toBe(true);
    expect(domain.properties.min.noLoopVariable).toBe(true);
    expect(domain.properties.max.noLoopVariable).toBe(true);
  });

  it("reaches the alternatives of a nullable or merged schema", () => {
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: {
        bound: {
          noLoopVariable: true,
          oneOf: [{ type: "null" }, { type: "object", properties: { low: { type: "number" } } }],
          allOf: [{ properties: { high: { type: "number" } } }],
        },
      },
    };
    const marked: any = inheritNoLoopVariable(schema);
    expect(marked.properties.bound.oneOf[1].properties.low.noLoopVariable).toBe(true);
    expect(marked.properties.bound.allOf[0].properties.high.noLoopVariable).toBe(true);
  });

  it("reaches a schema under every keyword whose value is a schema, a list of them or a map of them", () => {
    const leaf: CustomJSONSchema7 = { type: "string" };
    const markedLeaf = { type: "string", noLoopVariable: true };
    /** What `value` becomes under `keyword` of a marked setting. */
    const underMarked = (keyword: string, value: unknown): unknown => {
      const marked: any = inheritNoLoopVariable({
        type: "object",
        properties: { setting: { noLoopVariable: true, [keyword]: value } as CustomJSONSchema7 },
      });
      return marked.properties.setting[keyword];
    };

    const schemaKeywords = ["items", "additionalItems", "additionalProperties", "contains", "propertyNames", "not"];
    for (const keyword of [...schemaKeywords, "if", "then", "else"]) {
      expect(underMarked(keyword, leaf), keyword).toEqual(markedLeaf);
    }
    for (const keyword of ["items", "allOf", "anyOf", "oneOf"]) {
      expect(underMarked(keyword, [leaf, leaf]), keyword).toEqual([markedLeaf, markedLeaf]);
    }
    for (const keyword of ["properties", "patternProperties", "dependencies", "definitions"]) {
      expect(underMarked(keyword, { a: leaf }), keyword).toEqual({ a: markedLeaf });
    }
  });

  it("ends at a definition that names itself, pointing the copy at itself", () => {
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: { tree: { $ref: "#/definitions/Node", noLoopVariable: true } },
      definitions: {
        Node: {
          type: "object",
          properties: { label: { type: "string" }, children: { type: "array", items: { $ref: "#/definitions/Node" } } },
        },
      },
    };
    const marked: any = inheritNoLoopVariable(schema);
    const node = resolve(marked, marked.properties.tree.$ref);
    expect(node.properties.label.noLoopVariable).toBe(true);
    expect(node.properties.children.items.$ref).toBe(marked.properties.tree.$ref);
    // the original definition stays as it was
    expect(marked.definitions.Node.properties.label).toEqual({ type: "string" });
  });

  it("leaves the values a marked schema holds, rather than is made of, as they were", () => {
    const valueRules: ValueRuleSet = {
      allOf: [{ if: { parameter: { valEnum: ["C"] } }, then: { type: "number", exclusiveMinimum: 0 } }],
    };
    const schema: CustomJSONSchema7 = {
      type: "object",
      properties: {
        options: {
          type: "object",
          noLoopVariable: true,
          default: { mode: { type: "x" } },
          examples: [{ mode: "fast" }],
          properties: { mode: { type: "string", enum: ["fast", "slow"], valueRules } },
        },
      },
    };
    const marked: any = inheritNoLoopVariable(schema);
    expect(marked.properties.options.default).toEqual({ mode: { type: "x" } });
    expect(marked.properties.options.examples).toEqual([{ mode: "fast" }]);
    expect(marked.properties.options.properties.mode).toEqual({
      type: "string",
      enum: ["fast", "slow"],
      valueRules,
      noLoopVariable: true,
    });
  });

  it("changes nothing in a schema without the keyword, and never the schema it is given", () => {
    const plain: CustomJSONSchema7 = {
      type: "object",
      properties: { limit: { type: "integer" }, rows: { type: "array", items: { $ref: "#/definitions/Row" } } },
      definitions: { Row: { type: "object", properties: { name: { type: "string" } } } },
    };
    expect(inheritNoLoopVariable(plain)).toEqual(plain);

    const withMark: CustomJSONSchema7 = {
      type: "object",
      properties: { rows: { type: "array", noLoopVariable: true, items: { $ref: "#/definitions/Row" } } },
      definitions: { Row: { type: "object", properties: { name: { type: "string" } } } },
    };
    const before = cloneDeep(withMark);
    inheritNoLoopVariable(withMark);
    expect(withMark).toEqual(before);
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

  it("writes the options and the warning into the field's own props, which templateOptions also names", () => {
    // the JSON-schema mapper hands over a field whose `props` and `templateOptions` are one object
    const props = { label: "limit" };
    const field: FormlyFieldConfig = { key: "limit", type: "integer", props, templateOptions: props };
    applyLoopVariableField(field, "integer", ["K"]);
    expect(field.props).toBe(props);
    expect(field.templateOptions).toBe(props);
    expect(props).toEqual({
      label: "limit",
      loopVariableOptions: ["$K"],
      loopVariableWarning: expect.any(Function),
    });
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

  it("warns about a reference to a variable no enclosing block declares, without making it an error", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer", validators: { type: integerTypeValidator() } };
    applyLoopVariableField(field, "integer", ["K"]);
    const warning = field.props?.["loopVariableWarning"];
    expect(warning("$K")).toBeUndefined();
    expect(warning(3)).toBeUndefined();
    expect(warning("$foo")).toBe(
      "$foo is not a variable of an enclosing block; the run will fail if no Loop Start sets it"
    );
    // every validator lets the reference through, so the setting stays valid
    expect(Object.keys(field.validators ?? {})).toEqual(["type"]);
    expect(field.validators?.type.expression(control("$foo"), field)).toBe(true);
    expect(field.validation?.show).toBe(true);
  });

  it("copes with a field that carries no validators at all", () => {
    const field: FormlyFieldConfig = { key: "limit", type: "integer" };
    applyLoopVariableField(field, "integer", []);
    expect(field.validators?.type.expression(control("$K"), field)).toBe(true);
    expect(field.props?.["loopVariableWarning"]("$K")).toBe(
      "$K is not a variable of an enclosing block; the run will fail if no Loop Start sets it"
    );
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

describe("applyLoopVariableField on a field with value rules", () => {
  // a sklearn trainer's hyperparameter row: the value's rules follow the `parameter` chosen beside it
  const rules: ValueRuleSet = {
    allOf: [
      { if: { parameter: { valEnum: ["C"] } }, then: { type: "number", exclusiveMinimum: 0, examples: ["1.0"] } },
      {
        if: { parameter: { valEnum: ["kernel"] } },
        then: { enum: ["rbf", "linear", "poly", "sigmoid", "precomputed"] },
      },
    ],
  };
  /** The `type` validator formly's JSON-schema mapper attaches to a string field. */
  const stringTypeValidator = () => ({
    schemaType: ["string"],
    expression: ({ value }: AbstractControl) => value === undefined || typeof value === "string",
  });
  const control = (value: unknown) => ({ value }) as AbstractControl;

  let field: FormlyFieldConfig;
  let onInit: NonNullable<FormlyFieldConfig["hooks"]>["onInit"];
  /** The field as formly hands it to a validator in a row whose parameter is the one given. */
  const inRow = (parameter: string): FormlyFieldConfig => ({ ...field, parent: { model: { parameter } } });

  beforeEach(() => {
    // the row's value field as the property panel maps it: the JSON-schema mapper's output, then the
    // rules' own control and validator, then the loop variables
    const props = { label: "value" };
    field = {
      key: "value",
      type: "string",
      props,
      templateOptions: props,
      validators: { type: stringTypeValidator() },
    };
    setValueRules(field, rules);
    onInit = field.hooks?.onInit;
    applyLoopVariableField(field, "string", ["K"]);
  });

  it("renders as the loop-variable input, keeping the rules on the one props object", () => {
    expect(field.type).toBe(LOOP_VARIABLE_INPUT_TYPE);
    expect(field.props?.["loopVariableOptions"]).toEqual(["$K"]);
    expect(field.props?.["valueRules"]).toBe(rules);
    // the rules' message reads them from `props`, and formly may reach the object by either name
    expect(field.templateOptions).toBe(field.props);
  });

  it("lets a declared reference through every validator", () => {
    for (const [name, validator] of Object.entries(field.validators ?? {})) {
      expect(validator.expression(control("$K"), inRow("C")), name).toBe(true);
      expect(validator.expression(control("$K"), inRow("kernel")), name).toBe(true);
    }
    expect(Object.keys(field.validators ?? {}).sort()).toEqual(["type", "valueRules"]);
  });

  it("still holds every other value to the rules the row's parameter selects, with the rules' message", () => {
    const valueRules = field.validators?.valueRules;
    expect(valueRules.expression(control("-1"), inRow("C"))).toBe(false);
    expect(valueRules.expression(control("abc"), inRow("C"))).toBe(false);
    expect(valueRules.expression(control("1.0"), inRow("C"))).toBe(true);
    expect(valueRules.expression(control("rbf"), inRow("kernel"))).toBe(true);
    expect(valueRules.expression(control("uniform"), inRow("kernel"))).toBe(false);
    expect(valueRules.expression(control(""), inRow("C"))).toBe(true);
    // text that only resembles a reference is not one, so the rules judge it
    expect(valueRules.expression(control(" $K"), inRow("C"))).toBe(false);
    expect(valueRules.message(null, inRow("C"))).toBe("must be a number greater than 0");
  });

  it("warns about a reference to a variable no enclosing block declares, which the rules leave alone", () => {
    expect(field.props?.["loopVariableWarning"]("$foo")).toBe(
      "$foo is not a variable of an enclosing block; the run will fail if no Loop Start sets it"
    );
    // a warning, not an error: the rules do not call "$foo" a bad number either
    expect(field.validators?.valueRules.expression(control("$foo"), inRow("C"))).toBe(true);
  });

  it("keeps the hook that re-judges the value when the parameter beside it changes", () => {
    expect(onInit).toBeDefined();
    expect(field.hooks?.onInit).toBe(onInit);
  });

  it("stores the text as typed, with no default that the rules' own control lacks", () => {
    // a string field keeps the mapper's parsers (none here), so "1.0" and "$K" stay the text typed
    expect(field.parsers).toBeUndefined();
    // the empty-string default belongs to the plain "string" control, which this field never had
    expect(field.defaultValue).toBeUndefined();
  });
});
