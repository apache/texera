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

import { OperatorSchema } from "../types/operator-schema.interface";
import { WorkflowCompilingService } from "../service/compile-workflow/workflow-compiling.service";
import { propertyModel } from "../util/custom-formly-type";
import { FormControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { testNestedMacroSchema } from "../../common/testing/macro-graph";
import { mockScanSourceSchema } from "../service/operator-metadata/mock-operator-metadata.data";
import { CustomJSONSchema7 } from "../types/custom-json-schema.interface";

const nativeMacroWidgets: [string, string, CustomJSONSchema7, string, string | string[]][] = [
  ["ArrowFileScan", "fileName", { type: "string" }, "inputautocomplete", "data.arrow"],
  ["Projection", "attributes", { type: "array", items: { type: "string" } }, "repeat-section-dnd", ["value"]],
  ["ArrowFileScan", "path", { type: "string" }, "string", "plain text"],
];

function nestedFieldSchema(operatorType: string, jsonSchema: CustomJSONSchema7, values: object) {
  return testNestedMacroSchema({ ...mockScanSourceSchema, operatorType, jsonSchema }, values);
}

function conditionalMacroFields() {
  return nestedFieldSchema(
    "Configured",
    {
      type: "object",
      properties: {
        enabled: { type: "boolean", toggleHidden: ["detail"] },
        column: { type: "string" },
        detail: {
          type: "string",
          dependOn: "column",
          valueRules: {
            allOf: [{ if: { column: { valEnum: ["value"] } }, then: { type: "integer", minimum: 1 } }],
          },
        },
        other: { type: "string", hideTarget: "column", hideType: "equals", hideExpectedValue: "eventTime" },
      },
      allOf: [{ if: { properties: { column: { const: "eventTime" } } }, then: { required: ["detail"] } }],
    },
    { enabled: false, column: "eventTime", detail: "seed", other: "seed" }
  );
}

const conditionalMacroValues = () => ({
  "field-2-0": false,
  "field-2-1": "eventTime",
  "field-2-2": "seed",
  "field-2-3": "seed",
});

function evaluateField(field: FormlyFieldConfig, expression: string, model: object): unknown {
  const evaluate = field.expressions![expression] as (field: FormlyFieldConfig) => unknown;
  return evaluate({ ...field, parent: { model } });
}

/** The same native rules must survive rendering in the property panel and a rekeyed Form View card. */
function expectConditionalMacroField(field: FormlyFieldConfig, model: object): void {
  const evaluate = (name: string, values = model) => evaluateField(field, name, values);
  const edited = { ...model, "field-2-0": true, "field-2-1": "value" };
  expect(evaluate("hide")).toBe(true);
  expect(evaluate("templateOptions.description")).toBe("Input a datetime string");
  expect(evaluate("props.required")).toBe(true);
  expect(evaluate("hide", edited)).toBe(false);
  expect(evaluate("templateOptions.description", edited)).toBe("Input a positive number");
  expect(evaluate("props.required", edited)).toBe(false);
  const validate = field.validators!["valueRules"].expression;
  expect(validate(new FormControl("0"), { ...field, parent: { model: edited } })).toBe(false);
  expect(validate(new FormControl("2"), { ...field, parent: { model: edited } })).toBe(true);
}

/** Exercise the native property editor and Form View through their respective schema binding paths. */
export function nestedMacroFieldTests(
  formKey: string | undefined,
  render: (
    schema: OperatorSchema,
    properties: Record<string, unknown>,
    key: string
  ) => {
    field: FormlyFieldConfig;
    model: Record<string, unknown>;
  },
  compiler: () => Pick<WorkflowCompilingService, "getOperatorInputSchemaMap">,
  checkReorder?: (field: FormlyFieldConfig) => void
) {
  it.each(nativeMacroWidgets)(
    "retains the native %s.%s widget through two exposures",
    (type, key, property, widget, value) => {
      const schema = nestedFieldSchema(type, { type: "object", properties: { [key]: property } }, { [key]: value });
      const snapshot = JSON.stringify(schema);
      const publicKey = "field-2-0";
      const { field, model } = render(schema, { [publicKey]: value }, publicKey);
      const renderedKey = formKey ?? publicKey;
      expect(field.key).toBe(renderedKey);
      expect(field.type).toBe(widget);
      if (widget === "repeat-section-dnd") checkReorder?.(field);
      const edited = { ...model, [renderedKey]: "current value" };
      expect(propertyModel({ ...field, parent: { model: edited } } as FormlyFieldConfig)).toMatchObject({
        [key]: "current value",
      });
      expect(JSON.stringify(schema)).toBe(snapshot);
    }
  );

  it("evaluates nested toggle, dependency and validation rules on the exposed field", () => {
    vi.spyOn(compiler(), "getOperatorInputSchemaMap").mockReturnValue({
      "0_false": [{ attributeName: "eventTime", attributeType: "timestamp" }],
    });
    const key = "field-2-2";
    const { field, model } = render(conditionalMacroFields(), conditionalMacroValues(), key);
    expect(field.key).toBe(formKey ?? key);
    expectConditionalMacroField(field, model);
  });

  it("re-evaluates the native hideTarget when its exposed dependency changes", () => {
    const { field, model } = render(conditionalMacroFields(), conditionalMacroValues(), "field-2-3");
    expect(evaluateField(field, "hide", model)).toBe(true);
    expect(evaluateField(field, "hide", { ...model, "field-2-1": "value" })).toBe(false);
  });
}
