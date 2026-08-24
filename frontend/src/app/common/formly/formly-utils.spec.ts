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

import { FormlyFieldConfig } from "@ngx-formly/core";
import {
  createOutputFormChangeEventStream,
  createShouldHideFieldFunc,
  createValueRulesValidator,
  getFieldByName,
  matchingValueRule,
  setChildTypeDependency,
  setHideExpression,
  valueRulesValidationMessage,
} from "./formly-utils";
import { ValueRuleSet } from "../../workspace/types/custom-json-schema.interface";
import { Subject } from "rxjs";
import { FORM_DEBOUNCE_TIME_MS } from "../../workspace/service/execute-workflow/execute-workflow.service";
import { PortSchema } from "../../workspace/types/workflow-compiling.interface";

describe("getFieldByName", () => {
  it("returns the field whose key matches the name", () => {
    const a: FormlyFieldConfig = { key: "a" };
    const b: FormlyFieldConfig = { key: "b" };
    expect(getFieldByName("b", [a, b])).toBe(b);
  });

  it("returns undefined when no field matches", () => {
    expect(getFieldByName("z", [{ key: "a" }])).toBeUndefined();
  });

  it("returns the first match when several fields share a key", () => {
    const first: FormlyFieldConfig = { key: "dup" };
    const second: FormlyFieldConfig = { key: "dup" };
    expect(getFieldByName("dup", [first, second])).toBe(first);
  });
});

describe("setHideExpression", () => {
  it("sets the hide expression on each named field that exists", () => {
    const a: FormlyFieldConfig = { key: "a" };
    const b: FormlyFieldConfig = { key: "b" };
    setHideExpression(["a", "b"], [a, b], "toggle");
    expect(a.expressions).toEqual({ hide: "!field.parent.model.toggle" });
    expect(b.expressions).toEqual({ hide: "!field.parent.model.toggle" });
  });

  it("is a no-op for names that are not present", () => {
    const a: FormlyFieldConfig = { key: "a" };
    setHideExpression(["missing"], [a], "toggle");
    expect(a.expressions).toBeUndefined();
  });
});

describe("createShouldHideFieldFunc", () => {
  const fieldWithModel = (model: any): FormlyFieldConfig => ({ parent: { model } }) as FormlyFieldConfig;

  it("returns false when the parent model is missing", () => {
    const debugSpy = vi.spyOn(console, "debug").mockImplementation(() => {});
    try {
      const hide = createShouldHideFieldFunc("target", "equals", "x", false);
      expect(hide(undefined)).toBe(false);
      expect(hide({} as FormlyFieldConfig)).toBe(false);
    } finally {
      debugSpy.mockRestore();
    }
  });

  it("returns hideOnNull when the target value is null/undefined", () => {
    const hideOnNull = createShouldHideFieldFunc("target", "equals", "x", true);
    expect(hideOnNull(fieldWithModel({}))).toBe(true);

    const keepOnNull = createShouldHideFieldFunc("target", "equals", "x", false);
    expect(keepOnNull(fieldWithModel({}))).toBe(false);
  });

  it("hides in regex mode iff the value matches ^(expected)$", () => {
    const hide = createShouldHideFieldFunc("target", "regex", "ab|cd", false);
    expect(hide(fieldWithModel({ target: "ab" }))).toBe(true);
    expect(hide(fieldWithModel({ target: "cd" }))).toBe(true);
    expect(hide(fieldWithModel({ target: "abc" }))).toBe(false);
  });

  it("hides in equals mode iff value.toString() equals the expected value", () => {
    const hide = createShouldHideFieldFunc("target", "equals", "5", false);
    expect(hide(fieldWithModel({ target: 5 }))).toBe(true);
    expect(hide(fieldWithModel({ target: "5" }))).toBe(true);
    expect(hide(fieldWithModel({ target: 6 }))).toBe(false);
  });
});

describe("setChildTypeDependency", () => {
  it("collects timestamp attribute names across all ports and sets a description expression on the child", () => {
    const attributes: Record<string, PortSchema | undefined> = {
      outPort0: [
        { attributeName: "ts1", attributeType: "timestamp" },
        { attributeName: "label", attributeType: "string" },
      ],
      outPort1: [{ attributeName: "ts2", attributeType: "timestamp" }],
    };
    const child: FormlyFieldConfig = { key: "attrValue" };
    setChildTypeDependency(attributes, "attrName", [{ key: "other" }, child], "attrValue");

    expect(child.expressions).toEqual({
      "templateOptions.description":
        "[\"ts1\",\"ts2\"].includes(model.attrName)? 'Input a datetime string' : 'Input a positive number'",
    });
  });

  it("emits an empty timestamp list when there are no timestamp-typed attributes", () => {
    const attributes: Record<string, PortSchema | undefined> = {
      outPort0: [{ attributeName: "label", attributeType: "string" }],
    };
    const child: FormlyFieldConfig = { key: "attrValue" };
    setChildTypeDependency(attributes, "attrName", [child], "attrValue");

    expect(child.expressions).toEqual({
      "templateOptions.description":
        "[].includes(model.attrName)? 'Input a datetime string' : 'Input a positive number'",
    });
  });

  it("treats undefined attributes as an empty timestamp list", () => {
    const child: FormlyFieldConfig = { key: "attrValue" };
    setChildTypeDependency(undefined, "attrName", [child], "attrValue");

    expect(child.expressions).toEqual({
      "templateOptions.description":
        "[].includes(model.attrName)? 'Input a datetime string' : 'Input a positive number'",
    });
  });

  it("is a no-op when the named child field is not present", () => {
    const attributes: Record<string, PortSchema | undefined> = {
      outPort0: [{ attributeName: "ts1", attributeType: "timestamp" }],
    };
    const unrelated: FormlyFieldConfig = { key: "other" };
    setChildTypeDependency(attributes, "attrName", [unrelated], "missingChild");

    expect(unrelated.expressions).toBeUndefined();
  });
});

describe("createOutputFormChangeEventStream", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("debounces bursts, emitting only the latest value once the debounce window elapses", () => {
    const source = new Subject<Record<string, unknown>>();
    const emissions: Record<string, unknown>[] = [];
    createOutputFormChangeEventStream(source, () => true).subscribe(v => emissions.push(v));

    source.next({ v: 1 });
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS - 1);
    source.next({ v: 2 });
    // still within the debounce window relative to the second emission
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS - 1);
    expect(emissions).toEqual([]);

    vi.advanceTimersByTime(1);
    expect(emissions).toEqual([{ v: 2 }]);
  });

  it("drops a repeated identical value via distinctUntilChanged", () => {
    const source = new Subject<Record<string, unknown>>();
    const emissions: Record<string, unknown>[] = [];
    createOutputFormChangeEventStream(source, () => true).subscribe(v => emissions.push(v));

    const sameRef = { v: 1 };
    source.next(sameRef);
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS);
    source.next(sameRef);
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS);

    expect(emissions).toEqual([sameRef]);
  });

  it("gates emissions through the modelCheck predicate", () => {
    const source = new Subject<Record<string, unknown>>();
    const emissions: Record<string, unknown>[] = [];
    const modelCheck = vi.fn((formData: Record<string, unknown>) => formData["keep"] === true);
    createOutputFormChangeEventStream(source, modelCheck).subscribe(v => emissions.push(v));

    source.next({ keep: false });
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS);
    source.next({ keep: true });
    vi.advanceTimersByTime(FORM_DEBOUNCE_TIME_MS);

    expect(emissions).toEqual([{ keep: true }]);
    expect(modelCheck).toHaveBeenCalledTimes(2);
  });
});

describe("valueRules", () => {
  // the shape the sklearn trainers emit: one branch per hyperparameter, keyed on the
  // `parameter` chosen beside the value in the same row
  const rules: ValueRuleSet = {
    allOf: [
      { if: { parameter: { valEnum: ["C"] } }, then: { type: "number", examples: ["1.0"] } },
      { if: { parameter: { valEnum: ["degree"] } }, then: { type: "integer", examples: ["3"] } },
      // gamma takes either of two words or a number, which no type names
      {
        if: { parameter: { valEnum: ["gamma"] } },
        then: { pattern: "^\\s*(?:scale|auto|[-+]?[0-9]*\\.?[0-9]+)\\s*$", examples: ["scale"] },
      },
      // an accepted set carries no example: it already names every value worth offering, and
      // the estimator's own default leads
      {
        if: { parameter: { valEnum: ["kernel"] } },
        then: { enum: ["rbf", "linear", "poly", "sigmoid", "precomputed"] },
      },
    ],
  };

  const rowField = (row: unknown): FormlyFieldConfig => ({ parent: { model: row } }) as FormlyFieldConfig;
  const control = (value: unknown) => ({ value }) as any;
  const check = (parameter: string, value: unknown) =>
    createValueRulesValidator(rules)(control(value), rowField({ parameter }));

  describe("matchingValueRule", () => {
    it("selects the branch the sibling's value names", () => {
      expect(matchingValueRule(rules, { parameter: "kernel" })?.enum).toEqual([
        "rbf",
        "linear",
        "poly",
        "sigmoid",
        "precomputed",
      ]);
      expect(matchingValueRule(rules, { parameter: "degree" })?.type).toBe("integer");
    });

    it("selects nothing when the sibling holds a value no branch names", () => {
      expect(matchingValueRule(rules, { parameter: "metric_params" })).toBeUndefined();
    });

    it("selects nothing before the row has a sibling value at all", () => {
      expect(matchingValueRule(rules, {})).toBeUndefined();
      expect(matchingValueRule(rules, undefined)).toBeUndefined();
      expect(matchingValueRule(undefined, { parameter: "C" })).toBeUndefined();
    });
  });

  describe("createValueRulesValidator", () => {
    it("accepts a value the chosen parameter's set contains", () => {
      expect(check("kernel", "rbf")).toBe(true);
    });

    it("rejects a value outside that set, including one of another parameter's", () => {
      expect(check("kernel", "1")).toBe(false);
      expect(check("kernel", "uniform")).toBe(false);
    });

    it("holds a numeric parameter to a number", () => {
      expect(check("C", "1.0")).toBe(true);
      expect(check("C", "-2.5e3")).toBe(true);
      expect(check("C", "abc")).toBe(false);
    });

    it("holds a whole-number parameter to a whole number", () => {
      expect(check("degree", "3")).toBe(true);
      expect(check("degree", "-1")).toBe(true);
      // int() raises on this, so the form should not let it reach the operator
      expect(check("degree", "1.5")).toBe(false);
    });

    it("leaves emptiness to the required rule rather than answering twice", () => {
      expect(check("C", "")).toBe(true);
      expect(check("C", null)).toBe(true);
      expect(check("kernel", undefined)).toBe(true);
    });

    it("accepts anything for a parameter no branch constrains", () => {
      expect(check("metric_params", "whatever")).toBe(true);
    });

    it("holds a parameter with a pattern to the shape it declares", () => {
      // both halves of the union it describes
      expect(check("gamma", "scale")).toBe(true);
      expect(check("gamma", "auto")).toBe(true);
      expect(check("gamma", "0.1")).toBe(true);
      expect(check("gamma", " 1 ")).toBe(true);
      expect(check("gamma", "abc")).toBe(false);
      expect(check("gamma", "scaleauto")).toBe(false);
    });

    it("re-judges the same value when the row switches parameter", () => {
      // a value typed for one parameter is usually wrong for the next, and stays visible
      expect(check("C", "1.0")).toBe(true);
      expect(check("kernel", "1.0")).toBe(false);
    });
  });

  describe("valueRulesValidationMessage", () => {
    const field = (parameter: string): FormlyFieldConfig =>
      ({ props: { valueRules: rules }, parent: { model: { parameter } } }) as FormlyFieldConfig;

    it("names the accepted values when there is a set", () => {
      expect(valueRulesValidationMessage(null, field("kernel"))).toBe(
        "must be one of rbf, linear, poly, sigmoid, precomputed"
      );
    });

    it("distinguishes a whole number from a number", () => {
      expect(valueRulesValidationMessage(null, field("degree"))).toBe("must be a whole number");
      expect(valueRulesValidationMessage(null, field("C"))).toBe("must be a number");
    });

    it("points at a working value where a pattern is what the branch declares", () => {
      expect(valueRulesValidationMessage(null, field("gamma"))).toBe(
        "is not a value this parameter takes, such as scale"
      );
    });
  });
});
