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
import { createShouldHideFieldFunc, getFieldByName, setHideExpression } from "./formly-utils";

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
