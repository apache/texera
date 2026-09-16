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
  constValidationMessage,
  exclusiveMaximumValidationMessage,
  exclusiveMinimumValidationMessage,
  maxItemsValidationMessage,
  maxlengthValidationMessage,
  maxValidationMessage,
  minItemsValidationMessage,
  minlengthValidationMessage,
  minValidationMessage,
  multipleOfValidationMessage,
} from "./formly-config";

// the `err` argument is unused by every message builder, so any value is fine
const err = {} as any;
const field = (props: Record<string, any>): FormlyFieldConfig => ({ props }) as FormlyFieldConfig;

describe("formly validation messages", () => {
  it("minItemsValidationMessage reports the minItems bound", () => {
    expect(minItemsValidationMessage(err, field({ minItems: 3 }))).toBe("should NOT have fewer than 3 items");
  });

  it("maxItemsValidationMessage reports the maxItems bound", () => {
    expect(maxItemsValidationMessage(err, field({ maxItems: 5 }))).toBe("should NOT have more than 5 items");
  });

  it("minlengthValidationMessage reports the minLength bound", () => {
    expect(minlengthValidationMessage(err, field({ minLength: 2 }))).toBe("should NOT be shorter than 2 characters");
  });

  it("maxlengthValidationMessage reports the maxLength bound", () => {
    expect(maxlengthValidationMessage(err, field({ maxLength: 8 }))).toBe("should NOT be longer than 8 characters");
  });

  it("minValidationMessage reports the min bound", () => {
    expect(minValidationMessage(err, field({ min: 0 }))).toBe("should be >= 0");
  });

  it("maxValidationMessage reports the max bound", () => {
    expect(maxValidationMessage(err, field({ max: 100 }))).toBe("should be <= 100");
  });

  it("multipleOfValidationMessage reports the step value", () => {
    expect(multipleOfValidationMessage(err, field({ step: 4 }))).toBe("should be multiple of 4");
  });

  it("exclusiveMinimumValidationMessage reports the exclusive minimum", () => {
    expect(exclusiveMinimumValidationMessage(err, field({ exclusiveMinimum: 1 }))).toBe("should be > 1");
  });

  it("exclusiveMaximumValidationMessage reports the exclusive maximum", () => {
    expect(exclusiveMaximumValidationMessage(err, field({ exclusiveMaximum: 9 }))).toBe("should be < 9");
  });

  it("constValidationMessage quotes the expected constant", () => {
    expect(constValidationMessage(err, field({ const: "hello" }))).toBe('should be equal to constant "hello"');
  });

  it("renders the literal 'undefined' when the relevant prop is missing", () => {
    expect(minValidationMessage(err, field({}))).toBe("should be >= undefined");
    expect(constValidationMessage(err, field({}))).toBe('should be equal to constant "undefined"');
  });
});
