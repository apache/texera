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

// The formatters ignore their first (error) argument and read only field.props.
const err = {};
const field = (props: Record<string, unknown>): FormlyFieldConfig => ({ props }) as FormlyFieldConfig;

describe("formly-config validation-message formatters", () => {
  it("minItemsValidationMessage reports the minimum item count", () => {
    expect(minItemsValidationMessage(err, field({ minItems: 3 }))).toBe("should NOT have fewer than 3 items");
  });

  it("maxItemsValidationMessage reports the maximum item count", () => {
    expect(maxItemsValidationMessage(err, field({ maxItems: 5 }))).toBe("should NOT have more than 5 items");
  });

  it("minlengthValidationMessage reports the minimum length", () => {
    expect(minlengthValidationMessage(err, field({ minLength: 2 }))).toBe("should NOT be shorter than 2 characters");
  });

  it("maxlengthValidationMessage reports the maximum length", () => {
    expect(maxlengthValidationMessage(err, field({ maxLength: 10 }))).toBe("should NOT be longer than 10 characters");
  });

  it("minValidationMessage reports the inclusive minimum", () => {
    expect(minValidationMessage(err, field({ min: 0 }))).toBe("should be >= 0");
  });

  it("maxValidationMessage reports the inclusive maximum", () => {
    expect(maxValidationMessage(err, field({ max: 100 }))).toBe("should be <= 100");
  });

  it("multipleOfValidationMessage reports the step", () => {
    expect(multipleOfValidationMessage(err, field({ step: 4 }))).toBe("should be multiple of 4");
  });

  it("exclusiveMinimumValidationMessage reports the exclusive minimum", () => {
    expect(exclusiveMinimumValidationMessage(err, field({ exclusiveMinimum: 1 }))).toBe("should be > 1");
  });

  it("exclusiveMaximumValidationMessage reports the exclusive maximum", () => {
    expect(exclusiveMaximumValidationMessage(err, field({ exclusiveMaximum: 9 }))).toBe("should be < 9");
  });

  it("constValidationMessage reports the required constant", () => {
    expect(constValidationMessage(err, field({ const: "foo" }))).toBe('should be equal to constant "foo"');
  });

  it("interpolates undefined when the referenced prop is absent (optional-chaining branch)", () => {
    expect(minItemsValidationMessage(err, {} as FormlyFieldConfig)).toBe("should NOT have fewer than undefined items");
  });
});
