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

import { describe, expect, it } from "vitest";
import type { Validation, ValidationError } from "./workflow-validation";
import * as ValidationWorkflowService from "./workflow-validation";

describe("ValidationWorkflowService.combineValidation", () => {
  const invalid = (messages: Record<string, string>): Validation => ({ isValid: false, messages });

  it("should be valid when given no validations at all", () => {
    const combined = ValidationWorkflowService.combineValidation();

    expect(combined.isValid).toBe(true);
    // The valid branch returns { isValid } only, so consumers reading `messages`
    // off a valid result get undefined rather than an empty object.
    expect((combined as ValidationError).messages).toBeUndefined();
  });

  it("should be valid, with no messages, when every validation is valid", () => {
    const combined = ValidationWorkflowService.combineValidation({ isValid: true }, { isValid: true });

    expect(combined).toEqual({ isValid: true });
  });

  it("should be invalid and carry the messages when a single validation is invalid", () => {
    const combined = ValidationWorkflowService.combineValidation(
      { isValid: true },
      invalid({ jsonSchema: "property 'x' is required" })
    );

    expect(combined).toEqual({ isValid: false, messages: { jsonSchema: "property 'x' is required" } });
  });

  it("should stay invalid regardless of where the invalid validation sits", () => {
    const failure = invalid({ connection: "operator has no input" });

    expect(ValidationWorkflowService.combineValidation(failure, { isValid: true }).isValid).toBe(false);
    expect(ValidationWorkflowService.combineValidation({ isValid: true }, failure).isValid).toBe(false);
  });

  it("should merge the messages of several invalid validations", () => {
    const combined = ValidationWorkflowService.combineValidation(
      invalid({ jsonSchema: "property 'x' is required" }),
      { isValid: true },
      invalid({ connection: "operator has no input" })
    );

    expect(combined).toEqual({
      isValid: false,
      messages: {
        jsonSchema: "property 'x' is required",
        connection: "operator has no input",
      },
    });
  });

  it("should let a later message win when two invalid validations share a key", () => {
    const combined = ValidationWorkflowService.combineValidation(
      invalid({ connection: "first" }),
      invalid({ connection: "second" })
    );

    expect(combined).toEqual({ isValid: false, messages: { connection: "second" } });
  });

  it("should ignore messages attached to a validation that reports itself valid", () => {
    // The Validation union gives the valid arm no `messages`, but the merge is
    // guarded on isValid rather than on the key being absent, so a stray field
    // is dropped instead of leaking into the combined result.
    const validWithStrayMessages = { isValid: true, messages: { ignored: "not a real error" } } as Validation;

    const combined = ValidationWorkflowService.combineValidation(
      validWithStrayMessages,
      invalid({ connection: "operator has no input" })
    );

    expect(combined).toEqual({ isValid: false, messages: { connection: "operator has no input" } });
  });

  it("should report invalid with an empty message map when the failing validation has none", () => {
    const combined = ValidationWorkflowService.combineValidation(invalid({}));

    expect(combined).toEqual({ isValid: false, messages: {} });
  });

  it("should not mutate the validations it was given", () => {
    const first = invalid({ jsonSchema: "property 'x' is required" });
    const second = invalid({ connection: "operator has no input" });

    ValidationWorkflowService.combineValidation(first, second);

    expect(first).toEqual({ isValid: false, messages: { jsonSchema: "property 'x' is required" } });
    expect(second).toEqual({ isValid: false, messages: { connection: "operator has no input" } });
  });
});
