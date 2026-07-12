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

import { describe, expect, test } from "bun:test";
import { formatValidationErrors, formatCompactSchemaForError } from "./workflow-system-metadata";

describe("formatValidationErrors", () => {
  test("returns an empty string for a valid result", () => {
    expect(formatValidationErrors({ isValid: true })).toBe("");
  });

  test("joins each message as 'key: msg' with '; '", () => {
    expect(formatValidationErrors({ isValid: false, messages: { a: "x", b: "y" } })).toBe("a: x; b: y");
  });
});

describe("formatCompactSchemaForError", () => {
  test("lists required keys and JSON-stringifies only the present required properties", () => {
    expect(formatCompactSchemaForError({ required: ["a", "b"], properties: { a: { type: "string" } } })).toBe(
      'required: [a, b], properties: {"a":{"type":"string"}}'
    );
  });

  test("renders empty required and properties", () => {
    expect(formatCompactSchemaForError({ required: [], properties: {} })).toBe("required: [], properties: {}");
  });
});
