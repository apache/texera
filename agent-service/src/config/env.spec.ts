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
import { EnvSchema } from "./env";

// env.ts parses process.env once at import, so these tests go through the
// exported schema with an explicit environment instead.
function logPretty(value: string | undefined): boolean {
  return EnvSchema.parse(value === undefined ? {} : { LOG_PRETTY: value }).LOG_PRETTY;
}

describe("LOG_PRETTY", () => {
  test("is off when unset", () => {
    expect(logPretty(undefined)).toBe(false);
  });

  test("is off when empty", () => {
    expect(logPretty("")).toBe(false);
  });

  test.each(["false", "0", "FALSE", " False "])("is off for %p", value => {
    expect(logPretty(value)).toBe(false);
  });

  test.each(["true", "1", "TRUE", " True "])("is on for %p", value => {
    expect(logPretty(value)).toBe(true);
  });

  test.each(["yes", "off", "2", "truthy"])("rejects %p instead of guessing", value => {
    expect(() => logPretty(value)).toThrow();
  });
});

describe("EnvSchema defaults", () => {
  test("an empty environment still parses", () => {
    const parsed = EnvSchema.parse({});
    expect(parsed.PORT).toBe(3001);
    expect(parsed.LOG_PRETTY).toBe(false);
    expect(parsed.TEXERA_SERVICE_LOG_LEVEL).toBe("info");
  });
});
