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
import {
  createToolResult,
  createErrorResult,
  formatAddOperatorResult,
  formatModifyOperatorResult,
  formatExecuteOperatorResult,
  formatOperatorError,
} from "./tools-utility";

describe("createToolResult", () => {
  test("returns the message unchanged", () => {
    expect(createToolResult("ok")).toBe("ok");
  });

  test("preserves an empty string", () => {
    expect(createToolResult("")).toBe("");
  });
});

describe("createErrorResult", () => {
  test("wraps the error with the [ERROR] prefix", () => {
    expect(createErrorResult("boom")).toBe("[ERROR] boom");
  });

  test("keeps the prefix even for empty error text", () => {
    expect(createErrorResult("")).toBe("[ERROR] ");
  });
});

describe("formatExecuteOperatorResult", () => {
  test("renders the operator id in the standard executed message", () => {
    expect(formatExecuteOperatorResult("op-1")).toBe("Executed operator op-1");
  });
});

describe("formatOperatorError", () => {
  test("includes both operator id and error text", () => {
    expect(formatOperatorError("op-1", "bad input")).toBe("Error on operator op-1: bad input");
  });

  test("retains the trailing colon and space when error is empty", () => {
    expect(formatOperatorError("op-1", "")).toBe("Error on operator op-1: ");
  });
});

describe("formatAddOperatorResult", () => {
  test("emits only the summary when no links are provided", () => {
    expect(formatAddOperatorResult("op-1", 2, 1)).toBe("Added operator op-1, input ports: 2, output ports: 1");
  });

  test("appends created links after the summary when only createdLinks is provided", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Added operator op-1, input ports: 1, output ports: 1, created links: [u --> op-1]");
  });

  test("appends deleted links after the summary when only deletedLinks is provided", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, undefined, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Added operator op-1, input ports: 1, output ports: 1, deleted links: [u --> op-1]");
  });

  test("places deleted-links segment before created-links segment when both are provided", () => {
    const out = formatAddOperatorResult(
      "op-1",
      1,
      1,
      [{ source: "u", target: "op-1" }],
      [{ source: "old", target: "op-1" }]
    );
    expect(out).toBe(
      "Added operator op-1, input ports: 1, output ports: 1" +
        ", deleted links: [old --> op-1]" +
        ", created links: [u --> op-1]"
    );
    expect(out.indexOf("deleted links")).toBeLessThan(out.indexOf("created links"));
  });

  test("treats empty link arrays as absent (length-0 short-circuit)", () => {
    const out = formatAddOperatorResult("op-1", 0, 0, [], []);
    expect(out).toBe("Added operator op-1, input ports: 0, output ports: 0");
    expect(out).not.toContain("links:");
  });

  test("joins multiple link descriptions with a comma separator", () => {
    const out = formatAddOperatorResult("op-1", 1, 1, [
      { source: "a", target: "op-1" },
      { source: "b", target: "op-1" },
    ]);
    expect(out).toContain("created links: [a --> op-1, b --> op-1]");
  });
});

describe("formatModifyOperatorResult", () => {
  test("emits the bare modified summary when no links are provided", () => {
    expect(formatModifyOperatorResult("op-1")).toBe("Operator op-1 modified");
  });

  test("appends created links when only createdLinks is provided", () => {
    const out = formatModifyOperatorResult("op-1", [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Operator op-1 modified, created links: [u --> op-1]");
  });

  test("appends deleted links when only deletedLinks is provided", () => {
    const out = formatModifyOperatorResult("op-1", undefined, [{ source: "u", target: "op-1" }]);
    expect(out).toBe("Operator op-1 modified, deleted links: [u --> op-1]");
  });

  test("places deleted-links segment before created-links segment when both are provided", () => {
    const out = formatModifyOperatorResult(
      "op-1",
      [{ source: "new", target: "op-1" }],
      [{ source: "old", target: "op-1" }]
    );
    expect(out).toBe("Operator op-1 modified, deleted links: [old --> op-1], created links: [new --> op-1]");
    expect(out.indexOf("deleted links")).toBeLessThan(out.indexOf("created links"));
  });

  test("treats empty link arrays as absent", () => {
    const out = formatModifyOperatorResult("op-1", [], []);
    expect(out).toBe("Operator op-1 modified");
    expect(out).not.toContain("links:");
  });
});
