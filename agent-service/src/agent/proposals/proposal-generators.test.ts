/*
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
  FilterPredicatesResponseSchema,
  WorkerCountResponseSchema,
  proposeFilterPredicate,
  proposeWorkerCount,
  type FilterPredicatesResponse,
  type WorkerCountResponse,
} from "./proposal-generators";

/**
 * Minimal hand-rolled LanguageModelV2 stub — avoids importing `ai/test`,
 * which transitively requires `msw` (a dev-only HTTP mock not installed
 * in this project). `generateObject` only ever calls `doGenerate` for
 * non-streaming output, so we only need to implement that.
 */
type DoGenerate = (options: any) => Promise<{
  finishReason: string;
  usage: { inputTokens: number; outputTokens: number; totalTokens: number };
  content: { type: string; text: string }[];
  warnings: any[];
}>;

class StubModel {
  readonly specificationVersion = "v2";
  readonly provider = "stub-provider";
  readonly modelId = "stub-model";
  readonly supportedUrls = {};
  readonly doGenerateCalls: any[] = [];
  constructor(private readonly _doGenerate: DoGenerate) {}
  doGenerate = async (options: any) => {
    this.doGenerateCalls.push(options);
    return this._doGenerate(options);
  };
  doStream = async () => {
    throw new Error("doStream not implemented in StubModel");
  };
}

function mockJsonModel(payload: unknown) {
  const text = JSON.stringify(payload);
  return new StubModel(async () => ({
    finishReason: "stop",
    usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
    content: [{ type: "text", text }],
    warnings: [],
  }));
}

function mockThrowModel() {
  return new StubModel(async () => {
    throw new Error("boom");
  });
}

function captureUserPrompt(model: StubModel): string {
  const call = model.doGenerateCalls[0];
  const userMsg = call?.prompt?.find?.((m: any) => m.role === "user");
  if (!userMsg) return "";
  return Array.isArray(userMsg.content)
    ? userMsg.content.map((p: any) => p.text ?? "").join("\n")
    : String(userMsg.content ?? "");
}

describe("proposeFilterPredicate", () => {
  test("returns the parsed schema-shaped response on a well-formed model output", async () => {
    const payload: FilterPredicatesResponse = {
      predicates: [
        { attribute: "country", condition: "=", value: "US" },
        { attribute: "popularity", condition: ">", value: "0.5" },
      ],
      reasoning:
        "Aggregate downstream groups by country; filtering popularity > 0.5 is a sensible volume reduction.",
    };
    const model = mockJsonModel(payload);
    const out = await proposeFilterPredicate(model as any, {
      upstreamOpId: "csv-scan-1",
      downstreamOpId: "agg-1",
      upstreamSchema: [
        { attributeName: "country", attributeType: "string" },
        { attributeName: "popularity", attributeType: "double" },
      ],
      downstreamType: "Aggregate",
      downstreamProperties: { groupByKeys: ["country"] },
    });
    expect(out).toEqual(payload);
  });

  test("the schema accepts the 'is not null' fallback the prompt allows", () => {
    const parsed = FilterPredicatesResponseSchema.safeParse({
      predicates: [{ attribute: "id", condition: "is not null", value: "" }],
      reasoning: "Could not pick a useful predicate; falling back to is-not-null on a primary-id-like column.",
    });
    expect(parsed.success).toBe(true);
  });

  test("the schema rejects an empty predicates array", () => {
    const parsed = FilterPredicatesResponseSchema.safeParse({
      predicates: [],
      reasoning: "no idea",
    });
    expect(parsed.success).toBe(false);
  });

  test("the schema rejects more than 5 predicates", () => {
    const five: FilterPredicatesResponse = {
      predicates: Array.from({ length: 6 }, (_, i) => ({
        attribute: `col${i}`,
        condition: "is not null" as const,
        value: "",
      })),
      reasoning: "too many",
    };
    const parsed = FilterPredicatesResponseSchema.safeParse(five);
    expect(parsed.success).toBe(false);
  });

  test("the schema rejects an unknown condition value", () => {
    const parsed = FilterPredicatesResponseSchema.safeParse({
      predicates: [{ attribute: "x", condition: "starts with", value: "foo" }],
      reasoning: "r",
    });
    expect(parsed.success).toBe(false);
  });

  test("propagates errors from the model (caller treats any error as a 'miss')", async () => {
    await expect(
      proposeFilterPredicate(mockThrowModel() as any, {
        upstreamOpId: "u",
        downstreamOpId: "d",
        upstreamSchema: [{ attributeName: "a", attributeType: "string" }],
      })
    ).rejects.toThrow();
  });

  test("includes schema lines + downstream context + sample-row block in the prompt", async () => {
    const model = mockJsonModel({
      predicates: [{ attribute: "a", condition: "is not null", value: "" }],
      reasoning: "r",
    });

    await proposeFilterPredicate(model as any, {
      upstreamOpId: "csv-1",
      downstreamOpId: "agg-1",
      upstreamSchema: [
        { attributeName: "country", attributeType: "string" },
        { attributeName: "year", attributeType: "int" },
      ],
      downstreamType: "Aggregate",
      downstreamProperties: { groupByKeys: ["country"] },
      upstreamSamples: [{ country: "US", year: 2020 }],
    });

    const prompt = captureUserPrompt(model);
    expect(prompt).toContain("csv-1");
    expect(prompt).toContain("country (string)");
    expect(prompt).toContain("year (int)");
    expect(prompt).toContain("Aggregate");
    expect(prompt).toContain('"groupByKeys":["country"]');
    expect(prompt).toContain('"country":"US"');
  });
});

describe("proposeWorkerCount", () => {
  test("returns the parsed worker count on a well-formed model output", async () => {
    const payload: WorkerCountResponse = {
      workers: 6,
      reasoning: "Long runtime + low idle ratio on a Python UDF.",
    };
    const model = mockJsonModel(payload);
    const out = await proposeWorkerCount(model as any, {
      operatorId: "python-udf-1",
      operatorType: "PythonUDFV2",
      currentWorkers: 1,
      runtimeMs: 25_000,
      idleRatio: 0.1,
      inputRows: 5_000_000,
    });
    expect(out).toEqual(payload);
  });

  test("the schema enforces an integer in [1, 64]", () => {
    expect(WorkerCountResponseSchema.safeParse({ workers: 0, reasoning: "x" }).success).toBe(false);
    expect(WorkerCountResponseSchema.safeParse({ workers: 65, reasoning: "x" }).success).toBe(false);
    expect(WorkerCountResponseSchema.safeParse({ workers: 4.5, reasoning: "x" }).success).toBe(false);
    expect(WorkerCountResponseSchema.safeParse({ workers: 4, reasoning: "x" }).success).toBe(true);
  });

  test("the schema rejects missing reasoning", () => {
    expect(WorkerCountResponseSchema.safeParse({ workers: 4 }).success).toBe(false);
  });

  test("includes operator type + runtime + idle ratio in the prompt", async () => {
    const model = mockJsonModel({ workers: 4, reasoning: "r" });
    await proposeWorkerCount(model as any, {
      operatorId: "udf-1",
      operatorType: "PythonUDFV2",
      currentWorkers: 1,
      runtimeMs: 12_000,
      idleRatio: 0.05,
    });
    const prompt = captureUserPrompt(model);
    expect(prompt).toContain("udf-1");
    expect(prompt).toContain("PythonUDFV2");
    expect(prompt).toContain("12000 ms");
    expect(prompt).toContain("Idle ratio: 0.05");
  });

  test("handles missing optional metrics with 'unknown' placeholders in the prompt", async () => {
    const model = mockJsonModel({ workers: 4, reasoning: "r" });
    await proposeWorkerCount(model as any, {
      operatorId: "x",
      operatorType: "Sort",
      currentWorkers: 1,
    });
    const prompt = captureUserPrompt(model);
    expect(prompt).toContain("Runtime: unknown");
    expect(prompt).toContain("Idle ratio: unknown");
    expect(prompt).toContain("Input rows: unknown");
  });
});
