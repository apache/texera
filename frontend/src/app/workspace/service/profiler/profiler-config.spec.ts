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

import {
  parseProfilerConfig,
  profilerConfigEquals,
  serializeProfilerConfig,
} from "./profiler-config";

describe("parseProfilerConfig", () => {
  it("returns undefined for null / non-object inputs", () => {
    expect(parseProfilerConfig(undefined)).toBeUndefined();
    expect(parseProfilerConfig(null)).toBeUndefined();
    expect(parseProfilerConfig("hello")).toBeUndefined();
    expect(parseProfilerConfig(42)).toBeUndefined();
  });

  it("returns undefined for empty object (no recognized fields)", () => {
    expect(parseProfilerConfig({})).toBeUndefined();
    expect(parseProfilerConfig({ unrelated: "stuff" })).toBeUndefined();
  });

  it("parses a fully-valid config faithfully", () => {
    expect(parseProfilerConfig({ enabled: true, view: "throughput", hotThresholdPercentile: 90 })).toEqual({
      enabled: true,
      view: "throughput",
      hotThresholdPercentile: 90,
    });
  });

  it("falls back to defaults for missing fields when at least one is present", () => {
    const result = parseProfilerConfig({ enabled: true });
    expect(result).toEqual({ enabled: true, view: "runtime", hotThresholdPercentile: 80 });
  });

  it("falls back to default view when the persisted value is not a known view", () => {
    const result = parseProfilerConfig({
      enabled: true,
      view: "garbage",
      hotThresholdPercentile: 75,
    });
    expect(result?.view).toBe("runtime");
    // other fields preserved
    expect(result?.enabled).toBe(true);
    expect(result?.hotThresholdPercentile).toBe(75);
  });

  it("clamps out-of-range percentile to [0, 100]", () => {
    expect(parseProfilerConfig({ hotThresholdPercentile: 9999 })?.hotThresholdPercentile).toBe(100);
    expect(parseProfilerConfig({ hotThresholdPercentile: -50 })?.hotThresholdPercentile).toBe(0);
  });

  it("ignores non-number percentile (NaN, Infinity, string)", () => {
    expect(parseProfilerConfig({ hotThresholdPercentile: Number.NaN })?.hotThresholdPercentile).toBe(80);
    expect(parseProfilerConfig({ hotThresholdPercentile: Infinity })?.hotThresholdPercentile).toBe(80);
    expect(parseProfilerConfig({ hotThresholdPercentile: "90" as any })?.hotThresholdPercentile).toBe(80);
  });

  it("coerces non-boolean enabled to false", () => {
    expect(parseProfilerConfig({ enabled: "true" as any })?.enabled).toBe(false);
    expect(parseProfilerConfig({ enabled: 1 as any })?.enabled).toBe(false);
  });
});

describe("serializeProfilerConfig", () => {
  it("extracts just the persistable fields", () => {
    const result = serializeProfilerConfig({
      enabled: true,
      view: "io-imbalance",
      hotThresholdPercentile: 95,
    });
    expect(result).toEqual({
      enabled: true,
      view: "io-imbalance",
      hotThresholdPercentile: 95,
    });
  });

  it("round-trips through parse → serialize without loss", () => {
    const original = serializeProfilerConfig({
      enabled: true,
      view: "throughput",
      hotThresholdPercentile: 50,
    });
    expect(parseProfilerConfig(original)).toEqual(original);
  });
});

describe("profilerConfigEquals", () => {
  it("returns true for identical configs", () => {
    const a = { enabled: true, view: "runtime" as const, hotThresholdPercentile: 80 };
    const b = { enabled: true, view: "runtime" as const, hotThresholdPercentile: 80 };
    expect(profilerConfigEquals(a, b)).toBe(true);
  });

  it("returns false when any field differs", () => {
    const a = { enabled: true, view: "runtime" as const, hotThresholdPercentile: 80 };
    expect(
      profilerConfigEquals(a, { ...a, enabled: false })
    ).toBe(false);
    expect(
      profilerConfigEquals(a, { ...a, view: "throughput" })
    ).toBe(false);
    expect(
      profilerConfigEquals(a, { ...a, hotThresholdPercentile: 90 })
    ).toBe(false);
  });

  it("handles undefineds without throwing", () => {
    const a = { enabled: true, view: "runtime" as const, hotThresholdPercentile: 80 };
    expect(profilerConfigEquals(undefined, undefined)).toBe(true);
    expect(profilerConfigEquals(undefined, a)).toBe(false);
    expect(profilerConfigEquals(a, undefined)).toBe(false);
  });
});
