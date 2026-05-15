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

/**
 * Pure (de)serialization for the per-workflow profiler config block that lives
 * inside WorkflowContent.profilerConfig.
 *
 * - parseProfilerConfig: defensive cast from `unknown` (workflow JSON loaded from
 *   the backend is untyped) → ProfilerConfig | undefined. Same validation as
 *   ProfilerService.restoreConfig: unknown view → fallback; out-of-range percentile
 *   → clamped; non-boolean enabled → fallback.
 * - serializeProfilerConfig: extract the minimal serializable shape from in-memory
 *   profiler state.
 */

import type { ProfilerView } from "./profiler.service";

export interface ProfilerConfig {
  readonly enabled: boolean;
  readonly view: ProfilerView;
  readonly hotThresholdPercentile: number;
}

const DEFAULT_VIEW: ProfilerView = "runtime";
const DEFAULT_HOT_THRESHOLD = 80;
const VALID_VIEWS: ReadonlySet<string> = new Set<string>(["runtime", "throughput", "io-imbalance"]);

/**
 * Defensive parse of an unknown blob (e.g. a field on a freshly-loaded workflow
 * whose schema was authored by an older client). Returns `undefined` when the
 * blob isn't a plausible profiler config — callers should treat that as
 * "workflow has no per-workflow override" and fall back to user defaults.
 */
export function parseProfilerConfig(raw: unknown): ProfilerConfig | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const obj = raw as Record<string, unknown>;

  // Require at least one recognized field to be present so we don't manufacture
  // a config from an empty object.
  const hasAnyField = "enabled" in obj || "view" in obj || "hotThresholdPercentile" in obj;
  if (!hasAnyField) return undefined;

  const enabled = typeof obj.enabled === "boolean" ? obj.enabled : false;

  let view: ProfilerView = DEFAULT_VIEW;
  if (typeof obj.view === "string" && VALID_VIEWS.has(obj.view)) {
    view = obj.view as ProfilerView;
  }

  let hotThresholdPercentile = DEFAULT_HOT_THRESHOLD;
  const rawPct = obj.hotThresholdPercentile;
  if (typeof rawPct === "number" && Number.isFinite(rawPct)) {
    hotThresholdPercentile = Math.max(0, Math.min(100, rawPct));
  }

  return { enabled, view, hotThresholdPercentile };
}

/**
 * Extracts the wire-shaped profiler config from a ProfilerService state-like input.
 * Pure: takes only the fields it needs, no service dependency.
 */
export function serializeProfilerConfig(input: {
  enabled: boolean;
  view: ProfilerView;
  hotThresholdPercentile: number;
}): ProfilerConfig {
  return {
    enabled: input.enabled,
    view: input.view,
    hotThresholdPercentile: input.hotThresholdPercentile,
  };
}

/**
 * Deep equality check on two configs. Used by the bridge code that syncs
 * ProfilerService ↔ WorkflowActionService to break write-loops cheaply.
 */
export function profilerConfigEquals(a: ProfilerConfig | undefined, b: ProfilerConfig | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return (
    a.enabled === b.enabled &&
    a.view === b.view &&
    a.hotThresholdPercentile === b.hotThresholdPercentile
  );
}
