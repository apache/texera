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
 * Mirrors the Scala gateway DTOs in
 * amber/.../web/observability/gateway/dtos.scala.
 *
 * Kept as discriminated unions + readonly types so the compiler
 * catches drift between the wire format and the UI's assumptions.
 */

export type LogLevel = "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR";

/** Closed set — must match the LogLevel enum in dtos.scala. */
export const LOG_LEVELS: readonly LogLevel[] = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"] as const;

/** Server-enforced per-page maximum from the gateway. */
export const MAX_PAGE_SIZE = 1000;

/** Server-enforced max free-text query length. */
export const MAX_FREE_TEXT_LEN = 256;

/** Inbound request shape — matches RawLogsSearchRequest. */
export interface LogsSearchRequest {
  readonly workflowId?: number;
  readonly executionId?: number;
  readonly computingUnitId?: number;
  readonly userId?: number;
  /** Optional whitelist of service names — overrides the default
   *  texera-* stream prefix when present. Use the values returned
   *  by GET /observability/logs/sources. */
  readonly services?: ReadonlyArray<string>;
  readonly level?: LogLevel;
  readonly query?: string;
  readonly sort?: LogSort;
  readonly fromMs: number;
  readonly toMs: number;
  readonly pageSize: number;
  /** Opaque cursor returned in the previous page's response. */
  readonly pageCursor?: string;
}

/** Closed enum of sort orders — must match LogSort in dtos.scala. */
export type LogSort = "newest" | "oldest" | "severity" | "service";

export const LOG_SORTS: ReadonlyArray<{ value: LogSort; label: string }> = [
  { value: "newest", label: "Newest first" },
  { value: "oldest", label: "Oldest first" },
  { value: "severity", label: "Severity (high → low)" },
  { value: "service", label: "Service (A → Z)" },
] as const;

/** Distinct filter values currently in the logs store — backing for
 *  the panel's autofill dropdowns. */
export interface LogSourcesResponse {
  readonly services: ReadonlyArray<string>;
  readonly workflowIds: ReadonlyArray<number>;
  readonly computingUnitIds: ReadonlyArray<number>;
  readonly userIds: ReadonlyArray<number>;
  /** id -> display name for the user-id dropdown; ids without a name
   *  are absent and the UI falls back to the id. */
  readonly userNames?: Readonly<Record<number, string>>;
}

export interface LogEntry {
  readonly timestampMs: number;
  readonly level: string;
  readonly body: string;
  readonly traceId?: string;
  readonly spanId?: string;
  readonly attributes: Record<string, string>;
}

export interface LogsSearchResponse {
  readonly entries: ReadonlyArray<LogEntry>;
  readonly total: number;
  readonly nextCursor?: string;
}

export interface ObservabilityHealth {
  readonly status: "ok" | "degraded";
  readonly checks: {
    readonly logs: boolean;
    readonly metrics: boolean;
    readonly traces: boolean;
    readonly profiles: boolean;
  };
}

/** Stable error shape returned by the gateway. */
export interface GatewayErrorBody {
  readonly code: string;
  readonly message: string;
}

// ---- Metrics (PR 9) ---------------------------------------------------

/** Named server-side queries. Mirrors NamedMetric in dtos.scala —
 *  any string the UI sends that isn't in this enum is rejected
 *  client-side before HTTP dispatch (the gateway rejects it again
 *  on the server). */
export type NamedMetric =
  | "runsPerDay"
  | "totalRuns"
  | "activeWorkflows"
  | "successRate"
  | "failureRate"
  | "avgDuration"
  | "p50Duration"
  | "p95Duration"
  | "p99Duration";

export const NAMED_METRICS: readonly NamedMetric[] = [
  "runsPerDay",
  "totalRuns",
  "activeWorkflows",
  "successRate",
  "failureRate",
  "avgDuration",
  "p50Duration",
  "p95Duration",
  "p99Duration",
] as const;

export interface MetricsQueryRequest {
  readonly name: NamedMetric;
  readonly fromMs: number;
  readonly toMs: number;
  readonly stepSec?: number;
  // Optional filter for DB-backed counts (e.g. totalRuns): restrict to runs
  // launched by this user. Ignored by metrics-backend series.
  readonly userId?: number;
}

export interface MetricPoint {
  readonly timestampMs: number;
  readonly value: number;
}

export interface MetricsQueryResponse {
  readonly metric: string;
  readonly points: ReadonlyArray<MetricPoint>;
}

// ---- Traces (PR 10) ---------------------------------------------------

/** W3C trace-id format. Lowercase hex, exactly 32 chars. The
 *  service rejects anything else before HTTP dispatch and the
 *  gateway rejects it again on the server. */
export const TRACE_ID_RE = /^[0-9a-f]{32}$/;

export interface TraceSpan {
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly name: string;
  readonly startMs: number;
  readonly endMs: number;
  readonly attributes: Record<string, string>;
}

export interface TracesGetResponse {
  readonly traceId: string;
  readonly spans: ReadonlyArray<TraceSpan>;
}

// ---- Profiles (PR 11) -------------------------------------------------

export interface ProfilesQueryRequest {
  // Optional process filter (Parca `comm` label), e.g. "java" to focus on the
  // Texera JVMs. Per-workflow/execution filters were removed: the profiling
  // agent emits no such labels.
  readonly comm?: string;
  readonly fromMs: number;
  readonly toMs: number;
}


/** One row of the "top functions" table. `flat` is the self CPU spent in the
 *  function; flat sums to the total across rows. Unsymbolized frames are
 *  bucketed under a single "(unsymbolized)" name. */
export interface ProfileTopEntry {
  readonly name: string;
  readonly flat: number;
}

/** One point of the CPU-over-time timeline. */
export interface ProfileTimelinePoint {
  readonly timestampMs: number;
  readonly value: number;
}

/** High-level profile stats: a CPU timeline + a ranked top-functions table.
 *  The full flame graph lives in Parca (linked from the panel) to keep browser
 *  memory bounded. */
export interface ProfilesQueryResponse {
  readonly totalSamples: number;
  readonly top: ReadonlyArray<ProfileTopEntry>;
  readonly timeline: ReadonlyArray<ProfileTimelinePoint>;
}
