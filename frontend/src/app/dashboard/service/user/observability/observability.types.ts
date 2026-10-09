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
  readonly workflowId?: number;
  readonly executionId?: number;
  readonly fromMs: number;
  readonly toMs: number;
}

/** Recursive flame-graph frame. value is the sample count (or any
 *  positive measure) at this node; children's values sum to <= value
 *  per pprof convention. */
export interface FlameFrame {
  readonly name: string;
  readonly value: number;
  readonly children: ReadonlyArray<FlameFrame>;
}

export interface ProfilesQueryResponse {
  readonly root: FlameFrame | null;
  readonly totalSamples: number;
}
