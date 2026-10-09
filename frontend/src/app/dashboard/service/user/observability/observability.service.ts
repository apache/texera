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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { Observable } from "rxjs";
import { AppSettings } from "../../../../common/app-setting";
import {
  LogSourcesResponse,
  LogsSearchRequest,
  LogsSearchResponse,
  MAX_FREE_TEXT_LEN,
  MAX_PAGE_SIZE,
  MetricsQueryRequest,
  MetricsQueryResponse,
  NAMED_METRICS,
  ObservabilityHealth,
  TRACE_ID_RE,
  TracesGetResponse,
  ProfilesQueryRequest,
  ProfilesQueryResponse,
} from "./observability.types";

const BASE_URL = `${AppSettings.getApiEndpoint()}/observability`;

/**
 * Client for the Texera observability gateway.
 *
 * JWT is added by Angular's existing HttpClient interceptor — no
 * explicit auth handling needed here.
 *
 * Light client-side validation mirrors the server-side caps so we
 * surface obvious errors as form errors rather than as HTTP 400s.
 * The server is still the source of truth — these checks are
 * usability, not security.
 */
@Injectable({
  providedIn: "root",
})
export class ObservabilityService {
  constructor(private http: HttpClient) {}

  /**
   * Light reachability check used by the shell to render
   * "Disabled" / "Unreachable" panels.
   */
  health(): Observable<ObservabilityHealth> {
    return this.http.get<ObservabilityHealth>(`${BASE_URL}/health`);
  }

  /**
   * Search application logs. Throws synchronously (before HTTP
   * dispatch) if the request would fail the gateway's own
   * validators — keeps the form UX snappy.
   */
  searchLogs(req: LogsSearchRequest): Observable<LogsSearchResponse> {
    assertValid(req);
    return this.http.post<LogsSearchResponse>(`${BASE_URL}/logs/search`, req);
  }

  /**
   * Distinct filter values currently in the logs store. Used by the
   * logs panel to populate the service / workflow / CU dropdowns
   * with values that actually have data — saves the admin from
   * guessing IDs.
   */
  logSources(): Observable<LogSourcesResponse> {
    return this.http.get<LogSourcesResponse>(`${BASE_URL}/logs/sources`);
  }

  /**
   * Query a named server-side metric. The set of allowed names is
   * the same NAMED_METRICS enum the server enforces; we re-check it
   * here so a typo in a callsite fails fast rather than after a
   * 400 round-trip.
   */
  queryMetrics(req: MetricsQueryRequest): Observable<MetricsQueryResponse> {
    assertValidMetrics(req);
    return this.http.post<MetricsQueryResponse>(`${BASE_URL}/metrics/query`, req);
  }

  /**
   * Fetch a trace by id. The id is regex-validated against
   * TRACE_ID_RE (^[0-9a-f]{32}$) before reaching the network so a
   * malformed value never lands in the URL path. The gateway
   * applies the same regex server-side.
   */
  getTrace(traceId: string): Observable<TracesGetResponse> {
    assertValidTraceId(traceId);
    return this.http.get<TracesGetResponse>(`${BASE_URL}/traces/${encodeURIComponent(traceId)}`);
  }

  /**
   * Query CPU/alloc profiles from Parca. The gateway enforces the
   * time-window cap (7d for profiles); we mirror the start<end
   * check so an inverted range surfaces as a form error.
   */
  queryProfiles(req: ProfilesQueryRequest): Observable<ProfilesQueryResponse> {
    assertValidProfiles(req);
    return this.http.post<ProfilesQueryResponse>(`${BASE_URL}/profiles/query`, req);
  }
}

/** Light client-side validation. Throws Error with a stable
 *  ``code`` field that the UI can branch on. */
function assertValid(req: LogsSearchRequest): void {
  if (req.toMs <= req.fromMs) {
    throw new ValidationError("bad_time_window", "End time must be after start time.");
  }
  if (req.pageSize < 1 || req.pageSize > MAX_PAGE_SIZE) {
    throw new ValidationError("bad_page_size", `pageSize must be between 1 and ${MAX_PAGE_SIZE}.`);
  }
  if (req.query !== undefined && req.query.length > MAX_FREE_TEXT_LEN) {
    throw new ValidationError("free_text_too_long", `Query text must be ${MAX_FREE_TEXT_LEN} characters or fewer.`);
  }
}

function assertValidMetrics(req: MetricsQueryRequest): void {
  if (!NAMED_METRICS.includes(req.name)) {
    throw new ValidationError("bad_metric_name", `unknown metric '${req.name}'.`);
  }
  if (req.toMs <= req.fromMs) {
    throw new ValidationError("bad_time_window", "End time must be after start time.");
  }
  if (req.stepSec !== undefined && (req.stepSec < 1 || req.stepSec > 3600)) {
    throw new ValidationError("bad_step", "Step must be between 1 and 3600 seconds.");
  }
}

function assertValidProfiles(req: ProfilesQueryRequest): void {
  if (req.toMs <= req.fromMs) {
    throw new ValidationError("bad_time_window", "End time must be after start time.");
  }
}

function assertValidTraceId(traceId: string): void {
  if (typeof traceId !== "string" || !TRACE_ID_RE.test(traceId)) {
    throw new ValidationError("bad_trace_id", "Trace id must be 32 lowercase hex characters.");
  }
}

export class ValidationError extends Error {
  constructor(
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = "ValidationError";
  }
}
