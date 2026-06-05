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

import { TestBed } from "@angular/core/testing";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { ObservabilityService, ValidationError } from "./observability.service";
import {
  LogsSearchRequest,
  MAX_FREE_TEXT_LEN,
  MAX_PAGE_SIZE,
  MetricsQueryRequest,
} from "./observability.types";

describe("ObservabilityService", () => {
  let service: ObservabilityService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [ObservabilityService],
    });
    service = TestBed.inject(ObservabilityService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  // ----- health -------------------------------------------------------

  it("health() POSTs to /api/observability/health and returns the checks", () => {
    service.health().subscribe(h => {
      expect(h.status).toBe("ok");
      expect(h.checks.logs).toBe(true);
    });
    const req = httpMock.expectOne(r => r.url.endsWith("/observability/health"));
    expect(req.request.method).toBe("GET");
    req.flush({
      status: "ok",
      checks: { logs: true, metrics: true, traces: true, profiles: true },
    });
  });

  // ----- searchLogs validation guards (before HTTP dispatch) ----------

  it("searchLogs throws ValidationError if toMs <= fromMs (no HTTP dispatch)", () => {
    const req: LogsSearchRequest = {
      fromMs: 1_000_000,
      toMs: 500_000, // before fromMs
      pageSize: 50,
    };
    expect(() => service.searchLogs(req)).toThrow();
    httpMock.expectNone(r => r.url.endsWith("/observability/logs/search"));
  });

  it("searchLogs throws ValidationError if pageSize is over MAX_PAGE_SIZE", () => {
    const req: LogsSearchRequest = {
      fromMs: 0,
      toMs: 60_000,
      pageSize: MAX_PAGE_SIZE + 1,
    };
    expect(() => service.searchLogs(req)).toThrow(ValidationError);
    httpMock.expectNone(r => r.url.endsWith("/observability/logs/search"));
  });

  it("searchLogs throws ValidationError if query is over MAX_FREE_TEXT_LEN", () => {
    const tooLong = "x".repeat(MAX_FREE_TEXT_LEN + 1);
    const req: LogsSearchRequest = {
      fromMs: 0,
      toMs: 60_000,
      pageSize: 50,
      query: tooLong,
    };
    expect(() => service.searchLogs(req)).toThrow(ValidationError);
    httpMock.expectNone(r => r.url.endsWith("/observability/logs/search"));
  });

  // ----- searchLogs happy path ----------------------------------------

  it("searchLogs POSTs the validated body to the gateway", () => {
    const req: LogsSearchRequest = {
      fromMs: 0,
      toMs: 60_000,
      pageSize: 50,
      level: "ERROR",
      workflowId: 42,
    };
    service.searchLogs(req).subscribe(resp => {
      expect(resp.entries.length).toBe(0);
    });
    const http = httpMock.expectOne(r => r.url.endsWith("/observability/logs/search"));
    expect(http.request.method).toBe("POST");
    expect(http.request.body.workflowId).toBe(42);
    expect(http.request.body.level).toBe("ERROR");
    http.flush({ entries: [], total: 0 });
  });

  it("searchLogs surfaces server-side 403 unchanged for the caller to render", () => {
    const req: LogsSearchRequest = {
      fromMs: 0,
      toMs: 60_000,
      pageSize: 50,
      workflowId: 999,
    };
    let observedStatus = 0;
    service.searchLogs(req).subscribe({
      next: () => {},
      error: err => {
        observedStatus = err.status;
      },
    });
    const http = httpMock.expectOne(r => r.url.endsWith("/observability/logs/search"));
    http.flush({ code: "forbidden", message: "no access to that scope" }, {
      status: 403,
      statusText: "Forbidden",
    });
    expect(observedStatus).toBe(403);
  });

  // ----- queryMetrics (PR 9) ------------------------------------------

  it("queryMetrics rejects unknown metric names client-side", () => {
    const req = {
      // Cast through unknown to make the test exercise the runtime
      // guard — the type system would otherwise refuse this string.
      name: "evilQuery" as unknown as MetricsQueryRequest["name"],
      fromMs: 0,
      toMs: 60_000,
    };
    expect(() => service.queryMetrics(req)).toThrow(ValidationError);
    httpMock.expectNone(r => r.url.endsWith("/observability/metrics/query"));
  });

  it("queryMetrics rejects step outside [1, 3600]", () => {
    const tooBig: MetricsQueryRequest = {
      name: "runsPerDay",
      fromMs: 0,
      toMs: 60_000,
      stepSec: 99999,
    };
    expect(() => service.queryMetrics(tooBig)).toThrow(ValidationError);
    const tooSmall: MetricsQueryRequest = {
      name: "runsPerDay",
      fromMs: 0,
      toMs: 60_000,
      stepSec: 0,
    };
    expect(() => service.queryMetrics(tooSmall)).toThrow(ValidationError);
  });

  it("queryMetrics dispatches a valid request to /metrics/query", () => {
    const req: MetricsQueryRequest = {
      name: "p95Duration",
      fromMs: 0,
      toMs: 60_000,
      stepSec: 60,
    };
    service.queryMetrics(req).subscribe(r => {
      expect(r.metric).toBe("p95Duration");
    });
    const http = httpMock.expectOne(r => r.url.endsWith("/observability/metrics/query"));
    expect(http.request.method).toBe("POST");
    expect(http.request.body.name).toBe("p95Duration");
    http.flush({ metric: "p95Duration", points: [] });
  });

  // ----- getTrace (PR 10) ---------------------------------------------

  it("getTrace rejects malformed trace ids client-side", () => {
    expect(() => service.getTrace("not-a-trace-id")).toThrow(ValidationError);
    expect(() => service.getTrace("../../etc/passwd")).toThrow(ValidationError);
    expect(() => service.getTrace("0AF7651916CD43DD8448EB211C80319C")).toThrow(); // uppercase
    expect(() => service.getTrace("0af7651916cd43dd8448eb211c80319")).toThrow();  // too short
    httpMock.expectNone(r => r.url.includes("/observability/traces/"));
  });

  it("getTrace dispatches GET /observability/traces/{id} for a valid id", () => {
    const id = "0af7651916cd43dd8448eb211c80319c";
    service.getTrace(id).subscribe(resp => {
      expect(resp.traceId).toBe(id);
    });
    const http = httpMock.expectOne(r => r.url.endsWith(`/observability/traces/${id}`));
    expect(http.request.method).toBe("GET");
    http.flush({ traceId: id, spans: [] });
  });

  // ----- queryProfiles (PR 11) ----------------------------------------

  it("queryProfiles rejects an inverted time window client-side", () => {
    expect(() =>
      service.queryProfiles({ fromMs: 100, toMs: 50 })
    ).toThrow(ValidationError);
    httpMock.expectNone(r => r.url.endsWith("/observability/profiles/query"));
  });

  it("queryProfiles dispatches a valid request", () => {
    service
      .queryProfiles({
        workflowId: 42,
        executionId: 7,
        fromMs: 0,
        toMs: 60_000,
      })
      .subscribe(r => {
        expect(r.totalSamples).toBe(0);
      });
    const http = httpMock.expectOne(r => r.url.endsWith("/observability/profiles/query"));
    expect(http.request.method).toBe("POST");
    expect(http.request.body.workflowId).toBe(42);
    http.flush({ root: null, totalSamples: 0 });
  });
});
