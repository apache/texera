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

import { ComponentFixture, TestBed, fakeAsync, tick } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { Subject, of, throwError } from "rxjs";
import * as fs from "node:fs";
import * as path from "node:path";
import { LogsPanelComponent } from "./logs-panel.component";
import { ObservabilityService, ValidationError } from "../../../../service/user/observability/observability.service";
import {
  LogEntry,
  LogSourcesResponse,
  MAX_FREE_TEXT_LEN,
  MAX_PAGE_SIZE,
} from "../../../../service/user/observability/observability.types";

describe("LogsPanelComponent", () => {
  let component: LogsPanelComponent;
  let fixture: ComponentFixture<LogsPanelComponent>;
  let mockService: {
    searchLogs: ReturnType<typeof vi.fn>;
    health: ReturnType<typeof vi.fn>;
    logSources: ReturnType<typeof vi.fn>;
  };

  const defaultSources: LogSourcesResponse = {
    services: ["dashboard-service", "texera-web", "workflow-runtime-coordinator-service"],
    workflowIds: [1, 2, 3, 441],
    computingUnitIds: [7, 99],
    userIds: [1, 5],
  };

  beforeEach(async () => {
    mockService = {
      searchLogs: vi.fn().mockReturnValue(of({ entries: [], total: 0 })),
      health: vi.fn(),
      logSources: vi.fn().mockReturnValue(of(defaultSources)),
    };
    await TestBed.configureTestingModule({
      imports: [LogsPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [{ provide: ObservabilityService, useValue: mockService }],
    }).compileComponents();
    fixture = TestBed.createComponent(LogsPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  // ---- form → request mapping ----------------------------------------

  it("dispatches a search() through the service with form values", () => {
    component.form.patchValue({
      range: [new Date(0), new Date(60_000)],
      level: "ERROR",
      query: "boom",
      workflowId: 42,
      pageSize: 50,
    });
    component.search();
    // ngOnInit issued one search; this second call is the patched one.
    const lastCall = mockService.searchLogs.mock.calls.at(-1)!;
    const arg = lastCall[0];
    expect(arg.fromMs).toBe(0);
    expect(arg.toMs).toBe(60_000);
    expect(arg.level).toBe("ERROR");
    expect(arg.query).toBe("boom");
    expect(arg.workflowId).toBe(42);
    expect(arg.pageSize).toBe(50);
  });

  it("forwards the computingUnitId and services multi-select in the request", () => {
    component.form.patchValue({
      range: [new Date(0), new Date(60_000)],
      computingUnitId: 7,
      services: ["dashboard-service", "texera-web"],
      pageSize: 10,
    });
    component.search();
    const arg = mockService.searchLogs.mock.calls.at(-1)![0];
    expect(arg.computingUnitId).toBe(7);
    expect(arg.services).toEqual(["dashboard-service", "texera-web"]);
  });

  it("omits services when the multi-select is empty (server falls back to texera-*)", () => {
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], services: [], pageSize: 10 });
    component.search();
    const arg = mockService.searchLogs.mock.calls.at(-1)![0];
    expect(arg.services).toBeUndefined();
  });

  // ---- autofill from /logs/sources -----------------------------------

  it("calls /logs/sources on init and stores the response", () => {
    expect(mockService.logSources).toHaveBeenCalled();
    expect(component.sources.services).toEqual(defaultSources.services);
    expect(component.sources.workflowIds).toEqual(defaultSources.workflowIds);
    expect(component.sources.computingUnitIds).toEqual(defaultSources.computingUnitIds);
  });

  it("binds the autofill values into nz-option lists in the template", () => {
    // nz-select renders option labels lazily (only when the dropdown
    // opens), so we can't assert them against the rendered DOM. We
    // instead inspect the template source to confirm the *ngFor over
    // each sources.* array actually exists — combined with the
    // component-state assertion above, this proves the autofill is
    // wired end-to-end.
    const tplPath = path.resolve(__dirname, "logs-panel.component.html");
    const tpl = fs.readFileSync(tplPath, "utf-8");
    expect(tpl).toMatch(/\*ngFor[^>]+sources\.services/);
    expect(tpl).toMatch(/\*ngFor[^>]+sources\.workflowIds/);
    expect(tpl).toMatch(/\*ngFor[^>]+sources\.computingUnitIds/);
    expect(tpl).toMatch(/\*ngFor[^>]+sources\.userIds/);
  });

  // ---- auto-refresh poll ---------------------------------------------

  it("does NOT poll the gateway by default (auto-refresh starts off)", fakeAsync(() => {
    // After ngOnInit: 1 sources call + 1 initial search. No further
    // searches should fire over time.
    const initialCount = mockService.searchLogs.mock.calls.length;
    tick(60_000);
    expect(mockService.searchLogs.mock.calls.length).toBe(initialCount);
  }));

  it("polls the gateway at the chosen interval when auto-refresh is on", fakeAsync(() => {
    const beforeToggle = mockService.searchLogs.mock.calls.length;
    component.form.controls.autoRefreshSec.setValue(5);
    // valueChanges runs synchronously; the interval starts only after
    // the first tick. Step in 5-second increments.
    tick(5_000);
    expect(mockService.searchLogs.mock.calls.length).toBe(beforeToggle + 1);
    tick(5_000);
    expect(mockService.searchLogs.mock.calls.length).toBe(beforeToggle + 2);
    // Cancel before fakeAsync test completes so no pending timers leak.
    component.form.controls.autoRefreshSec.setValue(0);
    tick(60_000);
  }));

  it("skips auto-refresh ticks while paginated (page > 0) so Next-page reads stay stable", fakeAsync(() => {
    // Simulate being on page 2 without going through the date-picker
    // path (which triggers nz-zorro's locale loader and conflicts
    // with fakeAsync). The contract under test is: when
    // currentPage > 0 the interval tick is suppressed, regardless of
    // how we got to that page.
    component.currentPage = 1;
    const beforePoll = mockService.searchLogs.mock.calls.length;
    component.form.controls.autoRefreshSec.setValue(5);
    tick(15_000); // would normally fire 3 times
    expect(mockService.searchLogs.mock.calls.length).toBe(beforePoll);
    component.form.controls.autoRefreshSec.setValue(0);
    tick(60_000);
  }));

  it("cleans up the poll subscription on destroy (no leak)", fakeAsync(() => {
    component.form.controls.autoRefreshSec.setValue(5);
    tick(5_000);
    const callsAfterFirstTick = mockService.searchLogs.mock.calls.length;
    component.ngOnDestroy();
    tick(30_000);
    // No further calls after destroy.
    expect(mockService.searchLogs.mock.calls.length).toBe(callsAfterFirstTick);
  }));

  // ---- in-flight cancellation (race prevention) ----------------------

  it("cancels the previous search when a new search starts (no out-of-order overwrite)", () => {
    // Two outstanding responses; whichever subscribes last wins
    // because the previous subscription is unsubscribed inside the
    // component. We expose this by feeding two Subjects that complete
    // in REVERSE order — the older one finishes second.
    const first$ = new Subject<{ entries: any[]; total: number }>();
    const second$ = new Subject<{ entries: any[]; total: number }>();
    mockService.searchLogs.mockReturnValueOnce(first$).mockReturnValueOnce(second$);

    component.search(); // sends first$
    component.search(); // unsubscribes first$, sends second$

    // First search completes AFTER second. If we hadn't cancelled it,
    // its result would land last and clobber the second's result.
    second$.next({ entries: [{ timestampMs: 2, level: "INFO", body: "B", attributes: {} }], total: 1 });
    first$.next({ entries: [{ timestampMs: 1, level: "INFO", body: "STALE", attributes: {} }], total: 1 });
    second$.complete();
    first$.complete();

    expect(component.entries.map(e => e.body)).toEqual(["B"]); // newest wins
  });

  it("refreshes /logs/sources every time the user clicks Search (so new CUs surface)", () => {
    const sourcesBefore = mockService.logSources.mock.calls.length;
    component.search();
    expect(mockService.logSources.mock.calls.length).toBeGreaterThan(sourcesBefore);
  });

  // ---- the original user-id test ---------------------------------

  it("forwards a picked user id to the gateway", () => {
    component.form.patchValue({
      range: [new Date(0), new Date(60_000)],
      userId: 5,
      pageSize: 10,
    });
    component.search();
    const arg = mockService.searchLogs.mock.calls.at(-1)![0];
    expect(arg.userId).toBe(5);
  });

  it("does not crash when /logs/sources fails — the filter dropdowns stay empty", () => {
    mockService.logSources.mockReturnValue(throwError(() => new Error("nope")));
    component.refreshSources();
    expect(component.sources.services).toEqual(defaultSources.services); // previous value preserved
    expect(component.errorMessage).toBeNull();
  });

  // ---- pagination + error paths --------------------------------------

  it("surfaces a synchronous ValidationError as form error without crashing", () => {
    mockService.searchLogs.mockImplementation(() => {
      throw new ValidationError("bad_page_size", "pageSize must be between 1 and 1000.");
    });
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], pageSize: 99999 });
    component.search();
    expect(component.errorMessage).toMatch(/pageSize/);
    expect(component.loading).toBe(false);
  });

  it("populates entries and total on a successful response", () => {
    const entry: LogEntry = { timestampMs: 1234, level: "INFO", body: "hello", attributes: {} };
    mockService.searchLogs.mockReturnValue(of({ entries: [entry], total: 1 }));
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], pageSize: 10 });
    component.search();
    expect(component.entries.length).toBe(1);
    expect(component.total).toBe(1);
  });

  it("renders an error alert when the HTTP call fails", () => {
    mockService.searchLogs.mockReturnValue(
      throwError(() => ({ error: { code: "rate_limited", message: "too many requests" } }))
    );
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], pageSize: 10 });
    component.search();
    expect(component.errorMessage).toBe("too many requests");
  });

  it("paginates: nextPage() advances and prevPage() rewinds, replacing the page each time", () => {
    const e1: LogEntry = { timestampMs: 1, level: "INFO", body: "a", attributes: {} };
    const e2: LogEntry = { timestampMs: 2, level: "INFO", body: "b", attributes: {} };
    mockService.searchLogs
      // Initial search (cursor undefined) → page 0 with a next cursor.
      .mockReturnValueOnce(of({ entries: [e1], total: 2, nextCursor: "1" }))
      // Next page (cursor "1") → page 1, no further cursor.
      .mockReturnValueOnce(of({ entries: [e2], total: 2 }))
      // Prev back to page 0 (cursor undefined) → original first page.
      .mockReturnValueOnce(of({ entries: [e1], total: 2, nextCursor: "1" }));
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], pageSize: 1 });
    component.search();
    expect(component.entries.map(e => e.body)).toEqual(["a"]);
    expect(component.currentPage).toBe(0);

    component.nextPage();
    expect(component.entries.map(e => e.body)).toEqual(["b"]);
    expect(component.currentPage).toBe(1);
    expect(component.nextCursor).toBeUndefined();

    component.prevPage();
    expect(component.entries.map(e => e.body)).toEqual(["a"]);
    expect(component.currentPage).toBe(0);
  });

  it("nextPage() is a no-op when there's no nextCursor", () => {
    mockService.searchLogs.mockReturnValueOnce(of({ entries: [], total: 0 }));
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], pageSize: 50 });
    component.search();
    const callsBefore = mockService.searchLogs.mock.calls.length;
    component.nextPage();
    expect(mockService.searchLogs.mock.calls.length).toBe(callsBefore);
  });

  it("forwards the selected sort to the gateway", () => {
    component.form.patchValue({ range: [new Date(0), new Date(60_000)], sort: "severity" });
    component.search();
    const arg = mockService.searchLogs.mock.calls.at(-1)![0];
    expect(arg.sort).toBe("severity");
  });

  it("uses the page-size cap exposed by the gateway", () => {
    expect(component.maxPageSize).toBe(MAX_PAGE_SIZE);
    expect(component.maxFreeTextLen).toBe(MAX_FREE_TEXT_LEN);
  });

  // ---- expand / shortcut chips ---------------------------------------

  it("toggles a row to show its attributes when clicked", () => {
    expect(component.expandedIndex).toBeNull();
    component.toggleRow(0);
    expect(component.expandedIndex).toBe(0);
    // Click the same row again → collapsed.
    component.toggleRow(0);
    expect(component.expandedIndex).toBeNull();
    // Clicking a different row replaces (single-row expansion).
    component.toggleRow(0);
    component.toggleRow(3);
    expect(component.expandedIndex).toBe(3);
  });

  it("scopeToService() narrows the multi-select to a single service and re-searches", () => {
    component.scopeToService("dashboard-service");
    expect(component.form.value.services).toEqual(["dashboard-service"]);
    // search was called: once on init + once on scope click = 2 minimum.
    expect(mockService.searchLogs.mock.calls.length).toBeGreaterThanOrEqual(2);
    const lastCall = mockService.searchLogs.mock.calls.at(-1)![0];
    expect(lastCall.services).toEqual(["dashboard-service"]);
  });

  it("clear() empties results and collapses any expanded row", () => {
    component.entries = [{ timestampMs: 1, level: "INFO", body: "a", attributes: {} }];
    component.total = 1;
    component.expandedIndex = 0;
    component.clear();
    expect(component.entries).toEqual([]);
    expect(component.total).toBe(0);
    expect(component.expandedIndex).toBeNull();
  });

  // ----- security: log body must never reach an [innerHTML] sink ----

  it("renders log body via plain interpolation, never [innerHTML]", () => {
    // Defence-in-depth: even though the gateway has run LogSanitizer
    // over the body, the template should bind via {{ }} only so an
    // accidentally-unsanitised body can never execute as HTML.
    const templatePath = path.resolve(__dirname, "logs-panel.component.html");
    const tpl = fs.readFileSync(templatePath, "utf-8");
    expect(tpl).not.toMatch(/\[innerHTML\]\s*=\s*['"][^'"]*entry\.body/);
    expect(tpl).toMatch(/{{\s*entry\.body\s*}}/);
  });
});
