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

import { Component, OnDestroy, OnInit } from "@angular/core";
import { DatePipe, KeyValuePipe, LowerCasePipe, NgFor, NgIf } from "@angular/common";
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { EMPTY, Subject, Subscription, interval } from "rxjs";
import { startWith, switchMap, takeUntil } from "rxjs/operators";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzDatePickerComponent, NzRangePickerComponent } from "ng-zorro-antd/date-picker";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzOptionComponent, NzSelectComponent } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { ObservabilityService, ValidationError } from "../../../../service/user/observability/observability.service";
import { loadPanelPrefs, savePanelPrefs } from "../../../../service/user/observability/observability-prefs";
import { TracesPivotService } from "../../../../service/user/observability/traces-pivot.service";
import {
  LOG_LEVELS,
  LOG_SORTS,
  LogEntry,
  LogLevel,
  LogSort,
  LogSourcesResponse,
  MAX_FREE_TEXT_LEN,
  MAX_PAGE_SIZE,
} from "../../../../service/user/observability/observability.types";

/**
 * Grafana-style log explorer for cross-microservice debugging.
 *
 * Filters available (all optional except time):
 *   - Time window (always required)
 *   - Service multi-select (autofilled from /logs/sources)
 *   - Workflow id picker (autofilled)
 *   - Computing Unit id picker (autofilled)
 *   - Execution id (free numeric)
 *   - Level filter (closed enum)
 *   - Free-text body search (capped server-side)
 *
 * Each row in the result list renders as `[timestamp] [LEVEL] [service]
 * message`, color-coded by level. Clicking a row expands its attributes
 * (logger.name, thread.name, trace_id, etc.) in a side panel — same
 * affordance as Grafana Loki's "Show detected fields".
 *
 * Security: the log body is bound with regular {{ entry.body }}
 * interpolation. The gateway already redacts via LogSanitizer.
 */
@Component({
  selector: "texera-observability-logs-panel",
  templateUrl: "./logs-panel.component.html",
  styleUrls: ["./logs-panel.component.scss"],
  imports: [
    NgIf,
    NgFor,
    DatePipe,
    KeyValuePipe,
    LowerCasePipe,
    ReactiveFormsModule,
    NzAlertComponent,
    NzButtonComponent,
    NzDatePickerComponent,
    NzRangePickerComponent,
    NzInputDirective,
    NzOptionComponent,
    NzSelectComponent,
    NzSpinComponent,
  ],
})
export class LogsPanelComponent implements OnInit, OnDestroy {
  readonly levels = LOG_LEVELS;
  readonly sortOptions = LOG_SORTS;
  readonly maxPageSize = MAX_PAGE_SIZE;
  readonly maxFreeTextLen = MAX_FREE_TEXT_LEN;

  /** Auto-refresh interval choices (seconds). 0 means OFF. */
  readonly refreshOptions: ReadonlyArray<{ value: number; label: string }> = [
    { value: 0, label: "Off" },
    { value: 5, label: "5s" },
    { value: 10, label: "10s" },
    { value: 30, label: "30s" },
  ];

  private readonly destroy$ = new Subject<void>();
  /** Reference to the currently-in-flight searchLogs subscription so
   *  we can cancel it when a fresh search starts. Without this, a
   *  rapid Search click (or an auto-refresh tick) lets both responses
   *  land — the slower one then overwrites the newer one and the
   *  panel "stacks" the wrong page. */
  private currentSearch: Subscription | null = null;

  // nz-range-picker binds to a single tuple `[from, to]`. Separate
  // FormControls for from/to crash the picker at writeValue time
  // (it calls `.map` on the bound value expecting an array).
  form = new FormGroup({
    range: new FormControl<[Date, Date] | null>([defaultFrom(), new Date()], Validators.required),
    services: new FormControl<string[]>([]),
    workflowId: new FormControl<number | null>(null),
    computingUnitId: new FormControl<number | null>(null),
    userId: new FormControl<number | null>(null),
    executionId: new FormControl<number | null>(null),
    level: new FormControl<LogLevel | null>(null),
    query: new FormControl<string>("", Validators.maxLength(MAX_FREE_TEXT_LEN)),
    sort: new FormControl<LogSort>("newest"),
    pageSize: new FormControl<number>(200, [Validators.min(1), Validators.max(MAX_PAGE_SIZE)]),
    // Auto-refresh interval in seconds. 0 = off (default). Pinning
    // the live-update toggle to the form here keeps it next to the
    // other filter state and means form-snapshot logic doesn't need
    // a second source-of-truth.
    autoRefreshSec: new FormControl<number>(0),
  });

  /** 0-indexed current page number. A fresh search() resets to 0,
   *  paginate() steps through pages, the template renders +1 for
   *  user-visible "Page 1". The cursor sent to the server is just
   *  `pageNumber * pageSize` stringified — opaque to the wire. */
  currentPage = 0;
  /** History of cursors for the Prev button. The gateway only emits
   *  next-cursor; we remember where we came from so back-paging works
   *  without an extra round trip. */
  private cursorHistory: Array<string | undefined> = [undefined];

  /** Autofill options populated from `/observability/logs/sources` on
   *  init. Empty arrays render the dropdowns as "no data yet". */
  sources: LogSourcesResponse = { services: [], workflowIds: [], computingUnitIds: [], userIds: [] };

  entries: ReadonlyArray<LogEntry> = [];
  total = 0;
  nextCursor: string | undefined;
  loading = false;
  errorMessage: string | null = null;

  /** Index of the currently expanded row (for the attribute panel). */
  expandedIndex: number | null = null;

  constructor(
    private observabilityService: ObservabilityService,
    private tracesPivot: TracesPivotService
  ) {}

  ngOnInit(): void {
    // Restore the operator's last filters (service scope, level, sort, page
    // size, auto-refresh, etc.). The time range is intentionally not persisted
    // and keeps its fresh default window.
    const prefs = loadPanelPrefs<typeof this.form.value>("logs");
    if (prefs) this.form.patchValue(prefs);
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(v => savePanelPrefs("logs", v, ["range"]));

    this.refreshSources();
    this.search();

    // Auto-refresh: re-run the current first-page search at the
    // selected interval. We deliberately skip ticks while paginated
    // (currentPage !== 0) so a long Page 3 read isn't yanked out
    // from under the user; toggle pauses cleanly. switchMap also
    // means changing the interval value flips to the new cadence
    // without leaking old timers.
    this.form.controls.autoRefreshSec.valueChanges
      .pipe(
        startWith(this.form.controls.autoRefreshSec.value ?? 0),
        switchMap(secs => (secs && secs > 0 ? interval(secs * 1000) : EMPTY)),
        takeUntil(this.destroy$)
      )
      .subscribe(() => {
        if (this.currentPage === 0 && !this.loading) this.search();
      });
  }

  ngOnDestroy(): void {
    if (this.currentSearch) {
      this.currentSearch.unsubscribe();
      this.currentSearch = null;
    }
    this.destroy$.next();
    this.destroy$.complete();
  }

  /** Pull the latest filter values from the gateway and re-bind the
   *  autofill dropdowns. Called on init and after every successful
   *  search so newly-emitted services / CUs surface promptly. */
  refreshSources(): void {
    this.observabilityService
      .logSources()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: s => {
          // Defensive copy + normalisation: an empty object response,
          // or a partial response, must not throw at template iteration
          // time (*ngFor over undefined would NgError out the panel).
          this.sources = {
            services: s?.services ?? [],
            workflowIds: s?.workflowIds ?? [],
            computingUnitIds: s?.computingUnitIds ?? [],
            userIds: s?.userIds ?? [],
          };
          // One concise console line so an operator can confirm autofill
          // really did load — helps diagnose "empty dropdowns" without
          // attaching a debugger. Cheap; only fires once on init + on
          // explicit refresh.
          // eslint-disable-next-line no-console
          console.info(
            `[observability] sources loaded: ${this.sources.services.length} services, ` +
              `${this.sources.workflowIds.length} workflows, ` +
              `${this.sources.computingUnitIds.length} CUs, ` +
              `${this.sources.userIds.length} users`
          );
        },
        // Autofill failures don't block search — the form still works
        // with manual id entry. Surface only as a soft warning.
        error: (err: unknown) => {
          // Don't block the panel; the dropdowns just stay empty and the
          // manual numeric inputs still work. Log a soft warning so the
          // "why are my dropdowns empty?" question has an answer in the
          // console rather than requiring a debugger.
          // eslint-disable-next-line no-console
          console.warn("[observability] log sources failed to load — filter dropdowns will be empty", err);
        },
      });
  }

  /** Called by the template when the user clicks a trace id on a
   *  log row. Delegates regex validation to the pivot service —
   *  a corrupted log entry can never wedge the shell. */
  openTrace(traceId: string | undefined): void {
    if (traceId) this.tracesPivot.pivot(traceId);
  }

  toggleRow(i: number): void {
    this.expandedIndex = this.expandedIndex === i ? null : i;
  }

  /** Submit the current form. A fresh search() resets pagination;
   *  use [[nextPage]] / [[prevPage]] to step through pages. Also
   *  refreshes the autofill so newly-created CUs / services / users
   *  surface in the dropdowns without a full page reload. */
  search(): void {
    this.currentPage = 0;
    this.cursorHistory = [undefined];
    // Background-refresh sources; doesn't block the search.
    this.refreshSources();
    this.runQuery(undefined);
  }

  nextPage(): void {
    if (!this.nextCursor) return;
    // Remember the cursor that got us here so prev() can rewind.
    this.cursorHistory.push(this.nextCursor);
    this.currentPage += 1;
    this.runQuery(this.nextCursor);
  }

  prevPage(): void {
    if (this.currentPage === 0) return;
    this.cursorHistory.pop(); // discard the cursor we used for the current page
    this.currentPage -= 1;
    const prevCursor = this.cursorHistory[this.cursorHistory.length - 1];
    this.runQuery(prevCursor);
  }

  /** Shared execution path for first-page + paginated requests. */
  private runQuery(cursor: string | undefined): void {
    const v = this.form.value;
    const range = v.range;
    if (!range || range.length !== 2 || !range[0] || !range[1]) return;

    const req = {
      fromMs: range[0].getTime(),
      toMs: range[1].getTime(),
      level: v.level ?? undefined,
      query: nonEmpty(v.query),
      sort: v.sort ?? "newest",
      workflowId: nullToUndefined(v.workflowId),
      executionId: nullToUndefined(v.executionId),
      computingUnitId: nullToUndefined(v.computingUnitId),
      userId: nullToUndefined(v.userId),
      services: nonEmptyArray(v.services),
      pageSize: v.pageSize ?? 200,
      pageCursor: cursor,
    };

    this.loading = true;
    this.errorMessage = null;
    // Cancel any in-flight search BEFORE starting the new one. This
    // is the switchMap-style guarantee: if the user clicks Search
    // twice in a row (or auto-refresh fires while a click is still
    // resolving), the older HTTP response gets discarded instead of
    // overwriting the newer one. Same path covers pagination clicks.
    if (this.currentSearch) {
      this.currentSearch.unsubscribe();
      this.currentSearch = null;
    }
    try {
      this.currentSearch = this.observabilityService
        .searchLogs(req)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: resp => {
            // Pagination: REPLACE the visible page (each page stands
            // alone). The "Load more"-style append behaviour was
            // confusing on long runs because the visible row count
            // grew but the "Page" indicator didn't change.
            this.entries = resp.entries;
            this.total = resp.total;
            this.nextCursor = resp.nextCursor;
            this.loading = false;
            this.expandedIndex = null;
            this.currentSearch = null;
          },
          error: (err: unknown) => {
            this.errorMessage = humanizeError(err);
            this.loading = false;
            this.currentSearch = null;
          },
        });
    } catch (e) {
      if (e instanceof ValidationError) {
        this.errorMessage = e.message;
      } else {
        this.errorMessage = "Unexpected error.";
      }
      this.loading = false;
      this.currentSearch = null;
    }
  }

  clear(): void {
    this.entries = [];
    this.total = 0;
    this.nextCursor = undefined;
    this.errorMessage = null;
    this.expandedIndex = null;
    this.currentPage = 0;
    this.cursorHistory = [undefined];
  }

  /** "Show only this service" affordance — clicked from a row's service
   *  badge. Convenience over the multi-select dropdown when the user has
   *  already eye-balled a candidate service in the results. */
  scopeToService(service: string): void {
    this.form.patchValue({ services: [service] });
    this.search();
  }
}

function defaultFrom(): Date {
  // Default window: last hour.
  return new Date(Date.now() - 60 * 60 * 1000);
}

function nullToUndefined<T>(v: T | null | undefined): T | undefined {
  return v == null ? undefined : v;
}

function nonEmpty(v: string | null | undefined): string | undefined {
  const trimmed = (v ?? "").trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function nonEmptyArray(v: string[] | null | undefined): string[] | undefined {
  return v != null && v.length > 0 ? v : undefined;
}

function humanizeError(err: unknown): string {
  if (typeof err === "object" && err !== null) {
    const body = (err as { error?: { code?: string; message?: string } }).error;
    if (body?.message) return body.message;
  }
  return "Failed to load logs.";
}
