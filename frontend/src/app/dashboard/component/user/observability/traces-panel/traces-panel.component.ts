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

import { Component, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from "@angular/forms";
import { NzAlertComponent } from "ng-zorro-antd/alert";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { NzInputDirective } from "ng-zorro-antd/input";
import { Subject, takeUntil } from "rxjs";
import { ObservabilityService, ValidationError } from "../../../../service/user/observability/observability.service";
import { TracesPivotService } from "../../../../service/user/observability/traces-pivot.service";
import { TRACE_ID_RE, TraceSpan, TracesGetResponse } from "../../../../service/user/observability/observability.types";

/**
 * Trace tree panel.
 *
 * Inputs:
 *   - a free-text trace id input that the user can paste or type;
 *   - an @Input() initialTraceId set by the shell when pivoting in
 *     from the Logs panel.
 *
 * We render the spans as a typed tree built from the parentSpanId
 * relationships in the response. Span name, duration, and attribute
 * values are bound with Angular text interpolation only — never with
 * [innerHTML] — so a Jaeger span with a crafted name cannot inject
 * HTML into the renderer.
 *
 * The gateway has already validated the trace id (regex), enforced
 * scope, and proxied the response. We re-check the trace id here
 * (one regex, no allocation) so a stale @Input() value can never
 * trigger a request the gateway would reject anyway.
 */
@Component({
  selector: "texera-observability-traces-panel",
  templateUrl: "./traces-panel.component.html",
  styleUrls: ["./traces-panel.component.scss"],
  imports: [CommonModule, ReactiveFormsModule, NzAlertComponent, NzButtonComponent, NzEmptyComponent, NzInputDirective],
})
export class TracesPanelComponent implements OnInit, OnChanges, OnDestroy {
  /** Set by the shell after a logs→traces pivot. Changes here
   *  trigger an automatic fetch. */
  @Input() initialTraceId: string | null = null;

  form = new FormGroup({
    traceId: new FormControl<string>("", [Validators.required, Validators.pattern(TRACE_ID_RE)]),
  });

  loading = false;
  errorMessage: string | null = null;
  trace: TracesGetResponse | null = null;
  /** Rendered tree — pre-computed in component so the template
   *  stays declarative. */
  spanTree: ReadonlyArray<SpanNode> = [];

  private readonly destroy$ = new Subject<void>();

  constructor(
    private observabilityService: ObservabilityService,
    private tracesPivot: TracesPivotService
  ) {}

  ngOnInit(): void {
    this.tracesPivot.onPivot.pipe(takeUntil(this.destroy$)).subscribe(traceId => {
      this.form.patchValue({ traceId });
      this.fetch();
    });
  }

  ngOnChanges(changes: SimpleChanges): void {
    // Auto-fetch only on the first binding (panel created after the
    // pivot emit, which the onPivot subscription missed). Later changes
    // arrive with an onPivot emit, so fetching here too would duplicate.
    const change = changes["initialTraceId"];
    if (change?.isFirstChange() && this.initialTraceId) {
      this.form.patchValue({ traceId: this.initialTraceId });
      this.fetch();
    }
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  fetch(): void {
    const id = this.form.value.traceId?.trim() ?? "";
    if (!TRACE_ID_RE.test(id)) {
      this.errorMessage = "Trace id must be 32 lowercase hex characters.";
      return;
    }
    this.loading = true;
    this.errorMessage = null;
    this.trace = null;
    this.spanTree = [];

    try {
      this.observabilityService
        .getTrace(id)
        .pipe(takeUntil(this.destroy$))
        .subscribe({
          next: resp => {
            this.trace = resp;
            this.spanTree = buildSpanTree(resp.spans);
            this.loading = false;
          },
          error: (err: unknown) => {
            this.errorMessage = humanizeError(err);
            this.loading = false;
          },
        });
    } catch (e) {
      if (e instanceof ValidationError) {
        this.errorMessage = e.message;
      }
      this.loading = false;
    }
  }
}

/** Rendered tree node — keeps duration + indentation pre-computed
 *  so the template is dumb. */
export interface SpanNode {
  readonly span: TraceSpan;
  readonly durationMs: number;
  /** Sorted ascending by start time for visual stability. */
  readonly children: ReadonlyArray<SpanNode>;
}

/**
 * Pure function: convert a flat span list into a tree using
 * parentSpanId relations. Spans whose parent is missing (or
 * top-level) become roots. Defensive against cycles — a span that
 * lists itself as parent is treated as a root. Multiple roots are
 * supported (sorted by start time).
 *
 * Exported so it can be unit-tested without a Dropwizard/HTTP
 * fixture.
 */
export function buildSpanTree(spans: ReadonlyArray<TraceSpan>): ReadonlyArray<SpanNode> {
  const byId = new Map<string, TraceSpan>();
  spans.forEach(s => byId.set(s.spanId, s));

  const childrenByParent = new Map<string, TraceSpan[]>();
  const roots: TraceSpan[] = [];

  spans.forEach(s => {
    const parent = s.parentSpanId;
    if (!parent || parent === s.spanId || !byId.has(parent)) {
      roots.push(s);
    } else {
      const bucket = childrenByParent.get(parent) ?? [];
      bucket.push(s);
      childrenByParent.set(parent, bucket);
    }
  });

  const sortByStart = (a: TraceSpan, b: TraceSpan) => a.startMs - b.startMs;

  function build(s: TraceSpan): SpanNode {
    const kids = (childrenByParent.get(s.spanId) ?? []).slice().sort(sortByStart);
    return {
      span: s,
      durationMs: Math.max(0, s.endMs - s.startMs),
      children: kids.map(build),
    };
  }

  return roots.slice().sort(sortByStart).map(build);
}

function humanizeError(err: unknown): string {
  if (typeof err === "object" && err !== null) {
    const body = (err as { error?: { code?: string; message?: string } }).error;
    if (body?.message) return body.message;
  }
  return "Failed to load trace.";
}
