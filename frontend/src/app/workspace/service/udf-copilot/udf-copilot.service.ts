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
import { firstValueFrom, Observable, Subject } from "rxjs";
import { take } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";

const UDF_COPILOT_BASE = "/api/udf-copilot";

export interface UdfSchemaCol {
  name: string;
  type: string;
}

export interface UdfContext {
  upstreamSchema?: UdfSchemaCol[];
  sampleRow?: Record<string, unknown>;
  downstreamSchema?: UdfSchemaCol[];
  operatorType?: string;
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

@Injectable({ providedIn: "root" })
export class UdfCopilotService {
  // Operators we still need to fetch sample data for. An entry stays in this
  // set until selectPage(1, 5) returns a non-empty page. The result-initiate
  // stream re-tries when the backend finally tags an operator as paginated
  // (typically only sinks).
  private pendingSampleFetches = new Set<string>();

  // One-shot pending fix: stores error message per operator. Consumed by
  // CodeEditorComponent on startup to auto-open the fix overlay. Necessary
  // because the editor may not exist when "Fix with AI" is clicked.
  private pendingFixes = new Map<string, string>();
  // One-shot open requests: consumed by CodeareaCustomTemplateComponent in
  // ngOnInit. Necessary because the property panel may not be mounted yet
  // when "Fix with AI" is clicked (operator not selected).
  private openRequests = new Set<string>();

  // Live channels — fired by requestFixAndOpen for already-mounted listeners.
  private openEditorSubject = new Subject<{ operatorId: string }>();
  readonly openEditor$ = this.openEditorSubject.asObservable();
  private fixTriggerSubject = new Subject<{ operatorId: string; errorMessage: string }>();
  readonly fixTrigger$ = this.fixTriggerSubject.asObservable();

  /** Called from the error/console panel "Fix with AI" button. */
  requestFixAndOpen(operatorId: string, errorMessage: string): void {
    const enriched = this.enrichTracebackForFix(errorMessage);
    this.pendingFixes.set(operatorId, enriched);
    this.openRequests.add(operatorId);
    this.openEditorSubject.next({ operatorId });
    this.fixTriggerSubject.next({ operatorId, errorMessage: enriched });
  }

  /**
   * Pre-parse the Python traceback to give the LLM a clear signal about
   * whether the error originates in user code or in Texera's framework.
   * Texera writes UDFs to a temp file matching /udf-v\d+\.py/. If no such
   * frame appears in the traceback, the bug is almost certainly NOT in the
   * UDF code itself.
   */
  private enrichTracebackForFix(raw: string): string {
    const matches = [...raw.matchAll(/File "[^"]*\/udf-v\d+\.py", line (\d+), in (\w+)/g)];
    if (matches.length === 0) {
      return (
        raw +
        "\n\n[Diagnostic hint: no user UDF frame found in traceback — this looks " +
        "like a framework or API-contract error, not a Python bug in the UDF " +
        "code. Most likely the UDF yields the wrong shape (e.g. yields a scalar " +
        "instead of a TupleLike).]"
      );
    }
    const last = matches[matches.length - 1];
    return (
      raw +
      `\n\n[Diagnostic hint: deepest user frame is line ${last[1]} in function ` +
      `\`${last[2]}\` of the UDF file. Focus the fix on or near that line.]`
    );
  }

  /** Consumed once by CodeEditorComponent on startup. Returns null if none. */
  consumePendingFix(operatorId: string): string | null {
    const msg = this.pendingFixes.get(operatorId) ?? null;
    this.pendingFixes.delete(operatorId);
    return msg;
  }

  /** Consumed once by CodeareaCustomTemplateComponent in ngOnInit. */
  shouldOpenEditorForFix(operatorId: string): boolean {
    if (this.openRequests.has(operatorId)) {
      this.openRequests.delete(operatorId);
      return true;
    }
    return false;
  }

  // Per-operator sample-row cache. Filled async by selectPage(1) on first ask;
  // invalidated whenever the workflow result stream emits (re-run, dirty page,
  // etc.). The cache lets buildContext stay synchronous for the hot path.
  private sampleRowCache = new Map<string, Record<string, unknown>>();
  private sampleFetchInFlight = new Set<string>();

  constructor(
    private http: HttpClient,
    private workflowActionService: WorkflowActionService,
    private workflowCompilingService: WorkflowCompilingService,
    private workflowResultService: WorkflowResultService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    // The backend only registers a paginated result service for some
    // operators (typically sinks). When that registration finally happens,
    // resultInitiateStream fires — we re-attempt the fetch then.
    this.workflowResultService.getResultInitiateStream().subscribe(operatorId => {
      if (this.pendingSampleFetches.has(operatorId) && !this.sampleRowCache.has(operatorId)) {
        this.fetchPaginatedSampleAsync(operatorId);
      }
    });
  }

  /**
   * Build the Texera context for a UDF operator: the schema flowing into this
   * operator (which is the UDF's input), the downstream consumer's expected
   * schema, the operator type, and (if available) one sample row from the
   * upstream operator's execution result.
   */
  buildContext(operatorId: string): UdfContext {
    const ctx: UdfContext = {};
    const graph = this.workflowActionService.getTexeraGraph();

    try {
      const op = graph.getOperator(operatorId);
      ctx.operatorType = op?.operatorType;
    } catch {
      // operator may not exist yet; leave undefined
    }

    const inputSchema = this.workflowCompilingService.getPortInputSchema(operatorId, 0);
    if (inputSchema && inputSchema.length > 0) {
      ctx.upstreamSchema = inputSchema.map(a => ({ name: a.attributeName, type: a.attributeType }));
    }

    try {
      const outLinks = graph.getOutputLinksByOperatorId?.(operatorId) ?? [];
      const downstreamId = outLinks[0]?.target?.operatorID;
      if (downstreamId) {
        const downstreamSchema = this.workflowCompilingService.getPortInputSchema(downstreamId, 0);
        if (downstreamSchema && downstreamSchema.length > 0) {
          ctx.downstreamSchema = downstreamSchema.map(a => ({ name: a.attributeName, type: a.attributeType }));
        }
      }
    } catch {
      // downstream lookup is best-effort
    }

    const upstreamId = this.getUpstreamOperatorId(operatorId);
    if (upstreamId) {
      const sample = this.getUpstreamSampleRow(upstreamId);
      if (sample) ctx.sampleRow = sample;
    }
    // Fallback — if upstream sample isn't available (sources typically don't
    // get paginated), try ANY cached operator's row and filter to upstream
    // columns. Order of preference: the UDF itself (if it ran), the
    // immediate downstream, then anything else. Values are right; only the
    // origin of the row differs.
    if (!ctx.sampleRow && ctx.upstreamSchema) {
      const candidates: string[] = [
        operatorId, // the UDF's own output usually retains input columns
        this.getDownstreamOperatorId(operatorId) ?? "",
        ...Array.from(this.sampleRowCache.keys()),
      ].filter(Boolean);

      for (const candidateId of candidates) {
        const candidateRow = this.sampleRowCache.get(candidateId);
        if (!candidateRow) continue;
        const filtered: Record<string, unknown> = {};
        for (const col of ctx.upstreamSchema) {
          if (col.name in candidateRow) filtered[col.name] = candidateRow[col.name];
        }
        if (Object.keys(filtered).length > 0) {
          ctx.sampleRow = filtered;
          break;
        }
      }
    }

    return ctx;
  }

  private getDownstreamOperatorId(udfOperatorId: string): string | undefined {
    try {
      const outLinks = this.workflowActionService
        .getTexeraGraph()
        .getOutputLinksByOperatorId?.(udfOperatorId) ?? [];
      return outLinks[0]?.target?.operatorID;
    } catch {
      return undefined;
    }
  }

  /**
   * Warm the sample-row cache for the upstream of `udfOperatorId`. Call this
   * when the UDF editor opens / panel toggles — by the time the user sends a
   * message the row is usually back.
   *
   * Always kicks off a fresh fetch (bypassing the cache) so re-runs eventually
   * overwrite stale rows. The in-flight guard prevents duplicate concurrent
   * requests.
   */
  /**
   * Warm the sample-row cache when the UDF editor opens. Two paths:
   *   1. Worker-side capture — the Texera Python worker POSTs the first
   *      tuple it sees to agent-service. We pull it back by workerId here.
   *      Works for ANY workflow shape since it doesn't depend on backend
   *      pagination tagging.
   *   2. Paginated-service fallback — for operators the backend already
   *      paginates (sinks). Filtered to upstream-schema columns in
   *      buildContext().
   */
  prefetchUpstreamSample(udfOperatorId: string): void {
    const upstreamId = this.getUpstreamOperatorId(udfOperatorId);
    if (!upstreamId) return;

    // Primary path: pull from agent-service by the UDF's worker IDs. The
    // Python worker captured the first input tuple — that's our upstream
    // sample, served back via HTTP.
    const workerIds = this.executeWorkflowService.getWorkerIds(udfOperatorId) ?? [];
    for (const wid of workerIds) {
      this.fetchCapturedSampleByWorkerId(wid, upstreamId);
    }

    // Fallback path: standard paginated fetch for sinks/etc.
    const downstreamId = this.getDownstreamOperatorId(udfOperatorId);
    const sources = [upstreamId, udfOperatorId];
    if (downstreamId) sources.push(downstreamId);
    for (const id of sources) {
      if (this.sampleRowCache.has(id)) continue;
      this.pendingSampleFetches.add(id);
      this.fetchPaginatedSampleAsync(id);
    }
  }

  /**
   * Fetch a sample row that the Python worker shipped to agent-service.
   * Stores under `upstreamId` so buildContext() finds it via the regular
   * cache lookup path.
   */
  private fetchCapturedSampleByWorkerId(workerId: string, upstreamId: string): void {
    this.http
      .get<{ row: Record<string, unknown> | null }>(
        `${UDF_COPILOT_BASE}/sample-row?workerId=${encodeURIComponent(workerId)}`
      )
      .pipe(take(1))
      .subscribe({
        next: res => {
          if (res?.row && !this.sampleRowCache.has(upstreamId)) {
            this.sampleRowCache.set(upstreamId, this.truncateRow(res.row));
          }
        },
        error: () => {},
      });
  }

  private getUpstreamOperatorId(udfOperatorId: string): string | undefined {
    try {
      const inLinks = this.workflowActionService
        .getTexeraGraph()
        .getInputLinksByOperatorId(udfOperatorId);
      return inLinks[0]?.source?.operatorID;
    } catch {
      return undefined;
    }
  }

  private getUpstreamSampleRow(upstreamId: string): Record<string, unknown> | undefined {
    const cached = this.sampleRowCache.get(upstreamId);
    if (cached) return cached;

    // Try the viz-style synchronous snapshot (works for visualization
    // operators that hold their result client-side).
    try {
      const snapshot = this.workflowResultService.getResultService(upstreamId)?.getCurrentResultSnapshot();
      if (snapshot && snapshot.length > 0) {
        const truncated = this.truncateRow(snapshot[0] as Record<string, unknown>);
        this.sampleRowCache.set(upstreamId, truncated);
        return truncated;
      }
    } catch {}

    // Otherwise kick off a paginated fetch in the background. The cache
    // populates whenever the response lands; the next buildContext sees it.
    this.fetchPaginatedSampleAsync(upstreamId);
    return undefined;
  }

  /**
   * Fetch one page from an operator's paginated result service. Only succeeds
   * if WorkflowResultService has already spun up a service for this
   * operatorId (driven by the backend marking it as paginated — usually only
   * sinks). For non-paginated operators this is a no-op; the result-initiate
   * subscription will re-try if the operator gets paginated later.
   */
  private fetchPaginatedSampleAsync(operatorId: string): void {
    if (this.sampleFetchInFlight.has(operatorId)) return;
    const paginated = this.workflowResultService.getPaginatedResultService(operatorId);
    if (!paginated) return;
    this.sampleFetchInFlight.add(operatorId);
    paginated
      .selectPage(1, 5)
      .pipe(take(1))
      .subscribe({
        next: page => {
          const first = page?.table?.[0];
          if (first) {
            this.sampleRowCache.set(operatorId, this.truncateRow(first as Record<string, unknown>));
            this.pendingSampleFetches.delete(operatorId);
          }
          this.sampleFetchInFlight.delete(operatorId);
        },
        error: () => {
          this.sampleFetchInFlight.delete(operatorId);
        },
      });
  }

  /** Trim huge cells so we don't blow up the prompt. ~200 chars per cell. */
  private truncateRow(row: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(row)) {
      if (k === "__row_index__") continue;
      if (typeof v === "string") {
        out[k] = v.length > 200 ? v.slice(0, 200) + "…" : v;
      } else if (v && typeof v === "object") {
        const s = JSON.stringify(v);
        out[k] = s.length > 200 ? s.slice(0, 200) + "…" : v;
      } else {
        out[k] = v;
      }
    }
    return out;
  }

  complete(req: {
    prefix: string;
    suffix?: string;
    context?: UdfContext;
    modelType?: string;
  }): Observable<{ text: string }> {
    return this.http.post<{ text: string }>(`${UDF_COPILOT_BASE}/complete`, req);
  }

  completeAsync(req: {
    prefix: string;
    suffix?: string;
    context?: UdfContext;
    modelType?: string;
  }): Promise<{ text: string }> {
    return firstValueFrom(this.complete(req));
  }

  chat(req: {
    messages: ChatMessage[];
    code?: string;
    context?: UdfContext;
    modelType?: string;
  }): Observable<{ reply: string; suggestedCode?: string }> {
    return this.http.post<{ reply: string; suggestedCode?: string }>(`${UDF_COPILOT_BASE}/chat`, req);
  }

  rewrite(req: {
    selectedCode: string;
    allCode?: string;
    instruction: string;
    context?: UdfContext;
    modelType?: string;
  }): Observable<{ newCode: string }> {
    return this.http.post<{ newCode: string }>(`${UDF_COPILOT_BASE}/rewrite`, req);
  }

  fix(req: {
    errorMessage: string;
    code: string;
    range?: { startLine: number; startColumn: number; endLine: number; endColumn: number };
    context?: UdfContext;
    modelType?: string;
  }): Observable<{ newCode: string; explanation: string }> {
    return this.http.post<{ newCode: string; explanation: string }>(`${UDF_COPILOT_BASE}/fix`, req);
  }

  syncSchema(req: {
    code: string;
    currentOutputColumns?: { attributeName: string; attributeType: string }[];
    currentRetainInputColumns?: boolean;
    context?: UdfContext;
    modelType?: string;
  }): Observable<{
    retainInputColumns: boolean;
    outputColumns: { attributeName: string; attributeType: string }[];
    explanation: string;
  }> {
    return this.http.post<{
      retainInputColumns: boolean;
      outputColumns: { attributeName: string; attributeType: string }[];
      explanation: string;
    }>(`${UDF_COPILOT_BASE}/sync-schema`, req);
  }
}
