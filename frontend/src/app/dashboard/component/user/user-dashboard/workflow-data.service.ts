/**
 * Data source for the "From Workflow" path in the Add Widget modal.
 *
 * Texera does not expose operator tuple data via REST — results live in the
 * WebSocket cache during a workspace session. We work with two sources:
 *
 * 1. REST stats — `/executions/{wid}/latest` + `/executions/{wid}/stats/{eid}`
 *    return per-operator runtime stats (tuple counts, sizes, processing
 *    times). Always available after a run.
 *
 * 2. localStorage snapshots — workspace code (e.g. the Results Dashboard
 *    panel on the hackathon/dataset-results branch) writes structured run
 *    outputs to `texera.workflow.results.{wid}`. We read them here so the
 *    dashboard can display real numbers from evaluation operators (accuracy,
 *    F1, etc.) and tabular outputs without a new backend endpoint.
 *
 * The shape under `texera.workflow.results.{wid}` is documented as
 * `WorkflowResultsSnapshot` below — any code that wants to surface data on
 * the dashboard should write to that key.
 */

import { Injectable } from "@angular/core";
import { Observable, forkJoin, of } from "rxjs";
import { catchError, map, switchMap } from "rxjs/operators";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowExecutionsService } from "../../../service/user/workflow-executions/workflow-executions.service";
import { WorkflowRuntimeStatistics } from "../../../type/workflow-runtime-statistics";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";

// --- Public interfaces ---------------------------------------------------

export interface WorkflowSummary {
  wid: number;
  name: string;
  description?: string;
}

export interface OperatorSummary {
  operatorID: string;
  operatorType: string;
  displayName: string;
}

/**
 * Wire format for results that callers (workspace, evaluation operators,
 * etc.) drop into localStorage to make data available on the dashboard.
 *
 * Stored at key: `texera.workflow.results.{wid}`
 */
export interface WorkflowResultsSnapshot {
  wid: number;
  workflowName?: string;
  capturedAt: number;
  operators: Array<{
    operatorID: string;
    operatorName?: string;
    operatorType?: string;
    /** First N rows of tuple output. */
    columns?: string[];
    rows?: (string | number | null)[][];
    /** Scalar metrics keyed by name (e.g. {accuracy: 0.967, f1: 0.94}). */
    metrics?: Record<string, number | string>;
  }>;
}

export interface OperatorBundle {
  operator: OperatorSummary;
  /** Snapshot rows/columns from localStorage if present. */
  snapshot?: {
    columns: string[];
    rows: (string | number | null)[][];
  };
  /** Scalar metrics from localStorage if present. */
  metrics?: Record<string, number | string>;
  /** Runtime stats from REST. Undefined if no execution has happened. */
  stats?: WorkflowRuntimeStatistics;
}

export interface WorkflowDataBundle {
  workflow: WorkflowSummary;
  operators: OperatorBundle[];
  /** True if the workflow has at least one executed run (stats found). */
  hasRunStats: boolean;
  /** True if localStorage contains saved results for this workflow. */
  hasSavedResults: boolean;
  /** When the snapshot was captured (epoch ms), if any. */
  snapshotCapturedAt?: number;
}

// --- Service -------------------------------------------------------------

@Injectable({ providedIn: "root" })
export class WorkflowDataService {
  constructor(
    private workflowPersist: WorkflowPersistService,
    private executions: WorkflowExecutionsService
  ) {}

  listWorkflows(): Observable<WorkflowSummary[]> {
    return this.workflowPersist.retrieveWorkflowsBySessionUser().pipe(
      map((entries: DashboardWorkflow[]) =>
        entries
          .filter(e => e.workflow.wid !== undefined)
          .map(e => ({
            wid: e.workflow.wid as number,
            name: e.workflow.name,
            description: e.workflow.description,
          }))
      ),
      catchError(() => of([] as WorkflowSummary[]))
    );
  }

  /**
   * Returns the complete data bundle for a workflow — operators, optional
   * stored snapshot, optional runtime stats. Designed so the modal can call
   * once per workflow selection and render everything available.
   */
  getWorkflowData(wf: WorkflowSummary): Observable<WorkflowDataBundle> {
    const operators$ = this.workflowPersist.retrieveWorkflow(wf.wid).pipe(
      map(workflow => parseOperators(workflow.content)),
      catchError(() => of([] as OperatorSummary[]))
    );

    const stats$ = this.executions.retrieveLatestWorkflowExecution(wf.wid).pipe(
      switchMap(entry => {
        if (!entry || entry.eId === undefined || entry.cuId === undefined) {
          return of([] as WorkflowRuntimeStatistics[]);
        }
        return this.executions
          .retrieveWorkflowRuntimeStatistics(wf.wid, entry.eId, entry.cuId)
          .pipe(catchError(() => of([] as WorkflowRuntimeStatistics[])));
      }),
      map(rows => latestPerOperator(rows)),
      catchError(() => of(new Map<string, WorkflowRuntimeStatistics>()))
    );

    return forkJoin({ operators: operators$, stats: stats$ }).pipe(
      map(({ operators, stats }) => {
        const snapshot = readSnapshot(wf.wid);
        const snapshotByOp = new Map(
          (snapshot?.operators ?? []).map(o => [o.operatorID, o] as const)
        );
        const operatorBundles: OperatorBundle[] = operators.map(op => {
          const snap = snapshotByOp.get(op.operatorID);
          return {
            operator: op,
            snapshot: snap?.rows && snap.columns
              ? { columns: snap.columns, rows: snap.rows as (string | number | null)[][] }
              : undefined,
            metrics: snap?.metrics,
            stats: stats.get(op.operatorID),
          };
        });
        return {
          workflow: wf,
          operators: operatorBundles,
          hasRunStats: stats.size > 0,
          hasSavedResults: !!snapshot,
          snapshotCapturedAt: snapshot?.capturedAt,
        };
      })
    );
  }
}

// --- Helpers (exported for use by the modal) -----------------------------

/**
 * Reads cached operator results from localStorage. Two key conventions are
 * supported:
 *
 *   New (per-operator): `texera.results.{wid}.{opId}` — written by
 *     DashboardResultCacheService as the Result Panel receives data over
 *     WebSocket. Value shape: { columns, rows, timestamp }.
 *
 *   Legacy (bundle): `texera.workflow.results.{wid}` — single
 *     WorkflowResultsSnapshot for the whole workflow. Used by any caller
 *     that wants to write a pre-aggregated bundle.
 *
 * Both are merged into one WorkflowResultsSnapshot for the modal.
 */
export function readSnapshot(wid: number): WorkflowResultsSnapshot | undefined {
  const operatorsFromCache = readPerOperatorCache(wid);
  const legacyBundle = readLegacyBundle(wid);

  if (operatorsFromCache.length === 0 && !legacyBundle) {
    return undefined;
  }

  // Merge: cache entries take precedence over legacy bundle on opId clash.
  const merged = new Map<string, WorkflowResultsSnapshot["operators"][number]>();
  if (legacyBundle) {
    for (const op of legacyBundle.operators) merged.set(op.operatorID, op);
  }
  for (const op of operatorsFromCache) merged.set(op.operatorID, op);

  let capturedAt = legacyBundle?.capturedAt ?? 0;
  for (const op of operatorsFromCache) {
    // captured-at is tracked at the per-op level too
    const t = (op as any).__ts ?? 0;
    if (t > capturedAt) capturedAt = t;
  }

  return {
    wid,
    capturedAt: capturedAt || Date.now(),
    operators: Array.from(merged.values()),
  };
}

function readPerOperatorCache(wid: number): Array<WorkflowResultsSnapshot["operators"][number] & { __ts?: number }> {
  const prefix = `texera.results.${wid}.`;
  const out: Array<WorkflowResultsSnapshot["operators"][number] & { __ts?: number }> = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key || !key.startsWith(prefix)) continue;
      const opId = key.slice(prefix.length);
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object") continue;
        const columns: string[] = Array.isArray(parsed.columns) ? parsed.columns : [];
        const rows: (string | number | null)[][] = Array.isArray(parsed.rows) ? parsed.rows : [];
        const ts = parsed.timestamp ? Date.parse(parsed.timestamp) : 0;
        out.push({
          operatorID: opId,
          columns,
          rows,
          metrics: extractMetricsFromTable(columns, rows),
          __ts: ts,
        });
      } catch {
        // skip malformed entry
      }
    }
  } catch {
    // localStorage unavailable — give up
  }
  return out;
}

function readLegacyBundle(wid: number): WorkflowResultsSnapshot | undefined {
  try {
    const raw = localStorage.getItem(`texera.workflow.results.${wid}`);
    if (!raw) return undefined;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && Array.isArray(parsed.operators)) {
      return parsed as WorkflowResultsSnapshot;
    }
  } catch {
    // ignore
  }
  return undefined;
}

/**
 * Heuristically extract scalar metrics from a result table:
 *
 *   - Single-row table: every numeric column becomes a metric. This catches
 *     evaluation operators that output one row like {accuracy: 0.96, f1: 0.94}.
 *   - Two-column name/value table (≤ 20 rows): each row becomes a metric. This
 *     catches aggregations like {model: "RF", score: 0.94}.
 *
 * Returns undefined when no shape matches — the rows are still available for
 * Table / Bar / Donut widgets that consume the full table.
 */
function extractMetricsFromTable(
  columns: string[],
  rows: (string | number | null)[][]
): Record<string, number | string> | undefined {
  if (rows.length === 0 || columns.length === 0) return undefined;

  if (rows.length === 1) {
    const metrics: Record<string, number | string> = {};
    columns.forEach((col, i) => {
      const v = rows[0][i];
      if (v === null || v === undefined) return;
      const asNum = typeof v === "number" ? v : Number(v);
      if (!isNaN(asNum) && typeof v !== "string") {
        metrics[col] = asNum;
      } else if (typeof v === "string" && !isNaN(asNum)) {
        metrics[col] = asNum;
      } else if (typeof v === "string") {
        // Skip non-numeric strings — usually IDs, not interesting metrics.
      }
    });
    return Object.keys(metrics).length > 0 ? metrics : undefined;
  }

  if (columns.length === 2 && rows.length <= 20) {
    const looksLikeNameValue = rows.every(r => {
      const name = r[0];
      const value = r[1];
      const nameOk = typeof name === "string" || typeof name === "number";
      const valNum = typeof value === "number" ? value : Number(value as any);
      return nameOk && !isNaN(valNum);
    });
    if (looksLikeNameValue) {
      const metrics: Record<string, number | string> = {};
      rows.forEach(r => {
        const num = typeof r[1] === "number" ? r[1] : Number(r[1] as any);
        if (!isNaN(num)) {
          metrics[String(r[0])] = num;
        }
      });
      return Object.keys(metrics).length > 0 ? metrics : undefined;
    }
  }

  return undefined;
}

export function formatStatValue(
  value: number | undefined,
  kind: "int" | "bytes" | "nanos"
): string {
  if (value === undefined || value === null || isNaN(value)) return "—";
  if (kind === "int") return Number(value).toLocaleString();
  if (kind === "bytes") {
    if (value < 1024) return `${value} B`;
    if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
    if (value < 1024 * 1024 * 1024) return `${(value / 1024 / 1024).toFixed(1)} MB`;
    return `${(value / 1024 / 1024 / 1024).toFixed(2)} GB`;
  }
  // nanos
  if (value < 1000) return `${value} ns`;
  if (value < 1_000_000) return `${(value / 1000).toFixed(1)} µs`;
  if (value < 1_000_000_000) return `${(value / 1_000_000).toFixed(1)} ms`;
  return `${(value / 1_000_000_000).toFixed(2)} s`;
}

function parseOperators(rawContent: unknown): OperatorSummary[] {
  if (!rawContent) return [];
  const parsed: any = typeof rawContent === "string" ? safeParse(rawContent as string) : rawContent;
  const ops = parsed?.operators ?? [];
  return ops
    .map((op: any) => ({
      operatorID: op.operatorID ?? op.operatorId ?? "",
      operatorType: op.operatorType ?? "Operator",
      displayName: op.customDisplayName?.trim() || prettifyType(op.operatorType ?? "Operator"),
    }))
    .filter((o: OperatorSummary) => o.operatorID.length > 0);
}

function safeParse(raw: string): any {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function prettifyType(type: string): string {
  if (!type) return "Operator";
  return type.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/_/g, " ");
}

function latestPerOperator(
  rows: WorkflowRuntimeStatistics[]
): Map<string, WorkflowRuntimeStatistics> {
  const out = new Map<string, WorkflowRuntimeStatistics>();
  for (const r of rows) {
    const existing = out.get(r.operatorId);
    if (!existing || r.timestamp >= existing.timestamp) {
      out.set(r.operatorId, r);
    }
  }
  return out;
}
