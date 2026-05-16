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

export function readSnapshot(wid: number): WorkflowResultsSnapshot | undefined {
  try {
    const raw = localStorage.getItem(`texera.workflow.results.${wid}`);
    if (!raw) return undefined;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object" && Array.isArray(parsed.operators)) {
      return parsed as WorkflowResultsSnapshot;
    }
  } catch {
    // ignore — treat as no snapshot
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
