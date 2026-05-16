/**
 * Fetches the data the Dashboard Visualizer needs from Texera's REST APIs:
 * list of user workflows, list of operators in a workflow (parsed from its
 * persisted content), and the latest runtime statistics for each operator.
 *
 * Texera does not expose operator tuple data via REST (results live in the
 * WebSocket cache during a workspace session). What is persisted and
 * REST-queryable is per-operator runtime statistics: tuple counts, sizes,
 * processing times, worker counts. The dashboard renders those.
 */

import { Injectable } from "@angular/core";
import { HttpClient, HttpParams } from "@angular/common/http";
import { Observable, forkJoin, of } from "rxjs";
import { catchError, map, switchMap } from "rxjs/operators";
import { AppSettings } from "../../../../common/app-setting";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowExecutionsService } from "../../../service/user/workflow-executions/workflow-executions.service";
import { WorkflowRuntimeStatistics } from "../../../type/workflow-runtime-statistics";
import { DashboardWorkflow } from "../../../type/dashboard-workflow.interface";
import {
  BarConfig,
  DonutConfig,
  HBarConfig,
  MetricConfig,
  TableConfig,
  WidgetConfig,
  WidgetSource,
  WidgetType,
} from "./dashboard.types";

export interface OperatorSummary {
  operatorID: string;
  operatorType: string;
  /** A user-friendly name derived from the operator's customDisplayName or type. */
  displayName: string;
}

export interface WorkflowSummary {
  wid: number;
  name: string;
  description?: string;
}

export const METRIC_KEYS = [
  "outputTupleCount",
  "inputTupleCount",
  "outputTupleSize",
  "inputTupleSize",
  "totalDataProcessingTime",
  "totalIdleTime",
  "numberOfWorkers",
] as const;

export type MetricKey = (typeof METRIC_KEYS)[number];

export const METRIC_LABELS: Record<MetricKey, string> = {
  outputTupleCount: "Output tuples",
  inputTupleCount: "Input tuples",
  outputTupleSize: "Output size (bytes)",
  inputTupleSize: "Input size (bytes)",
  totalDataProcessingTime: "Processing time (ns)",
  totalIdleTime: "Idle time (ns)",
  numberOfWorkers: "Workers",
};

export const METRIC_FORMAT: Record<MetricKey, "int" | "bytes" | "nanos"> = {
  outputTupleCount: "int",
  inputTupleCount: "int",
  outputTupleSize: "bytes",
  inputTupleSize: "bytes",
  totalDataProcessingTime: "nanos",
  totalIdleTime: "nanos",
  numberOfWorkers: "int",
};

@Injectable({ providedIn: "root" })
export class WorkflowDataService {
  constructor(
    private http: HttpClient,
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
   * Parses the workflow's content JSON to extract operator IDs and types.
   * Returns [] if the workflow has no content yet.
   */
  listOperators(wid: number): Observable<OperatorSummary[]> {
    return this.workflowPersist.retrieveWorkflow(wid).pipe(
      map(workflow => {
        const raw = workflow.content as unknown;
        if (!raw) return [];
        // content may be an object or JSON string depending on backend version
        const parsed: any = typeof raw === "string" ? safeParse(raw as string) : raw;
        const ops = parsed?.operators ?? [];
        return ops.map((op: any) => ({
          operatorID: op.operatorID ?? op.operatorId ?? "",
          operatorType: op.operatorType ?? "Operator",
          displayName: op.customDisplayName?.trim() || prettifyType(op.operatorType ?? "Operator"),
        })).filter((o: OperatorSummary) => o.operatorID.length > 0);
      }),
      catchError(() => of([] as OperatorSummary[]))
    );
  }

  /**
   * Returns runtime statistics for the latest execution of `wid`. The result
   * is the *latest snapshot per operator* (the stats endpoint returns one row
   * per (operator, timestamp) tick, so we take the latest row per operator).
   */
  getLatestStats(wid: number): Observable<Map<string, WorkflowRuntimeStatistics>> {
    return this.executions.retrieveLatestWorkflowExecution(wid).pipe(
      switchMap(entry => {
        if (!entry || entry.eId === undefined || entry.cuId === undefined) {
          return of([] as WorkflowRuntimeStatistics[]);
        }
        return this.executions
          .retrieveWorkflowRuntimeStatistics(wid, entry.eId, entry.cuId)
          .pipe(catchError(() => of([] as WorkflowRuntimeStatistics[])));
      }),
      map(rows => {
        // Keep the latest row per operator
        const out = new Map<string, WorkflowRuntimeStatistics>();
        for (const r of rows) {
          const existing = out.get(r.operatorId);
          if (!existing || r.timestamp >= existing.timestamp) {
            out.set(r.operatorId, r);
          }
        }
        return out;
      }),
      catchError(() => of(new Map<string, WorkflowRuntimeStatistics>()))
    );
  }

  /**
   * One-call helper: returns operators + latest stats for the workflow.
   */
  getWorkflowSnapshot(
    wid: number
  ): Observable<{ operators: OperatorSummary[]; stats: Map<string, WorkflowRuntimeStatistics> }> {
    return forkJoin({
      operators: this.listOperators(wid),
      stats: this.getLatestStats(wid),
    });
  }
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
  // CamelCase / PascalCase → spaced
  return type.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/_/g, " ");
}

/** Format a metric value for display. */
export function formatMetricValue(value: number, metric: MetricKey): string {
  const kind = METRIC_FORMAT[metric];
  if (value === null || value === undefined || isNaN(value)) return "—";
  if (kind === "int") return Number(value).toLocaleString();
  if (kind === "bytes") return formatBytes(value);
  if (kind === "nanos") return formatNanos(value);
  return String(value);
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function formatNanos(n: number): string {
  if (n < 1000) return `${n} ns`;
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)} µs`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)} ms`;
  return `${(n / 1_000_000_000).toFixed(2)} s`;
}

const CHART_PALETTE = [
  "#4cc9f0",
  "#7c5cff",
  "#52c41a",
  "#f5587b",
  "#ff9c6e",
  "#b37feb",
  "#fadb14",
  "#36cfc9",
];

/**
 * Given a widget type + workflow source descriptor + freshly-fetched
 * operators and stats, build the WidgetConfig that the widget renderer
 * consumes. Returns undefined if the source describes data we can't produce
 * (e.g. text widgets are never workflow-sourced).
 */
export function buildWidgetFromStats(
  type: WidgetType,
  source: WidgetSource,
  operators: OperatorSummary[],
  stats: Map<string, WorkflowRuntimeStatistics>
): WidgetConfig | undefined {
  if (source.kind !== "workflow") return undefined;
  const metric = source.metric as MetricKey;
  const metricLabel = METRIC_LABELS[metric] ?? source.metric;

  if (type === "metric") {
    if (!source.operatorId) return undefined;
    const row = stats.get(source.operatorId);
    const value = row ? Number((row as any)[metric] ?? 0) : 0;
    const config: MetricConfig = {
      title: `${source.operatorName ?? source.operatorId} · ${metricLabel}`,
      value: row ? formatMetricValue(value, metric) : "—",
      caption: row ? `From ${source.workflowName}` : "No execution data yet",
      color: CHART_PALETTE[0],
    };
    return { type: "metric", config };
  }

  // Chart types ("all-operators" scope): one bar/slice/row per operator.
  const items = operators
    .map((op, i) => {
      const row = stats.get(op.operatorID);
      const v = row ? Number((row as any)[metric] ?? 0) : 0;
      return { op, value: v, color: CHART_PALETTE[i % CHART_PALETTE.length] };
    })
    .filter(x => x.value > 0 || stats.has(x.op.operatorID));

  if (type === "bar") {
    const cfg: BarConfig = {
      title: `${metricLabel} per operator · ${source.workflowName}`,
      categories: items.map(x => x.op.displayName),
      series: [
        {
          name: metricLabel,
          color: CHART_PALETTE[0],
          values: items.map(x => x.value),
        },
      ],
      yAxisLabel: metricLabel,
    };
    return { type: "bar", config: cfg };
  }

  if (type === "hbar") {
    const cfg: HBarConfig = {
      title: `${metricLabel} per operator · ${source.workflowName}`,
      color: CHART_PALETTE[1],
      items: items.map(x => ({ label: x.op.displayName, value: x.value })),
    };
    return { type: "hbar", config: cfg };
  }

  if (type === "donut") {
    const cfg: DonutConfig = {
      title: `${metricLabel} share · ${source.workflowName}`,
      segments: items.map(x => ({ label: x.op.displayName, value: x.value, color: x.color })),
      centerLabel: `${items.length} ops`,
    };
    return { type: "donut", config: cfg };
  }

  if (type === "table") {
    // All operators × all metrics
    const cfg: TableConfig = {
      title: `Stats · ${source.workflowName}`,
      columns: ["Operator", ...METRIC_KEYS.map(k => METRIC_LABELS[k])],
      rows: operators.map(op => {
        const row = stats.get(op.operatorID);
        return [
          op.displayName,
          ...METRIC_KEYS.map(k => (row ? formatMetricValue(Number((row as any)[k] ?? 0), k) : "—")),
        ];
      }),
    };
    return { type: "table", config: cfg };
  }

  return undefined;
}
