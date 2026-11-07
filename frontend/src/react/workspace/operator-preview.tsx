/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { ExecutionState } from "../../core/workspace/types/execute-workflow.interface";
import type { RuntimeSnapshot, WorkflowRuntime } from "./workflow-runtime";
import type { WorkflowSession } from "./workflow-session";
import { ResultsRepository } from "../results/results-repository";
import {
  HeatmapView,
  formatMetricForView,
  heatmapViewTitle,
  rawMetricForView,
} from "../../core/workspace/service/heatmap/heatmap-scoring";
import { extractPerformanceMetrics } from "../../core/workspace/service/workflow-status/performance-metrics";
import { scoreToColor } from "../../core/workspace/service/heatmap/heatmap-color";
type PreviewSnapshot = Pick<
  RuntimeSnapshot,
  "state" | "errors" | "console" | "results" | "materializedResults" | "compilation"
>;
export function formatPreviewCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  const text = typeof value === "object" ? JSON.stringify(value) : String(value);
  return text.length > 24 ? text.slice(0, 23) + "…" : text;
}
export function describePreview(
  id: string,
  snapshot: PreviewSnapshot,
  outputs: number
): {
  kind: "error" | "table" | "chart" | "none";
  summary: string;
  detail?: string;
  columns: string[];
  moreColumns: number;
} {
  const base = { columns: [] as string[], moreColumns: 0 };
  const error = snapshot.errors
    .concat(Object.values(snapshot.compilation?.operatorErrors ?? {}))
    .find(error => error.operatorId === id);
  const consoleError = snapshot.console[id]?.find(message => message.msgType.name === "ERROR");
  if (error || consoleError)
    return { ...base, kind: "error", summary: "failed", detail: error?.message ?? consoleError?.title };
  const result = snapshot.results[id];
  if (result?.mode.type === "PaginationMode" && "totalNumTuples" in result) {
    const columns = (Object.values(snapshot.compilation?.operatorOutputSchemas[id] ?? {})[0] ?? []).map(
        attribute => attribute.attributeName
      ),
      count = result.totalNumTuples;
    return {
      kind: "table",
      summary:
        `${count.toLocaleString()} ${count === 1 ? "row" : "rows"} · ${columns.length} ${columns.length === 1 ? "column" : "columns"}` +
        (outputs > 1 ? ` · first of ${outputs} outputs` : ""),
      columns: columns.slice(0, 6),
      moreColumns: Math.max(0, columns.length - 6),
    };
  }
  if (result) return { ...base, kind: "chart", summary: "chart" };
  return {
    ...base,
    kind: "none",
    summary: "no result",
    detail:
      snapshot.state === ExecutionState.Uninitialized
        ? "Run the workflow to see its result."
        : "This run did not keep this operator's result. Turn on its eye icon to keep it next time.",
  };
}
export function OperatorPreview({
  id,
  runtime,
  session,
  onPin,
}: {
  id: string;
  runtime: WorkflowRuntime;
  session: WorkflowSession;
  onPin: () => void;
}) {
  const snapshot = useSyncExternalStore(runtime.subscribe, runtime.getSnapshot);
  const operator = session.graph.hasOperator(id) ? session.graph.getOperator(id) : undefined;
  const preview = describePreview(id, snapshot, operator?.outputPorts.length ?? 1),
    update = snapshot.results[id];
  const repository = useMemo(() => new ResultsRepository(runtime.socket), [runtime]);
  const [rows, setRows] = useState<readonly object[]>(),
    [error, setError] = useState<string>();
  useEffect(() => {
    if (preview.kind !== "table") return;
    let active = true;
    void repository.page(id, { pageIndex: 1, pageSize: 5, columnLimit: 6, portIndex: 0 }).then(
      page => {
        if (active) {
          setRows(page.table);
          setError(undefined);
        }
      },
      reason => {
        if (active) setError(reason instanceof Error ? reason.message : "Preview unavailable.");
      }
    );
    return () => {
      active = false;
    };
  }, [repository, id, update, preview.kind]);
  return (
    <section aria-label="Operator result preview">
      <strong>{operator?.customDisplayName ?? operator?.operatorType ?? id}</strong>
      <p>{preview.summary}</p>
      {preview.detail && <p>{preview.detail}</p>}
      {error && <p role="alert">{error}</p>}
      {preview.kind === "table" &&
        (rows ? (
          <table>
            <thead>
              <tr>
                {preview.columns.map(column => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index}>
                  {preview.columns.map(column => (
                    <td
                      key={column}
                      title={String((row as Record<string, unknown>)[column] ?? "")}>
                      {formatPreviewCell((row as Record<string, unknown>)[column])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>Loading preview…</p>
        ))}
      {preview.moreColumns > 0 && <p>+{preview.moreColumns} more columns</p>}
      <button onClick={onPin}>Pin result window</button>
    </section>
  );
}
export function HeatmapLegend({ view, snapshot }: { view: HeatmapView; snapshot: RuntimeSnapshot | undefined }) {
  const raw = Object.values(snapshot?.operators ?? {})
    .filter(stats => !stats.reusedFromCache)
    .map(stats => rawMetricForView(extractPerformanceMetrics(stats), view))
    .filter((value): value is number => value !== undefined && Number.isFinite(value));
  return (
    <aside
      className="react-heatmap-legend"
      aria-label="Heat map legend">
      <strong>{heatmapViewTitle(view)}</strong>
      <div
        style={{
          height: 10,
          background: `linear-gradient(to right,${scoreToColor(0)},${scoreToColor(0.5)},${scoreToColor(1)})`,
        }}
      />
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <span>{raw.length ? formatMetricForView(Math.min(...raw), view) : "—"}</span>
        <span>{raw.length ? formatMetricForView(Math.max(...raw), view) : "—"}</span>
      </div>
    </aside>
  );
}
