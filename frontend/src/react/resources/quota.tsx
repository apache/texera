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
import { useCallback, useMemo, useState } from "react";
import type { ExecutionQuota } from "./api";
import { Dialog, LoadState, Notices, useOperation, useResource, type ResourcePageProps } from "./pages";

export function formatQuotaSize(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${Number((value / 1024 ** index).toFixed(2))} ${units[index]}`;
}

/** Last-year upload counts, using the same month/group threshold as the existing quota view. */
export function aggregateQuotaTrend(timestamps: number[], now = new Date()): [string, number][] {
  const cutoff = new Date(now);
  cutoff.setFullYear(cutoff.getFullYear() - 1);
  const counts = new Map<string, number>();
  for (const timestamp of timestamps) {
    const date = new Date(timestamp);
    if (!Number.isFinite(date.getTime()) || date < cutoff) continue;
    const key = date.toISOString().slice(0, 10);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  const days = [...counts].sort(([left], [right]) => left.localeCompare(right));
  if (days.length < 8) return days;
  if (new Set(days.map(([date]) => date.slice(0, 7))).size >= 3) {
    const months = new Map<string, number>();
    days.forEach(([date, count]) => {
      const key = date.slice(0, 7);
      months.set(key, (months.get(key) ?? 0) + count);
    });
    return [...months];
  }
  const start = Date.parse(days[0][0]);
  const end = Date.parse(days.at(-1)![0]);
  const groupWidth = Math.max(1, Math.ceil((end - start) / 86400000 / 5));
  const groups = new Map<string, number>();
  for (const [date, count] of days) {
    const group = Math.floor((Date.parse(date) - start) / 86400000 / groupWidth);
    const key = new Date(start + group * groupWidth * 86400000).toISOString().slice(0, 10);
    groups.set(key, (groups.get(key) ?? 0) + count);
  }
  return [...groups];
}

function TrendChart({ title, points }: { title: string; points: [string, number][] }) {
  const maximum = Math.max(1, ...points.map(([, count]) => count));
  const path = points
    .map(
      ([, count], index) => `${30 + (index * 440) / Math.max(points.length - 1, 1)},${165 - (count * 130) / maximum}`
    )
    .join(" ");
  return (
    <figure className="resource-chart">
      <figcaption>{title}</figcaption>
      <svg
        viewBox="0 0 500 210"
        role="img"
        aria-label={title}>
        <path
          d="M30 20V165H480"
          fill="none"
          stroke="#8a9cad"
        />
        <text
          x="6"
          y="25">
          {maximum}
        </text>
        <text
          x="10"
          y="169">
          0
        </text>
        <polyline
          points={path}
          fill="none"
          stroke="#377ca5"
          strokeWidth="3"
        />
        {points.map(([date, count], index) => (
          <g key={date}>
            <circle
              cx={30 + (index * 440) / Math.max(points.length - 1, 1)}
              cy={165 - (count * 130) / maximum}
              r="4"
              fill="#377ca5">
              <title>
                {date}: {count}
              </title>
            </circle>
            {(index === 0 || index === points.length - 1) && (
              <text
                x={30 + (index * 440) / Math.max(points.length - 1, 1)}
                y="190"
                textAnchor={index === 0 ? "start" : "end"}>
                {date}
              </text>
            )}
          </g>
        ))}
        {!points.length && (
          <text
            x="250"
            y="95"
            textAnchor="middle">
            No uploads in the past year
          </text>
        )}
      </svg>
      <details>
        <summary>View chart data</summary>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Uploads</th>
            </tr>
          </thead>
          <tbody>
            {points.map(([date, count]) => (
              <tr key={date}>
                <td>{date}</td>
                <td>{count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

function DistributionChart({ datasets }: { datasets: { name: string; size: number }[] }) {
  const total = datasets.reduce((sum, item) => sum + Math.max(0, item.size), 0);
  let angle = -Math.PI / 2;
  const colors = ["#387ca5", "#e4a34d", "#6e9d73", "#a37db6", "#dd7770", "#5caaac"];
  return (
    <figure className="resource-chart">
      <figcaption>Dataset Size Distribution</figcaption>
      <svg
        viewBox="0 0 500 210"
        role="img"
        aria-label="Dataset Size Distribution">
        {total <= 0 ? (
          <text
            x="250"
            y="100"
            textAnchor="middle">
            No dataset storage
          </text>
        ) : (
          datasets.map((dataset, index) => {
            const fraction = Math.max(0, dataset.size) / total;
            const end = angle + fraction * Math.PI * 2;
            const path = `M250,100 L${250 + 85 * Math.cos(angle)},${100 + 85 * Math.sin(angle)} A85,85 0 ${fraction > 0.5 ? 1 : 0},1 ${250 + 85 * Math.cos(end)},${100 + 85 * Math.sin(end)} Z`;
            angle = end;
            return fraction >= 0.999999 ? (
              <circle
                key={index}
                cx="250"
                cy="100"
                r="85"
                fill={colors[index % colors.length]}>
                <title>
                  {dataset.name}: {formatQuotaSize(dataset.size)}
                </title>
              </circle>
            ) : (
              <path
                key={index}
                d={path}
                fill={colors[index % colors.length]}>
                <title>
                  {dataset.name}: {formatQuotaSize(dataset.size)}
                </title>
              </path>
            );
          })
        )}
      </svg>
      <ul className="resource-chart-legend">
        {datasets.map((dataset, index) => (
          <li key={index}>
            <span style={{ background: colors[index % colors.length] }} />
            {dataset.name}: {formatQuotaSize(dataset.size)}
          </li>
        ))}
      </ul>
    </figure>
  );
}

export function QuotaPage({ resources, onNotice, userId }: ResourcePageProps & { userId?: number }) {
  const load = useCallback((signal: AbortSignal) => resources.quota(userId, { signal }), [resources, userId]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [deleting, setDeleting] = useState<ExecutionQuota>();
  const [descending, setDescending] = useState(false);
  const datasetTrend = useMemo(
    () => aggregateQuotaTrend(state.data?.datasets.map(item => item.creationTime) ?? []),
    [state.data]
  );
  const workflowTrend = useMemo(
    () => aggregateQuotaTrend(state.data?.workflows.map(item => item.creationTime) ?? []),
    [state.data]
  );
  const executionBytes = (item: ExecutionQuota) => item.resultBytes + item.logBytes + item.runTimeStatsBytes;
  const groups = new Map<number, { name: string; executions: ExecutionQuota[] }>();
  for (const item of state.data?.executions ?? []) {
    const group = groups.get(item.workflowId) ?? { name: item.workflowName, executions: [] };
    group.executions.push(item);
    groups.set(item.workflowId, group);
  }
  return (
    <main className="resource-page">
      <header>
        <h1>{userId === undefined ? "Your quota" : `Quota for user ${userId}`}</h1>
        <button
          type="button"
          onClick={state.refresh}>
          Refresh
        </button>
      </header>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      {state.data && (
        <>
          <section className="resource-panel resource-summary">
            <p>Uploaded datasets: {state.data.datasets.length}</p>
            <p>Dataset storage: {formatQuotaSize(state.data.datasets.reduce((sum, item) => sum + item.size, 0))}</p>
            <p>Created workflows: {state.data.workflows.length}</p>
            <p>Accessible workflows: {state.data.accessWorkflows.length}</p>
            <p>
              Execution storage:{" "}
              {formatQuotaSize(state.data.executions.reduce((sum, item) => sum + executionBytes(item), 0))}
            </p>
          </section>
          <section className="resource-charts">
            <DistributionChart datasets={state.data.datasets} />
            <TrendChart
              title="Dataset Upload Overview"
              points={datasetTrend}
            />
            <TrendChart
              title="Workflow Upload Overview"
              points={workflowTrend}
            />
          </section>
          <section className="resource-panel">
            <h2>Datasets</h2>
            {state.data.datasets.length ? (
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Size</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {state.data.datasets.map(item => (
                    <tr key={item.did}>
                      <td>{item.name}</td>
                      <td>{formatQuotaSize(item.size)}</td>
                      <td>{new Date(item.creationTime).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No datasets.</p>
            )}
          </section>
          <section className="resource-panel">
            <h2>Execution collections</h2>
            <button
              type="button"
              onClick={() => setDescending(!descending)}>
              Sort collections by size {descending ? "↓" : "↑"}
            </button>
            {groups.size ? (
              [...groups].map(([workflowId, group]) => (
                <details
                  open
                  key={workflowId}>
                  <summary>
                    {group.name} ({workflowId})
                  </summary>
                  <table>
                    <thead>
                      <tr>
                        <th>Execution</th>
                        <th>Result</th>
                        <th>Runtime statistics</th>
                        <th>Logs</th>
                        <th>Total</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...group.executions]
                        .sort((left, right) => (executionBytes(left) - executionBytes(right)) * (descending ? -1 : 1))
                        .map(item => (
                          <tr key={item.eid}>
                            <td>{item.eid}</td>
                            <td>{formatQuotaSize(item.resultBytes)}</td>
                            <td>{formatQuotaSize(item.runTimeStatsBytes)}</td>
                            <td>{formatQuotaSize(item.logBytes)}</td>
                            <td>{formatQuotaSize(executionBytes(item))}</td>
                            <td>
                              <button
                                type="button"
                                aria-label={`Delete execution collection ${item.eid}`}
                                disabled={operation.busy}
                                onClick={() => {
                                  operation.clear();
                                  setDeleting(item);
                                }}>
                                Delete
                              </button>
                            </td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </details>
              ))
            ) : (
              <p>No execution collections.</p>
            )}
          </section>
        </>
      )}
      {deleting && (
        <Dialog
          title={`Delete execution collection ${deleting.eid}?`}
          busy={operation.busy}
          close={() => setDeleting(undefined)}>
          <p>This permanently deletes the stored results, runtime statistics and logs for this execution.</p>
          <footer>
            <button
              type="button"
              disabled={operation.busy}
              onClick={() => setDeleting(undefined)}>
              Cancel
            </button>
            <button
              type="button"
              disabled={operation.busy}
              onClick={() =>
                void operation.run(
                  () => resources.deleteExecutionCollection(deleting.eid, userId !== undefined),
                  () => {
                    state.setData(
                      current =>
                        current && {
                          ...current,
                          executions: current.executions.filter(item => item.eid !== deleting.eid),
                        }
                    );
                    setDeleting(undefined);
                  },
                  "Execution collection deleted."
                )
              }>
              Delete
            </button>
          </footer>
        </Dialog>
      )}
    </main>
  );
}
