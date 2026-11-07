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

import { useEffect, useMemo, useState } from "react";
import Fuse from "fuse.js";
import type { ApiClient } from "../core/api-client";
import type { WorkflowRuntimeStatistics } from "../../core/dashboard/type/workflow-runtime-statistics";
import type { WorkflowSocket } from "../workspace/workflow-socket";
import { ResultsRepository, type WorkflowExecutionsEntry } from "./results-repository";
import "./results.css";
const STATUS = ["Initializing", "Running", "Paused", "Completed", "Failed", "Killed"];
export function searchExecutions(
  entries: readonly WorkflowExecutionsEntry[],
  query: string
): WorkflowExecutionsEntry[] {
  if (!query.trim()) return [...entries];
  const conditions = [...new Set(query.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g))];
  const paths = conditions.map(condition => {
    const colon = condition.indexOf(":");
    if (colon < 0) return { $path: ["name"], $val: condition.replace(/^"|"$/g, "") };
    const field = condition.slice(0, colon),
      value = condition
        .slice(colon + 1)
        .replace(/^"|"$/g, "")
        .toLowerCase();
    if (condition.indexOf(":", colon + 1) >= 0) throw new Error("Please check the format of the search query.");
    if (!["user", "status"].includes(field)) throw new Error("Cannot search by " + field);
    if (field === "status") {
      const status = STATUS.findIndex(status => status.toLowerCase() === value);
      if (status < 0) throw new Error("Status " + value + " is not available to execution.");
      return { $path: ["status"], $val: String(status) };
    }
    return { $path: ["userName"], $val: value };
  });
  return new Fuse(entries, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["name", "userName", "status"],
  })
    .search({ $and: paths })
    .map(result => result.item);
}
export interface RuntimeSeries {
  operatorId: string;
  name: string;
  points: { x: number; y: number }[];
}
export function runtimeSeries(stats: readonly WorkflowRuntimeStatistics[], metric: string): RuntimeSeries[] {
  const grouped = new Map<string, RuntimeSeries>(),
    initial = stats[0]?.timestamp ?? 0;
  for (const stat of stats) {
    if (!stat.operatorId || stat.operatorId.startsWith("sink")) continue;
    const value = Number(stat[metric]),
      x = (stat.timestamp - initial) / 1000;
    if (!Number.isFinite(value) || !Number.isFinite(x)) continue;
    const series = grouped.get(stat.operatorId) ?? {
      operatorId: stat.operatorId,
      name: stat.operatorId.split("-")[0] + "-" + stat.operatorId.slice(-6),
      points: [],
    };
    series.points.push({
      x,
      y: value / (/^total(?:DataProcessing|ControlProcessing|Idle)Time$/.test(metric) ? 1e9 : 1),
    });
    grouped.set(stat.operatorId, series);
  }
  return [...grouped.values()];
}
export interface ExecutionHistoryPageProps {
  api: ApiClient;
  wid: number;
  readonly?: boolean;
  onOpenExecution?: (entry: WorkflowExecutionsEntry) => void;
  onOpenWorkflow?: (wid: number, vid?: number) => void;
}
function message(reason: unknown) {
  return reason instanceof Error ? reason.message : "Execution request failed.";
}
export function ExecutionHistoryPage({
  api,
  wid,
  readonly = false,
  onOpenExecution,
  onOpenWorkflow,
}: ExecutionHistoryPageProps) {
  const repository = useMemo(() => new ResultsRepository({} as WorkflowSocket, api), [api]);
  const [entries, setEntries] = useState<WorkflowExecutionsEntry[]>([]),
    [loading, setLoading] = useState(true),
    [error, setError] = useState(""),
    [selected, setSelected] = useState<number[]>([]),
    [query, setQuery] = useState(""),
    [sort, setSort] = useState("startingTime"),
    [ascending, setAscending] = useState(false),
    [page, setPage] = useState(1),
    [size, setSize] = useState(10),
    [busy, setBusy] = useState(false),
    [deleting, setDeleting] = useState<number[]>(),
    [renaming, setRenaming] = useState<WorkflowExecutionsEntry>(),
    [name, setName] = useState(""),
    [statistics, setStatistics] = useState<{ entry: WorkflowExecutionsEntry; stats: WorkflowRuntimeStatistics[] }>(),
    [summary, setSummary] = useState(false);
  useEffect(() => {
    let active = true;
    setLoading(true);
    setSelected([]);
    void repository.history(wid).then(
      value => {
        if (active) {
          setEntries(value);
          setLoading(false);
          setError("");
        }
      },
      reason => {
        if (active) {
          setError(message(reason));
          setLoading(false);
        }
      }
    );
    return () => {
      active = false;
    };
  }, [repository, wid]);
  let filtered: WorkflowExecutionsEntry[] = [],
    searchError = "";
  try {
    filtered = searchExecutions(entries, query);
  } catch (reason) {
    searchError = message(reason);
  }
  filtered.sort((a, b) => {
    const av = a[sort as keyof WorkflowExecutionsEntry],
      bv = b[sort as keyof WorkflowExecutionsEntry];
    const compared =
      typeof av === "number" && typeof bv === "number"
        ? av - bv
        : String(av).toLowerCase().localeCompare(String(bv).toLowerCase());
    return ascending ? compared : -compared;
  });
  const pages = Math.max(1, Math.ceil(filtered.length / size)),
    visible = filtered.slice((page - 1) * size, page * size);
  useEffect(() => {
    if (page > pages) setPage(pages);
  }, [page, pages]);
  async function action(task: () => Promise<void>) {
    setBusy(true);
    setError("");
    try {
      await task();
    } catch (reason) {
      setError(message(reason));
    } finally {
      setBusy(false);
    }
  }
  async function bookmarks(ids: readonly number[]) {
    const already = ids.every(id => entries.find(entry => entry.eId === id)?.bookmarked);
    await action(async () => {
      await repository.bookmarkExecutions(wid, ids, already);
      setEntries(current =>
        current.map(entry => (ids.includes(entry.eId) ? { ...entry, bookmarked: !already } : entry))
      );
    });
  }
  return (
    <section className="react-results execution-history">
      <h2>Execution history</h2>
      <div className="results-heading">
        <label>
          Search executions{" "}
          <input
            value={query}
            onChange={event => {
              setQuery(event.target.value);
              setPage(1);
            }}
            placeholder="name user:name status:completed"
          />
        </label>
        <label>
          Sort by{" "}
          <select
            value={sort}
            onChange={event => setSort(event.target.value)}>
            {[
              ["startingTime", "Start time"],
              ["completionTime", "Completion time"],
              ["name", "Name"],
              ["userName", "User"],
              ["status", "Status"],
            ].map(([value, label]) => (
              <option
                key={value}
                value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <button onClick={() => setAscending(!ascending)}>{ascending ? "Ascending" : "Descending"}</button>
        <button onClick={() => setSummary(!summary)}>{summary ? "Hide charts" : "Show charts"}</button>
      </div>
      {error && <p role="alert">{error}</p>}
      {searchError && <p role="alert">{searchError}</p>}
      {loading && <p role="status">Loading execution history…</p>}
      {!readonly && (
        <div>
          <button
            disabled={!selected.length || busy}
            onClick={() => void bookmarks([...selected])}>
            Toggle selected bookmarks
          </button>
          <button
            disabled={!selected.length || busy}
            onClick={() => setDeleting([...selected])}>
            Delete selected
          </button>
        </div>
      )}
      <div className="results-table-scroll">
        <table>
          <thead>
            <tr>
              <th>
                <input
                  aria-label="Select displayed executions"
                  type="checkbox"
                  checked={!!visible.length && visible.every(entry => selected.includes(entry.eId))}
                  onChange={event =>
                    setSelected(
                      event.target.checked
                        ? [...new Set([...selected, ...visible.map(entry => entry.eId)])]
                        : selected.filter(id => !visible.some(entry => entry.eId === id))
                    )
                  }
                />
              </th>
              <th>Name (ID)</th>
              <th>User</th>
              <th>Computing unit</th>
              <th>Warehouse</th>
              <th>Start time</th>
              <th>Completion time</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {visible.map(entry => (
              <tr key={entry.eId}>
                <td>
                  <input
                    type="checkbox"
                    aria-label={"Select execution " + entry.eId}
                    checked={selected.includes(entry.eId)}
                    onChange={event =>
                      setSelected(
                        event.target.checked ? [...selected, entry.eId] : selected.filter(id => id !== entry.eId)
                      )
                    }
                  />
                </td>
                <td>
                  <strong>{entry.name}</strong> ({entry.eId})
                </td>
                <td>{entry.userName}</td>
                <td>{entry.cuId}</td>
                <td>{entry.whId ?? "Shared default"}</td>
                <td>{new Date(entry.startingTime).toLocaleString()}</td>
                <td>{new Date(entry.completionTime).toLocaleString()}</td>
                <td>{STATUS[entry.status] ?? "Unknown"}</td>
                <td>
                  {!readonly && (
                    <>
                      <button
                        disabled={busy}
                        aria-label={(entry.bookmarked ? "Unbookmark" : "Bookmark") + " execution " + entry.eId}
                        onClick={() => void bookmarks([entry.eId])}>
                        {entry.bookmarked ? "★" : "☆"}
                      </button>
                      <button
                        disabled={busy}
                        onClick={() => {
                          setRenaming(entry);
                          setName(entry.name);
                        }}>
                        Rename
                      </button>
                      <button
                        disabled={busy}
                        onClick={() => setDeleting([entry.eId])}>
                        Delete
                      </button>
                    </>
                  )}
                  <button
                    disabled={busy}
                    onClick={() =>
                      void action(async () => {
                        const stats = await repository.runtimeStatistics(wid, entry.eId, entry.cuId);
                        setStatistics({ entry, stats });
                      })
                    }>
                    Runtime statistics {entry.eId}
                  </button>
                  {onOpenWorkflow && (
                    <button onClick={() => onOpenWorkflow(wid, entry.vId)}>Workflow version {entry.vId}</button>
                  )}
                  {onOpenExecution && (
                    <button onClick={() => onOpenExecution(entry)}>Open execution {entry.eId}</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!loading && !filtered.length && <p>No executions found.</p>}
      <footer className="result-pagination">
        <button
          disabled={page === 1}
          onClick={() => setPage(page - 1)}>
          Previous executions
        </button>
        <span>
          Page {page} of {pages} · {filtered.length} executions
        </span>
        <button
          disabled={page >= pages}
          onClick={() => setPage(page + 1)}>
          Next executions
        </button>
        <label>
          Executions per page{" "}
          <select
            value={size}
            onChange={event => {
              setSize(Number(event.target.value));
              setPage(1);
            }}>
            {[5, 10, 20, 30, 40].map(n => (
              <option key={n}>{n}</option>
            ))}
          </select>
        </label>
      </footer>
      {summary && <HistoryCharts entries={entries} />}
      {deleting && (
        <div className="result-modal-backdrop">
          <section
            role="dialog"
            aria-modal="true"
            aria-label="Delete executions"
            className="result-modal">
            <h3>Delete {deleting.length} execution(s)?</h3>
            <p>Execution results, logs, and statistics will be deleted.</p>
            <button
              disabled={busy}
              onClick={() =>
                void action(async () => {
                  const ids = [...deleting];
                  await repository.deleteExecutions(wid, ids);
                  setEntries(current => current.filter(entry => !ids.includes(entry.eId)));
                  setSelected(current => current.filter(id => !ids.includes(id)));
                  setDeleting(undefined);
                })
              }>
              Confirm deletion
            </button>
            <button
              disabled={busy}
              onClick={() => setDeleting(undefined)}>
              Cancel
            </button>
          </section>
        </div>
      )}
      {renaming && (
        <div className="result-modal-backdrop">
          <form
            role="dialog"
            aria-label="Rename execution"
            className="result-modal"
            onSubmit={event => {
              event.preventDefault();
              void action(async () => {
                const id = renaming.eId;
                if (name !== renaming.name) await repository.renameExecution(wid, id, name);
                setEntries(current => current.map(entry => (entry.eId === id ? { ...entry, name } : entry)));
                setRenaming(undefined);
              });
            }}>
            <label>
              Execution name{" "}
              <input
                value={name}
                onChange={event => setName(event.target.value)}
              />
            </label>
            <button disabled={busy || !name.trim()}>Save name</button>
            <button
              type="button"
              disabled={busy}
              onClick={() => setRenaming(undefined)}>
              Cancel
            </button>
          </form>
        </div>
      )}
      {statistics && (
        <RuntimeStatisticsDialog
          {...statistics}
          onClose={() => setStatistics(undefined)}
        />
      )}
    </section>
  );
}
const METRICS = [
  ["inputTupleCount", "Input tuple count"],
  ["inputTupleSize", "Input tuple size (bytes)"],
  ["outputTupleCount", "Output tuple count"],
  ["outputTupleSize", "Output tuple size (bytes)"],
  ["totalDataProcessingTime", "Data processing time (s)"],
  ["totalControlProcessingTime", "Control processing time (s)"],
  ["totalIdleTime", "Idle time (s)"],
  ["numberOfWorkers", "Number of workers"],
];
function RuntimeStatisticsDialog({
  entry,
  stats,
  onClose,
}: {
  entry: WorkflowExecutionsEntry;
  stats: WorkflowRuntimeStatistics[];
  onClose: () => void;
}) {
  const [metric, setMetric] = useState("inputTupleCount"),
    series = runtimeSeries(stats, metric);
  const maxX = Math.max(1, ...series.flatMap(s => s.points.map(p => p.x))),
    maxY = Math.max(1, ...series.flatMap(s => s.points.map(p => p.y)));
  const colors = ["#176ac4", "#ad3826", "#438c45", "#8e479b", "#bb7f20"];
  return (
    <div className="result-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label="Runtime statistics"
        className="result-modal">
        <h2>Runtime statistics · {entry.name}</h2>
        <label>
          Metric{" "}
          <select
            value={metric}
            onChange={event => setMetric(event.target.value)}>
            {METRICS.map(([key, label]) => (
              <option
                key={key}
                value={key}>
                {label}
              </option>
            ))}
          </select>
        </label>
        {!series.length ? (
          <p>No runtime statistics available.</p>
        ) : (
          <>
            <svg
              viewBox="0 0 800 360"
              role="img"
              aria-label="Operator runtime metric over time">
              <path
                d="M40 10V320H790"
                stroke="#666"
                fill="none"
              />
              {series.map((s, index) => (
                <g key={s.operatorId}>
                  <polyline
                    fill="none"
                    stroke={colors[index % colors.length]}
                    strokeWidth="2"
                    points={s.points.map(p => 40 + (p.x / maxX) * 740 + "," + (320 - (p.y / maxY) * 300)).join(" ")}
                  />
                  {s.points.map((p, i) => (
                    <circle
                      key={i}
                      cx={40 + (p.x / maxX) * 740}
                      cy={320 - (p.y / maxY) * 300}
                      r="3"
                      fill={colors[index % colors.length]}>
                      <title>
                        {s.name}: {p.y} at {p.x}s
                      </title>
                    </circle>
                  ))}
                </g>
              ))}
              <text
                x="360"
                y="350">
                Time (seconds)
              </text>
              <text
                x="45"
                y="20">
                {maxY.toPrecision(4)}
              </text>
            </svg>
            <ul>
              {series.map((s, i) => (
                <li
                  key={s.operatorId}
                  style={{ color: colors[i % colors.length] }}>
                  {s.name}: {s.points.at(-1)?.y}
                </li>
              ))}
            </ul>
            <details>
              <summary>Metric data</summary>
              <table>
                <thead>
                  <tr>
                    <th>Operator</th>
                    <th>Time (s)</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {series.flatMap(s =>
                    s.points.map((p, i) => (
                      <tr key={s.operatorId + ":" + i}>
                        <td>{s.name}</td>
                        <td>{p.x}</td>
                        <td>{p.y}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </details>
          </>
        )}
        <button onClick={onClose}>Close statistics</button>
      </section>
    </div>
  );
}
function HistoryCharts({ entries }: { entries: readonly WorkflowExecutionsEntry[] }) {
  const users = new Map<string, number>(),
    statuses = new Map<string, number>();
  for (const entry of entries) {
    users.set(entry.userName, (users.get(entry.userName) ?? 0) + 1);
    const status = STATUS[entry.status] ?? "Unknown";
    statuses.set(status, (statuses.get(status) ?? 0) + 1);
  }
  const divider = Math.max(1, Math.ceil(entries.length / 10)),
    performance: { label: string; value: number }[] = [];
  for (let index = 0; index + divider <= entries.length; index += divider) {
    const group = entries.slice(index, index + divider);
    performance.push({
      label: index + 1 + "~" + (index + divider),
      value: group.reduce((sum, e) => sum + (e.completionTime - e.startingTime) / 60000, 0) / divider,
    });
  }
  const max = Math.max(1, ...performance.map(p => p.value));
  return (
    <div className="history-charts">
      <PieChart
        title="Users who ran the execution"
        values={[...users]}
      />
      <PieChart
        title="Execution statuses"
        values={[...statuses]}
      />
      <figure>
        <figcaption>Average processing time (minutes)</figcaption>
        <svg
          viewBox="0 0 600 260"
          role="img"
          aria-label="Execution performance">
          {performance.map((p, index) => (
            <g key={p.label}>
              <rect
                x={index * 55 + 20}
                y={220 - (p.value / max) * 200}
                width="35"
                height={Math.max(0, (p.value / max) * 200)}
                fill="#176ac4">
                <title>
                  {p.label}: {p.value} minutes
                </title>
              </rect>
              <text
                x={index * 55 + 20}
                y="245"
                fontSize="10">
                {p.label}
              </text>
            </g>
          ))}
        </svg>
      </figure>
    </div>
  );
}
function PieChart({ title, values }: { title: string; values: [string, number][] }) {
  const total = values.reduce((sum, [, count]) => sum + count, 0);
  let offset = 0;
  return (
    <figure>
      <figcaption>{title}</figcaption>
      <svg
        viewBox="0 0 220 220"
        role="img"
        aria-label={title}>
        {values.map(([label, count], index) => {
          const length = (count / Math.max(1, total)) * 100,
            start = offset;
          offset += length;
          return (
            <circle
              key={label}
              cx="110"
              cy="110"
              r="75"
              fill="none"
              stroke={["#176ac4", "#ad3826", "#438c45", "#8e479b", "#bb7f20"][index % 5]}
              strokeWidth="65"
              pathLength="100"
              strokeDasharray={length + " " + (100 - length)}
              strokeDashoffset={-start}
              transform="rotate(-90 110 110)">
              <title>
                {label}: {count}
              </title>
            </circle>
          );
        })}
      </svg>
      <ul>
        {values.map(([label, count]) => (
          <li key={label}>
            {label}: {count}
          </li>
        ))}
      </ul>
    </figure>
  );
}
