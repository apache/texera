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

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import type { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import type { WorkflowRuntime, RuntimeSnapshot } from "../workspace/workflow-runtime";
import type { WorkflowSession } from "../workspace/workflow-session";
import type { WebResultUpdate } from "../../core/workspace/types/execute-workflow.interface";
import type { ConsoleMessage } from "../../core/workspace/types/workflow-common.interface";
import type { PaginatedResultEvent } from "../../core/workspace/types/workflow-websocket.interface";
import { isVideoUrl, isAudioUrl, isImageUrl } from "../../core/common/util/media-type.util";
import {
  ResultsRepository,
  resultColumns,
  valueText,
  visualizationDocument,
  type WorkflowExecutionsEntry,
  type ResultExportInput,
} from "./results-repository";
import "./results.css";
import { ExecutionHistoryPage } from "./execution-history";
import { ResourceCreatePage } from "../resources/storage-pages";
import { StorageApi } from "../resources/storage-api";
import { resetOperatorDebugHits } from "../workspace/operator-debug-source";
export interface ResultExportContext {
  computingUnitId: number;
  computingUnitType?: string;
  getToken?: () => string | null;
}
export interface ResultsPanelProps {
  runtime: WorkflowRuntime;
  session: WorkflowSession;
  config: GuiConfig;
  operatorIds?: readonly string[];
  selectedOperatorId?: string;
  onSelectOperator?: (id: string) => void;
  api?: ApiClient;
  exportContext?: ResultExportContext;
  onOpenExecution?: (entry: WorkflowExecutionsEntry) => void;
}
type Snapshot = RuntimeSnapshot & {
  materializedResults?: Readonly<Record<string, readonly object[]>>;
  workers?: Readonly<Record<string, readonly string[]>>;
};
const EMPTY: readonly object[] = [];
function errorText(error: unknown) {
  return error instanceof Error ? error.message : "The result request failed.";
}
export function ResultsPanel({
  runtime,
  session,
  config,
  operatorIds,
  selectedOperatorId,
  onSelectOperator,
  api,
  exportContext,
  onOpenExecution,
}: ResultsPanelProps) {
  const snapshot = useSyncExternalStore(runtime.subscribe, runtime.getSnapshot) as Snapshot;
  const { workflow } = useSyncExternalStore(session.subscribe, session.getSnapshot);
  const repository = useMemo(() => new ResultsRepository(runtime.socket, api), [runtime, api]);
  useEffect(() => {
    if (snapshot.state === "Initializing" && !Object.keys(snapshot.results).length) repository.clearNavigation();
  }, [repository, snapshot.state, snapshot.results]);
  const [tab, setTab] = useState<"results" | "console" | "errors" | "history">("results");
  const [localOperator, setLocalOperator] = useState("");
  const [portIndex, setPortIndex] = useState(0);
  const [exportInput, setExportInput] = useState<Partial<ResultExportInput>>();
  const operators = operatorIds
    ? workflow.content.operators.filter(op => operatorIds.includes(op.operatorID))
    : workflow.content.operators;
  const selected = operators.find(op => op.operatorID === (selectedOperatorId ?? localOperator)) ?? operators[0];
  const operatorID = selected?.operatorID;
  useEffect(() => setPortIndex(0), [operatorID]);
  const raw = operatorID ? snapshot.results[operatorID] : undefined;
  const materialized =
    (operatorID ? snapshot.materializedResults?.[operatorID] : undefined) ??
    (raw && raw.mode.type === "SetSnapshotMode" && "table" in raw ? raw.table : EMPTY);
  const chart = useMemo(() => visualizationDocument(materialized), [materialized]);
  const exportEnabled = !!config.exportExecutionResultEnabled && !!api && !!exportContext && workflow.wid !== undefined;
  const idle = ["Uninitialized", "Completed", "Failed", "Killed", "Terminated"].includes(snapshot.state);
  return (
    <section
      className="react-results"
      aria-label="Workflow results">
      <header className="results-heading">
        <label>
          Operator{" "}
          <select
            aria-label="Result operator"
            value={operatorID ?? ""}
            onChange={event => {
              setLocalOperator(event.target.value);
              onSelectOperator?.(event.target.value);
            }}>
            {operators.map(op => (
              <option
                key={op.operatorID}
                value={op.operatorID}>
                {op.customDisplayName ?? op.operatorType}
              </option>
            ))}
          </select>
        </label>
        {selected && selected.outputPorts.length > 1 && (
          <label>
            Output port{" "}
            <select
              aria-label="Output port"
              value={portIndex}
              onChange={event => setPortIndex(Number(event.target.value))}>
              {selected.outputPorts.map((port, index) => (
                <option
                  key={port.portID}
                  value={index}>
                  {port.displayName ?? "Output " + (index + 1)}
                </option>
              ))}
            </select>
          </label>
        )}
        <span>
          {snapshot.state} · {(snapshot.duration / 1000).toFixed(1)}s
        </span>
        {exportEnabled && (
          <button
            disabled={!idle || !operatorID || !raw}
            onClick={() => setExportInput({ operatorIds: operatorID ? [operatorID] : [] })}>
            Export results
          </button>
        )}
      </header>
      <div
        role="tablist"
        aria-label="Result views">
        <button
          role="tab"
          aria-selected={tab === "results"}
          onClick={() => setTab("results")}>
          Results
        </button>
        <button
          role="tab"
          aria-selected={tab === "console"}
          onClick={() => setTab("console")}>
          Console ({operatorID ? snapshot.console[operatorID]?.length ?? 0 : 0})
        </button>
        <button
          role="tab"
          aria-selected={tab === "errors"}
          onClick={() => setTab("errors")}>
          Errors ({snapshot.errors.length})
        </button>
        {config.workflowExecutionsTrackingEnabled && api && (
          <button
            role="tab"
            aria-selected={tab === "history"}
            onClick={() => setTab("history")}>
            Execution history
          </button>
        )}
      </div>
      {tab === "results" && (
        <div
          role="tabpanel"
          aria-label="Results">
          {!selected ? (
            <p>No operators to display.</p>
          ) : raw?.mode.type === "PaginationMode" ? (
            <ResultTable
              key={operatorID + ":" + portIndex}
              repository={repository}
              operatorID={operatorID!}
              portIndex={portIndex}
              update={raw}
              stats={portIndex === 0 ? snapshot.tableStats[operatorID!] : undefined}
              columnLimit={config.limitColumns}
              api={api}
              onExport={
                exportEnabled && idle
                  ? (rowIndex, columnIndex, columnName) =>
                      setExportInput({
                        operatorIds: [operatorID!],
                        format: "data",
                        rowIndex,
                        columnIndex,
                        filename: columnName + "_" + rowIndex,
                      })
                  : undefined
              }
            />
          ) : chart ? (
            <iframe
              className="result-visualization"
              title={"Visualization for " + (selected.customDisplayName ?? selected.operatorType)}
              sandbox="allow-scripts allow-downloads"
              srcDoc={chart}
            />
          ) : materialized.length ? (
            <SnapshotTable rows={materialized} />
          ) : (
            <p>Empty result set. Enable the eye icon on the operator and execute the workflow to view its results.</p>
          )}
        </div>
      )}
      {tab === "console" && (
        <ConsoleView
          session={session}
          key={operatorID}
          runtime={runtime}
          snapshot={snapshot}
          operatorID={operatorID}
        />
      )}
      {tab === "errors" && (
        <div
          role="tabpanel"
          aria-label="Errors">
          {!snapshot.errors.length ? (
            <p>No error to display.</p>
          ) : (
            snapshot.errors.map((error, index) => (
              <details
                key={index}
                className="fatal-error">
                <summary>
                  {error.type.name}: {error.message}
                </summary>
                <pre>{error.details}</pre>
                <p>
                  {error.workerId} ·{" "}
                  {new Date(error.timestamp.seconds * 1000 + error.timestamp.nanos / 1e6).toLocaleString()}
                </p>
                {error.operatorId !== "unknown operator" && (
                  <button
                    onClick={() => {
                      setLocalOperator(error.operatorId);
                      onSelectOperator?.(error.operatorId);
                    }}>
                    Focus operator {error.operatorId}
                  </button>
                )}
              </details>
            ))
          )}
        </div>
      )}
      {tab === "history" && workflow.wid !== undefined && (
        <ExecutionHistoryPage
          api={api!}
          wid={workflow.wid}
          onOpenExecution={onOpenExecution}
        />
      )}
      {exportInput && exportContext && workflow.wid !== undefined && (
        <ExportDialog
          api={api!}
          repository={repository}
          input={{
            workflowId: workflow.wid,
            workflowName: workflow.name,
            computingUnitId: exportContext.computingUnitId,
            operatorIds: exportInput.operatorIds ?? [],
            format: "csv",
            filename: workflow.name,
            ...exportInput,
            outputTypes: Object.fromEntries(
              Object.entries(snapshot.results).map(([id, result]) => [
                id,
                result.mode.type === "PaginationMode" ? exportInput.format ?? "csv" : "html",
              ])
            ),
          }}
          context={exportContext}
          onClose={() => setExportInput(undefined)}
        />
      )}
    </section>
  );
}
function SnapshotTable({ rows }: { rows: readonly object[] }) {
  const columns = resultColumns(rows, []);
  return (
    <div className="results-table-scroll">
      <table>
        <thead>
          <tr>
            {columns.map(column => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              {columns.map(column => (
                <td key={column}>{valueText((row as Record<string, unknown>)[column])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
interface TableProps {
  repository: ResultsRepository;
  operatorID: string;
  portIndex: number;
  update: WebResultUpdate;
  columnLimit?: number;
  stats?: Record<string, Record<string, number>>;
  api?: ApiClient;
  onExport?: (row: number, column: number, name: string) => void;
}
function ResultTable({ repository, operatorID, portIndex, update, columnLimit, stats, api, onExport }: TableProps) {
  const limit =
    typeof columnLimit === "number" && Number.isSafeInteger(columnLimit) && columnLimit > 0 ? columnLimit : 25;
  const retained = repository.getNavigation(operatorID, portIndex);
  const [pageIndex, setPage] = useState(retained?.pageIndex ?? 1),
    [pageSize, setSize] = useState(retained?.pageSize ?? 5),
    [columnOffset, setOffset] = useState(retained?.columnOffset ?? 0),
    [search, setSearch] = useState(retained?.columnSearch ?? "");
  const [page, setResult] = useState<PaginatedResultEvent>(),
    [loading, setLoading] = useState(false),
    [error, setError] = useState(""),
    [receivedTotal, setReceivedTotal] = useState(0);
  const [rowIndex, setRow] = useState<number>();
  const previous = useRef(update);
  const [refresh, setRefresh] = useState(0);
  const key = [operatorID, portIndex, pageIndex, pageSize, columnOffset, limit, search].join("|");
  useEffect(
    () =>
      repository.rememberNavigation(operatorID, {
        pageIndex,
        pageSize,
        columnOffset,
        columnLimit: limit,
        columnSearch: search,
        portIndex,
      }),
    [repository, key, operatorID, pageIndex, pageSize, columnOffset, limit, search, portIndex]
  );
  useEffect(() => {
    const old = previous.current;
    previous.current = update;
    if (
      old !== update &&
      portIndex === 0 &&
      update.mode.type === "PaginationMode" &&
      "dirtyPageIndices" in update &&
      update.dirtyPageIndices.includes(pageIndex)
    )
      setRefresh(value => value + 1);
  }, [update, portIndex, pageIndex]);
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    setResult(undefined);
    void repository
      .page(operatorID, { portIndex, pageIndex, pageSize, columnOffset, columnLimit: limit, columnSearch: search })
      .then(
        result => {
          if (!cancelled) {
            setResult(result);
            setReceivedTotal(result.totalTuples);
            setLoading(false);
          }
        },
        reason => {
          if (!cancelled) {
            setError(errorText(reason));
            setLoading(false);
          }
        }
      );
    return () => {
      cancelled = true;
    };
  }, [repository, key, refresh, operatorID, portIndex, pageIndex, pageSize, columnOffset, limit, search]);
  const total = portIndex === 0 && "totalNumTuples" in update ? update.totalNumTuples : receivedTotal;
  const columns = resultColumns(page?.table ?? [], page?.schema ?? []);
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  useEffect(() => {
    if (page && !loading && pageIndex > pageCount) setPage(pageCount);
  }, [page, pageIndex, pageCount, loading]);
  return (
    <div className="result-table-view">
      <div className="result-table-toolbar">
        <label>
          Search columns{" "}
          <input
            aria-label="Search columns"
            value={search}
            onChange={event => {
              setSearch(event.target.value);
              setOffset(0);
            }}
          />
        </label>
        <button
          disabled={columnOffset === 0}
          onClick={() => setOffset(Math.max(0, columnOffset - limit))}>
          Previous columns
        </button>
        <span>
          Columns {columnOffset + 1}–{columnOffset + columns.length}
        </span>
        <button
          disabled={!page || columns.length < limit}
          onClick={() => setOffset(columnOffset + limit)}>
          Next columns
        </button>
      </div>
      {loading && <p role="status">Loading results…</p>}
      {error && <p role="alert">{error}</p>}
      <div className="results-table-scroll">
        <table aria-label="Operator result table">
          <thead>
            <tr>
              <th className="row-actions">Row</th>
              {columns.map(column => (
                <th key={column}>{column}</th>
              ))}
            </tr>
            {stats && columns.some(column => stats[column]) && (
              <tr className="result-column-stats">
                <th>Statistics</th>
                {columns.map(column => (
                  <th key={column}>
                    {Object.entries(stats[column] ?? {}).map(([name, value]) => (
                      <div key={name}>
                        {name}: {String(value)}
                      </div>
                    ))}
                  </th>
                ))}
              </tr>
            )}
          </thead>
          <tbody>
            {page?.table.map((row, index) => (
              <tr key={index}>
                <td>
                  <button
                    aria-label={"View row " + ((pageIndex - 1) * pageSize + index + 1)}
                    onClick={() => setRow((pageIndex - 1) * pageSize + index)}>
                    Details
                  </button>
                </td>
                {columns.map((column, columnIndex) => (
                  <td key={column}>
                    <button
                      className="result-cell"
                      onClick={() => setRow((pageIndex - 1) * pageSize + index)}>
                      {mediaLabel(row[column]) ?? valueText(row[column])}
                    </button>
                    {onExport && (
                      <button
                        aria-label={"Export " + column + " row " + ((pageIndex - 1) * pageSize + index + 1)}
                        onClick={() =>
                          onExport((pageIndex - 1) * pageSize + index, columnOffset + columnIndex, column)
                        }>
                        ↓
                      </button>
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!loading && !page?.table.length && !error && <p>Empty result set.</p>}
      <footer className="result-pagination">
        <button
          disabled={pageIndex === 1 || loading}
          onClick={() => setPage(pageIndex - 1)}>
          Previous page
        </button>
        <span>
          Page {pageIndex} of {pageCount} · {total} rows
        </span>
        <button
          disabled={pageIndex >= pageCount || loading}
          onClick={() => setPage(pageIndex + 1)}>
          Next page
        </button>
        <label>
          Rows per page{" "}
          <select
            value={pageSize}
            onChange={event => {
              const size = Number(event.target.value);
              setPage(Math.floor(((pageIndex - 1) * pageSize) / size) + 1);
              setSize(size);
            }}>
            {[5, 10, 25, 50, 100].map(size => (
              <option key={size}>{size}</option>
            ))}
          </select>
        </label>
      </footer>
      {rowIndex !== undefined && (
        <RowDetails
          repository={repository}
          operatorID={operatorID}
          portIndex={portIndex}
          rowIndex={rowIndex}
          pageSize={pageSize}
          total={total}
          api={api}
          setRow={setRow}
        />
      )}
    </div>
  );
}
function mediaLabel(value: unknown): string | undefined {
  if (typeof value !== "string") return;
  return isVideoUrl(value)
    ? "Play Video"
    : isAudioUrl(value)
      ? "Play Audio"
      : isImageUrl(value)
        ? "View Image"
        : undefined;
}
function RowDetails({
  repository,
  operatorID,
  portIndex,
  rowIndex,
  pageSize,
  total,
  api,
  setRow,
}: {
  repository: ResultsRepository;
  operatorID: string;
  portIndex: number;
  rowIndex: number;
  pageSize: number;
  total: number;
  api?: ApiClient;
  setRow: (row: number | undefined) => void;
}) {
  const [row, setResult] = useState<Record<string, unknown>>(),
    [error, setError] = useState(""),
    [copy, setCopy] = useState("");
  useEffect(() => {
    let active = true;
    setResult(undefined);
    setError("");
    void repository.row(operatorID, rowIndex, pageSize, portIndex).then(
      value => {
        if (active) {
          setResult(value);
          if (!value) setError("This row is no longer available.");
        }
      },
      reason => {
        if (active) setError(errorText(reason));
      }
    );
    return () => {
      active = false;
    };
  }, [repository, operatorID, rowIndex, pageSize, portIndex]);
  return (
    <div className="result-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label="Row details"
        className="result-modal">
        <h2>Row details · {rowIndex + 1}</h2>
        {error && <p role="alert">{error}</p>}
        {!row && !error && <p role="status">Loading row…</p>}
        {row && (
          <>
            <button
              onClick={() => {
                void navigator.clipboard.writeText(JSON.stringify(row, null, 2)).then(
                  () => setCopy("Copied to clipboard"),
                  () => setCopy("Failed to copy")
                );
              }}>
              Copy JSON
            </button>
            {copy && <p role="status">{copy}</p>}
            <dl>
              {Object.entries(row).map(([name, value]) => (
                <div key={name}>
                  <dt>{name}</dt>
                  <dd>
                    <ResultMedia
                      value={value}
                      api={api}
                    />
                  </dd>
                </div>
              ))}
            </dl>
          </>
        )}
        <footer>
          <button
            disabled={!rowIndex}
            onClick={() => setRow(rowIndex - 1)}>
            Previous row
          </button>
          <button
            disabled={rowIndex >= total - 1}
            onClick={() => setRow(rowIndex + 1)}>
            Next row
          </button>
          <button onClick={() => setRow(undefined)}>Close</button>
        </footer>
      </section>
    </div>
  );
}
function ResultMedia({ value, api }: { value: unknown; api?: ApiClient }) {
  const text = valueText(value),
    kind = mediaLabel(value);
  const [source, setSource] = useState<string>();
  useEffect(() => {
    let active = true,
      url: string | undefined;
    setSource(undefined);
    if (!kind) return;
    if (/^https?:\/\//i.test(text)) {
      if (api)
        void api.get<Blob>("huggingface/media-proxy?url=" + encodeURIComponent(text), { responseType: "blob" }).then(
          blob => {
            if (active) {
              url = URL.createObjectURL(blob);
              setSource(url);
            }
          },
          () => undefined
        );
    } else setSource(text);
    return () => {
      active = false;
      if (url) URL.revokeObjectURL(url);
    };
  }, [api, text, kind]);
  if (!source) return <pre>{text}</pre>;
  return kind === "Play Video" ? (
    <video
      src={source}
      controls
    />
  ) : kind === "Play Audio" ? (
    <audio
      src={source}
      controls
    />
  ) : kind === "View Image" ? (
    <img
      src={source}
      alt="Result"
    />
  ) : (
    <pre>{text}</pre>
  );
}
function ConsoleView({
  session,
  runtime,
  snapshot,
  operatorID,
}: {
  session: WorkflowSession;
  runtime: WorkflowRuntime;
  snapshot: Snapshot;
  operatorID?: string;
}) {
  const [hidden, setHidden] = useState<string[]>([]),
    [timestamp, setTimestamp] = useState(true),
    [source, setSource] = useState(true),
    [worker, setWorker] = useState("all"),
    [command, setCommand] = useState(""),
    [error, setError] = useState("");
  const messages: readonly ConsoleMessage[] = operatorID ? snapshot.console[operatorID] ?? [] : [];
  const visible = messages.filter(message => !hidden.includes(message.msgType.name));
  const types = [
    ...new Set(["PRINT", "COMMAND", "DEBUGGER", "ERROR", ...messages.map(message => message.msgType.name)]),
  ];
  const workers = operatorID
    ? [
        ...new Set([
          ...(snapshot.workers?.[operatorID] ?? []),
          ...(snapshot.currentTuples?.[operatorID]?.tuples.map(tuple => tuple.workerID) ?? []),
          ...messages.map(message => message.workerId).filter(Boolean),
        ]),
      ]
    : [];
  const list = useRef<HTMLDivElement>(null),
    latest = visible.at(-1);
  useEffect(() => {
    if (list.current) list.current.scrollTop = list.current.scrollHeight;
  }, [latest]);
  function send(cmd: string) {
    try {
      if (!operatorID) return;
      if (!runtime.canControl()) throw new Error("Write access to the computing unit is required to debug.");
      for (const workerId of worker === "all" ? workers : [worker])
        runtime.socket.send("DebugCommandRequest", { operatorId: operatorID, workerId, cmd });
      if (cmd === "next" || cmd === "continue") resetOperatorDebugHits(session, operatorID);
      setError("");
      setCommand("");
    } catch (reason) {
      setError(errorText(reason));
    }
  }
  return (
    <div
      role="tabpanel"
      aria-label="Console">
      <div className="console-options">
        <label>
          <input
            type="checkbox"
            checked={timestamp}
            onChange={event => setTimestamp(event.target.checked)}
          />
          Show timestamp
        </label>
        <label>
          <input
            type="checkbox"
            checked={source}
            onChange={event => setSource(event.target.checked)}
          />
          Show source
        </label>
        <button onClick={() => setHidden(hidden.length ? [] : types)}>Select {hidden.length ? "all" : "none"}</button>
        {types.map(type => (
          <label key={type}>
            <input
              type="checkbox"
              aria-label={"Show " + type.charAt(0) + type.slice(1).toLowerCase() + " messages"}
              checked={!hidden.includes(type)}
              onChange={event =>
                setHidden(event.target.checked ? hidden.filter(item => item !== type) : [...hidden, type])
              }
            />
            {type}
          </label>
        ))}
      </div>
      {messages.length !== visible.length && <p>Hidden by filter: {messages.length - visible.length}</p>}
      <div
        className="console-messages"
        ref={list}>
        {!messages.length && <p>No console messages.</p>}
        {visible.map((message, index) => (
          <article
            key={index}
            className={"console-message message-" + message.msgType.name.toLowerCase()}>
            <span>{message.msgType.name}</span>
            {message.message ? (
              <details>
                <summary>{message.title}</summary>
                <pre>{message.message}</pre>
              </details>
            ) : (
              <p>{message.title}</p>
            )}
            <aside>
              {source && <span>{message.source}</span>}{" "}
              {timestamp && (
                <time>
                  {new Date(message.timestamp.seconds * 1000 + message.timestamp.nanos / 1e6).toLocaleString()}
                </time>
              )}{" "}
              {message.workerId && <span title={message.workerId}>W{message.workerId.split("-").at(-1)}</span>}
            </aside>
          </article>
        ))}
      </div>
      {snapshot.state === "Paused" && operatorID && runtime.canControl() && (
        <form
          onSubmit={event => {
            event.preventDefault();
            send(command);
          }}
          className="console-debug">
          <label>
            Worker{" "}
            <select
              value={worker}
              onChange={event => setWorker(event.target.value)}>
              <option value="all">All workers</option>
              {workers.map(id => (
                <option key={id}>{id}</option>
              ))}
            </select>
          </label>
          <button
            type="button"
            disabled={!workers.length}
            onClick={() => {
              try {
                runtime.skip(workers);
              } catch (reason) {
                setError(errorText(reason));
              }
            }}>
            Skip tuple
          </button>
          <button
            type="button"
            disabled={!workers.length}
            onClick={() => {
              try {
                runtime.retry(workers);
              } catch (reason) {
                setError(errorText(reason));
              }
            }}>
            Retry tuple
          </button>
          <button
            type="button"
            disabled={!workers.length}
            onClick={() => send("next")}>
            Step
          </button>
          <button
            type="button"
            disabled={!workers.length}
            onClick={() => send("continue")}>
            Continue
          </button>
          <input
            aria-label="Debug command"
            value={command}
            onChange={event => setCommand(event.target.value)}
          />
          <button disabled={!workers.length || !command}>Send</button>
        </form>
      )}
      {error && <p role="alert">{error}</p>}
    </div>
  );
}
function ExportDialog({
  api,
  repository,
  input,
  context,
  onClose,
}: {
  api: ApiClient;
  repository: ResultsRepository;
  input: ResultExportInput;
  context: ResultExportContext;
  onClose: () => void;
}) {
  const [filename, setFilename] = useState(input.filename),
    [creatingDataset, setCreatingDataset] = useState(false),
    [format, setFormat] = useState(input.format),
    [destination, setDestination] = useState("local"),
    [datasets, setDatasets] = useState<number[]>([]),
    [datasetOptions, setDatasetOptions] = useState<{ id: number; name: string }[]>(),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [busy, setBusy] = useState(false);
  const storage = useMemo(() => new StorageApi(api, { getToken: context.getToken }), [api, context.getToken]);
  const allVisualizations = Object.values(input.outputTypes ?? {}).every(type => type === "html");
  const allTables = Object.values(input.outputTypes ?? {}).every(type => type !== "html");
  const [binary, setBinary] = useState(false);
  const exportKey = JSON.stringify([input.operatorIds, input.outputTypes, input.format]);
  const exportScope = useMemo(() => {
    const [operatorIds, outputTypes, format] = JSON.parse(exportKey) as [
      string[],
      Record<string, string> | null,
      string,
    ];
    return { operatorIds, outputTypes, format };
  }, [exportKey]);
  useEffect(() => {
    if (exportScope.format === "data") return;
    let active = true;
    const tableIds = exportScope.operatorIds.filter(id => exportScope.outputTypes?.[id] !== "html");
    void Promise.all(tableIds.map(id => repository.page(id, { pageIndex: 1, pageSize: 1 }))).then(
      pages => {
        if (active) {
          const containsBinary = pages.some(page =>
            page.schema.some(attribute => attribute.attributeType === "binary")
          );
          setBinary(containsBinary);
          setFormat(allVisualizations ? "html" : allTables ? (containsBinary ? "arrow" : "csv") : "parquet");
        }
      },
      reason => {
        if (active) setError(errorText(reason));
      }
    );
    return () => {
      active = false;
    };
  }, [repository, exportScope, allTables, allVisualizations]);
  useEffect(() => {
    if (destination !== "dataset") return;
    let active = true;
    void repository.writableDatasets().then(
      options => {
        if (active) setDatasetOptions(options);
      },
      reason => {
        if (active) setError(errorText(reason));
      }
    );
    return () => {
      active = false;
    };
  }, [destination, repository]);
  async function submit() {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const datasetIds = destination === "dataset" ? datasets : [];
      if (destination === "dataset" && !datasetIds.length) throw new Error("Select at least one dataset.");
      const prepared = await repository.prepareExport({
        ...input,
        format,
        filename,
        datasetIds,
        outputTypes: Object.fromEntries(
          Object.entries(input.outputTypes ?? {}).map(([id, type]) => [id, type === "html" ? "html" : format])
        ),
      });
      if (prepared.blocked.length)
        setNotice(
          "Some operators were skipped because their results depend on datasets that are not downloadable: " +
            prepared.blocked.join(", ")
        );
      if (destination === "local")
        repository.exportToLocal(prepared.request, context.computingUnitType, context.getToken);
      else await repository.exportToDataset(prepared.request, context.computingUnitType);
      setNotice(
        (prepared.blocked.length ? "Some operators were skipped. " : "") +
          (destination === "local" ? "Download started." : "Result exported successfully.")
      );
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="result-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label="Export results"
        className="result-modal">
        <h2>Export results</h2>
        <label>
          Filename{" "}
          <input
            value={filename}
            onChange={event => setFilename(event.target.value)}
          />
        </label>
        <label>
          Format{" "}
          <select
            value={format}
            disabled={input.format === "data"}
            onChange={event => setFormat(event.target.value)}>
            {(input.format === "data"
              ? ["data"]
              : [
                  ...(allTables ? ["arrow", ...(!binary ? ["csv"] : [])] : []),
                  ...(allVisualizations ? ["html"] : []),
                  "parquet",
                ]
            ).map(type => (
              <option key={type}>{type}</option>
            ))}
          </select>
        </label>
        <label>
          Destination{" "}
          <select
            value={destination}
            onChange={event => setDestination(event.target.value)}>
            <option value="local">Local download</option>
            <option value="dataset">Dataset</option>
          </select>
        </label>
        {destination === "dataset" && (
          <fieldset>
            <legend>Writable datasets</legend>
            {!datasetOptions ? (
              <p>Loading datasets…</p>
            ) : !datasetOptions.length ? (
              <p>No writable datasets.</p>
            ) : (
              datasetOptions.map(dataset => (
                <label key={dataset.id}>
                  <input
                    type="checkbox"
                    checked={datasets.includes(dataset.id)}
                    onChange={event =>
                      setDatasets(
                        event.target.checked ? [...datasets, dataset.id] : datasets.filter(id => id !== dataset.id)
                      )
                    }
                  />
                  {dataset.name}
                </label>
              ))
            )}
            <button
              type="button"
              onClick={() => setCreatingDataset(true)}>
              Create new dataset
            </button>
          </fieldset>
        )}
        {creatingDataset && (
          <section aria-label="Create export dataset">
            <ResourceCreatePage
              api={api}
              storage={storage}
              kind="dataset"
              onCreated={entry => {
                setDatasetOptions(current => [{ id: entry.id, name: entry.resource.name }, ...(current ?? [])]);
                setDatasets(current => [...current, entry.id]);
                setCreatingDataset(false);
              }}
            />
            <button
              type="button"
              onClick={() => setCreatingDataset(false)}>
              Cancel dataset creation
            </button>
          </section>
        )}
        {error && <p role="alert">{error}</p>}
        {notice && <p role="status">{notice}</p>}
        <footer>
          <button
            disabled={busy}
            onClick={() => void submit()}>
            {busy ? "Exporting…" : "Export"}
          </button>
          <button
            disabled={busy}
            onClick={onClose}>
            Close
          </button>
        </footer>
      </section>
    </div>
  );
}
