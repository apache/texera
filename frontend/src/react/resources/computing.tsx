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
import { useCallback, useEffect, useRef, useState, type SetStateAction } from "react";
import { ApiError } from "../core/api-client";
import type { ComputingImage, ComputingUnit, ComputingUnitCreate } from "./api";
import { Dialog, LoadState, Notices, useOperation, useResource, type ResourcePageProps } from "./pages";
import { formatQuotaSize } from "./quota";

const messageOf = (error: unknown) => (error instanceof Error ? error.message : "The request failed.");
const always = () => true;
export function usePollingResource<T>(
  load: (signal: AbortSignal) => Promise<T>,
  intervalMs: number,
  shouldPoll: (value: T) => boolean = always,
  retryInitialFailure = true
) {
  const [data, setDataState] = useState<T>();
  const latest = useRef<T | undefined>(undefined);
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const setData = (update: SetStateAction<T | undefined>) =>
    setDataState(previous => {
      const next =
        typeof update === "function" ? (update as (value: T | undefined) => T | undefined)(previous) : update;
      latest.current = next;
      return next;
    });
  useEffect(() => {
    let active = true;
    let inFlight = false;
    let controller: AbortController | undefined;
    const fetch = (foreground: boolean) => {
      if (inFlight) return;
      inFlight = true;
      controller = new AbortController();
      const signal = controller.signal;
      if (foreground) setLoading(true);
      void load(signal)
        .then(
          value => {
            if (active) {
              latest.current = value;
              setDataState(value);
              setError(undefined);
            }
          },
          reason => {
            if (active && !signal.aborted) setError(reason);
          }
        )
        .finally(() => {
          inFlight = false;
          if (active) setLoading(false);
        });
    };
    fetch(true);
    const timer = setInterval(
      () => {
        if (latest.current === undefined ? retryInitialFailure : shouldPoll(latest.current)) fetch(false);
      },
      Math.max(10, intervalMs)
    );
    return () => {
      active = false;
      controller?.abort();
      clearInterval(timer);
    };
  }, [load, intervalMs, shouldPoll, retryInitialFailure, revision]);
  return { data, setData, error, loading, refresh: () => setRevision(value => value + 1) };
}

function quantity(value: string, kind: "cpu" | "memory"): number {
  const match = /^(\d+(?:\.\d+)?)([a-zA-Z]*)$/.exec(value ?? "");
  if (!match) return 0;
  const number = Number(match[1]);
  const unit = match[2];
  const scale =
    kind === "cpu"
      ? ({ n: 1e-9, u: 1e-6, m: 1e-3, "": 1 } as Record<string, number>)[unit]
      : (
          {
            "": 1,
            B: 1,
            K: 1e3,
            M: 1e6,
            G: 1e9,
            T: 1e12,
            Ki: 1024,
            Mi: 1024 ** 2,
            Gi: 1024 ** 3,
            Ti: 1024 ** 4,
          } as Record<string, number>
        )[unit];
  return number * (scale ?? 0);
}
export function resourcePercentage(usage: string, limit: string, kind: "cpu" | "memory"): number {
  const maximum = quantity(limit, kind);
  return maximum > 0 ? Math.max(0, Math.min(100, (quantity(usage, kind) / maximum) * 100)) : 0;
}
export function jvmMemoryChoices(memory: string): { choices: number[]; selected: number } {
  const match = /^(\d+(?:\.\d+)?)(Gi|Mi)$/.exec(memory);
  const gb = match ? (match[2] === "Gi" ? Number(match[1]) : Math.max(1, Math.floor(Number(match[1]) / 1024))) : 1;
  const choices: number[] = [];
  for (let value = gb <= 3 ? 1 : 2; value <= gb; value *= 2) choices.push(value);
  return { choices: choices.length ? choices : [1], selected: gb <= 3 ? (gb === 1 ? 1 : 2) : 2 };
}

export interface ComputingUnitsProps extends ResourcePageProps {
  admin?: boolean;
  refreshMs?: number;
  sharingEnabled?: boolean;
  onShare?: (cuid: number) => void;
  beforeTerminate?: (cuid: number) => Promise<void> | void;
}
export function ComputingUnitsPage({
  resources,
  onNotice,
  admin = false,
  refreshMs = 5000,
  sharingEnabled = false,
  onShare,
  beforeTerminate,
}: ComputingUnitsProps) {
  const terminated = useRef(new Set<number>());
  const load = useCallback(
    async (signal: AbortSignal) =>
      (await resources.computingUnits(admin, { signal })).filter(
        item => !terminated.current.has(item.computingUnit.cuid)
      ),
    [resources, admin]
  );
  const state = usePollingResource(load, refreshMs);
  const operation = useOperation(onNotice);
  const [creating, setCreating] = useState(false);
  const [createdNotice, setCreatedNotice] = useState<string>();
  const [renaming, setRenaming] = useState<ComputingUnit>();
  const [name, setName] = useState("");
  const [deleting, setDeleting] = useState<ComputingUnit>();
  const [details, setDetails] = useState<ComputingUnit>();
  const [typeFilter, setTypeFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [sort, setSort] = useState("name");
  const [descending, setDescending] = useState(false);
  const rows = (state.data ?? [])
    .filter(
      item => (!typeFilter || item.computingUnit.type === typeFilter) && (!statusFilter || item.status === statusFilter)
    )
    .sort((left, right) => {
      const value =
        sort === "created"
          ? left.computingUnit.creationTime - right.computingUnit.creationTime
          : String(
              sort === "owner"
                ? left.ownerName
                : sort === "status"
                  ? left.status
                  : sort === "type"
                    ? left.computingUnit.type
                    : left.computingUnit.name
            ).localeCompare(
              String(
                sort === "owner"
                  ? right.ownerName
                  : sort === "status"
                    ? right.status
                    : sort === "type"
                      ? right.computingUnit.type
                      : right.computingUnit.name
              )
            );
      return value * (descending ? -1 : 1);
    });
  return (
    <main className="resource-page">
      <header>
        <h1>{admin ? "All computing units" : "Computing units"}</h1>
        <div>
          <button
            type="button"
            onClick={state.refresh}>
            Refresh
          </button>
          {!admin && (
            <button
              type="button"
              onClick={() => {
                operation.clear();
                setCreating(true);
              }}>
              Create computing unit
            </button>
          )}
        </div>
      </header>
      <LoadState
        loading={state.loading}
        error={state.error ? messageOf(state.error) : undefined}
        retry={state.refresh}
      />
      {!!state.error && state.data && <p>The rows show the last successful snapshot; current status is unavailable.</p>}
      <Notices
        error={operation.error}
        message={operation.message ?? createdNotice}
      />
      {admin && <ClusterCapacityPanel resources={resources} />}
      <section className="resource-input-grid resource-panel">
        <label>
          Computing unit type filter
          <select
            value={typeFilter}
            onChange={event => setTypeFilter(event.target.value)}>
            <option value="">All types</option>
            <option value="local">Local</option>
            <option value="kubernetes">Kubernetes</option>
          </select>
        </label>
        <label>
          Computing unit status filter
          <select
            value={statusFilter}
            onChange={event => setStatusFilter(event.target.value)}>
            <option value="">All statuses</option>
            {["Running", "Pending", "Failed", "Unknown", "Terminating"].map(status => (
              <option key={status}>{status}</option>
            ))}
          </select>
        </label>
        <label>
          Sort computing units by
          <select
            value={sort}
            onChange={event => setSort(event.target.value)}>
            {["name", "owner", "type", "status", "created"].map(value => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
        <button
          type="button"
          onClick={() => setDescending(!descending)}>
          {descending ? "Descending" : "Ascending"}
        </button>
      </section>
      {!!state.data &&
        (rows.length ? (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Owner</th>
                <th>Type</th>
                <th>Status</th>
                <th>CPU</th>
                <th>Memory</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(item => (
                <tr key={item.computingUnit.cuid}>
                  <td>
                    <button
                      type="button"
                      className="resource-link"
                      aria-label={`Computing unit details ${item.computingUnit.name}`}
                      onClick={() => setDetails(item)}>
                      {item.computingUnit.name}
                    </button>
                  </td>
                  <td>{item.ownerName}</td>
                  <td>{item.computingUnit.type}</td>
                  <td title={item.statusReason ?? ""}>
                    {item.status}
                    {item.statusReason && <p>{item.statusReason}</p>}
                  </td>
                  <td>
                    <progress
                      aria-label={`CPU usage ${item.computingUnit.name}`}
                      max={100}
                      value={resourcePercentage(item.metrics.cpuUsage, item.computingUnit.resource.cpuLimit, "cpu")}
                    />
                    {item.metrics.cpuUsage} /{" "}
                    {item.computingUnit.resource.cpuLimit === "NaN" ? "—" : item.computingUnit.resource.cpuLimit}
                  </td>
                  <td>
                    <progress
                      aria-label={`Memory usage ${item.computingUnit.name}`}
                      max={100}
                      value={resourcePercentage(
                        item.metrics.memoryUsage,
                        item.computingUnit.resource.memoryLimit,
                        "memory"
                      )}
                    />
                    {item.metrics.memoryUsage} /{" "}
                    {item.computingUnit.resource.memoryLimit === "NaN" ? "—" : item.computingUnit.resource.memoryLimit}
                  </td>
                  <td>{new Date(item.computingUnit.creationTime).toLocaleString()}</td>
                  <td className="resource-row-actions">
                    {item.isOwner && (
                      <button
                        type="button"
                        disabled={operation.busy}
                        aria-label={`Rename computing unit ${item.computingUnit.name}`}
                        onClick={() => {
                          operation.clear();
                          setRenaming(item);
                          setName(item.computingUnit.name);
                        }}>
                        Rename
                      </button>
                    )}
                    {sharingEnabled && onShare && (
                      <button
                        type="button"
                        aria-label={`Share computing unit ${item.computingUnit.name}`}
                        onClick={() => onShare(item.computingUnit.cuid)}>
                        Share
                      </button>
                    )}
                    <button
                      type="button"
                      disabled={operation.busy || item.status === "Terminating"}
                      aria-label={`${item.computingUnit.type === "local" ? "Disconnect" : "Terminate"} computing unit ${item.computingUnit.name}`}
                      onClick={() => {
                        if (!item.computingUnit.uri) operation.fail("Invalid computing unit.");
                        else {
                          operation.clear();
                          setDeleting(item);
                        }
                      }}>
                      {item.computingUnit.type === "local" ? "Disconnect" : "Terminate"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No computing units match the selected filters.</p>
        ))}
      {creating && (
        <CreateUnitDialog
          resources={resources}
          close={() => setCreating(false)}
          created={item => {
            state.setData(current => [
              ...(current ?? []).filter(previous => previous.computingUnit.cuid !== item.computingUnit.cuid),
              item,
            ]);
            setCreating(false);
            setCreatedNotice("Computing unit created.");
          }}
          onNotice={onNotice}
        />
      )}
      {renaming && (
        <Dialog
          title="Rename computing unit"
          close={() => setRenaming(undefined)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              const next = name.trim();
              if (!next || next.length > 128) {
                operation.fail("Computing unit name must contain between 1 and 128 characters.");
                return;
              }
              void operation.run(
                () => resources.renameComputingUnit(renaming.computingUnit.cuid, next),
                () => {
                  state.setData(current =>
                    current?.map(item =>
                      item.computingUnit.cuid === renaming.computingUnit.cuid
                        ? { ...item, computingUnit: { ...item.computingUnit, name: next } }
                        : item
                    )
                  );
                  setRenaming(undefined);
                },
                "Computing unit renamed."
              );
            }}>
            <label>
              New computing unit name
              <input
                value={name}
                maxLength={128}
                disabled={operation.busy}
                onChange={event => setName(event.target.value)}
              />
            </label>
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setRenaming(undefined)}>
                Cancel
              </button>
              <button
                type="submit"
                disabled={operation.busy}>
                Save name
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {deleting && (
        <Dialog
          title={`${deleting.computingUnit.type === "local" ? "Disconnect" : "Terminate"} computing unit "${deleting.computingUnit.name}"?`}
          close={() => setDeleting(undefined)}
          busy={operation.busy}>
          <p>
            Are you sure you want to {deleting.computingUnit.type === "local" ? "disconnect from" : "terminate"} this
            computing unit?
          </p>
          {deleting.computingUnit.type === "kubernetes" && (
            <p className="resource-error">All execution results in this computing unit will be lost.</p>
          )}
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
                  async () => {
                    await beforeTerminate?.(deleting.computingUnit.cuid);
                    await resources.terminateComputingUnit(deleting.computingUnit.cuid);
                  },
                  () => {
                    terminated.current.add(deleting.computingUnit.cuid);
                    state.setData(current =>
                      current?.filter(item => item.computingUnit.cuid !== deleting.computingUnit.cuid)
                    );
                    setDeleting(undefined);
                  },
                  "Computing unit disconnected or terminated."
                )
              }>
              {deleting.computingUnit.type === "local" ? "Disconnect" : "Terminate"}
            </button>
          </footer>
        </Dialog>
      )}
      {details && (
        <Dialog
          title="Computing unit information"
          close={() => setDetails(undefined)}>
          <dl>
            {[
              ["Name", details.computingUnit.name],
              ["Status", details.status],
              ["Type", details.computingUnit.type],
              ["URI", details.computingUnit.uri],
              ["Access", details.isOwner ? "Owner" : details.accessPrivilege],
              ...Object.entries(details.computingUnit.resource).map(([key, value]) => [
                key,
                Array.isArray(value) ? value.join(", ") : value,
              ]),
            ].map(([key, value]) => (
              <div key={key}>
                <dt>{key}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          <button
            type="button"
            onClick={() => setDetails(undefined)}>
            Close details
          </button>
        </Dialog>
      )}
    </main>
  );
}

function ClusterCapacityPanel({ resources }: ResourcePageProps) {
  const load = useCallback((_signal: AbortSignal) => resources.clusterCapacity(), [resources]);
  const state = useResource(load);
  if (state.error)
    return (
      <section className="resource-panel">
        <h2>Cluster capacity</h2>
        <p>Cluster capacity is unavailable for this deployment.</p>
        <button
          type="button"
          onClick={state.refresh}>
          Retry capacity
        </button>
      </section>
    );
  return (
    <section className="resource-panel">
      <h2>Cluster capacity</h2>
      {state.loading ? (
        <p>Loading capacity…</p>
      ) : (
        state.data && (
          <div className="resource-input-grid">
            {(["cpu", "memory", "gpu"] as const).map(key => {
              const item = state.data![key];
              const format =
                key === "memory"
                  ? formatQuotaSize
                  : key === "cpu"
                    ? (value: number) => `${value / 1000} cores`
                    : (value: number) => `${value} devices`;
              return (
                <div key={key}>
                  <h3>{key.toUpperCase()}</h3>
                  <progress
                    aria-label={`${key} allocated capacity`}
                    max={Math.max(item.total, 1)}
                    value={item.allocated}
                  />
                  <p>Allocated: {format(item.allocated)}</p>
                  <p>Available: {format(item.available)}</p>
                  <p>Total: {format(item.total)}</p>
                </div>
              );
            })}
          </div>
        )
      )}
    </section>
  );
}

function CreateUnitDialog({
  resources,
  onNotice,
  close,
  created,
}: ResourcePageProps & { close: () => void; created: (unit: ComputingUnit) => void }) {
  const [imageError, setImageError] = useState<string>();
  const load = useCallback(
    async (_signal: AbortSignal) => {
      const [types, limits, images] = await Promise.all([
        resources.computingUnitTypes(),
        resources.computingUnitLimits(),
        resources.images().catch(error => {
          if (!(error instanceof ApiError && error.status === 503)) setImageError(messageOf(error));
          return [];
        }),
      ]);
      return { types: types.typeOptions, limits, images: images.filter(item => item.status === "READY") };
    },
    [resources]
  );
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [type, setType] = useState<"local" | "kubernetes">();
  const [name, setName] = useState("My Computing Unit");
  const [cpu, setCpu] = useState("");
  const [memory, setMemory] = useState("");
  const [gpu, setGpu] = useState("0");
  const [jvm, setJvm] = useState(1);
  const [shm, setShm] = useState(64);
  const [shmUnit, setShmUnit] = useState<"Mi" | "Gi">("Mi");
  const [imageId, setImageId] = useState(0);
  const [uri, setUri] = useState(`${window.location.protocol}//${window.location.host}/wsapi`);
  useEffect(() => {
    if (state.data) {
      setType(state.data.types.includes("kubernetes") ? "kubernetes" : state.data.types[0]);
      setCpu(state.data.limits.cpuLimitOptions[0] ?? "1");
      const selectedMemory = state.data.limits.memoryLimitOptions[0] ?? "1Gi";
      setMemory(selectedMemory);
      setGpu(state.data.limits.gpuLimitOptions[0] ?? "0");
      setJvm(jvmMemoryChoices(selectedMemory).selected);
    }
  }, [state.data]);
  function submit() {
    if (!type) {
      operation.fail("Please select a valid computing unit type.");
      return;
    }
    if (type === "local" && !uri.trim()) {
      operation.fail("URI for local computing unit cannot be empty.");
      return;
    }
    if (type === "kubernetes" && (!name.trim() || name.trim().length > 128)) {
      operation.fail("Computing unit name must contain between 1 and 128 characters.");
      return;
    }
    if (
      type === "kubernetes" &&
      (!Number.isFinite(shm) || shm <= 0 || quantity(`${shm}${shmUnit}`, "memory") > quantity(memory, "memory"))
    ) {
      operation.fail("Shared memory must be positive and cannot exceed total memory.");
      return;
    }
    const request: ComputingUnitCreate =
      type === "local"
        ? { unitType: type, name, uri: uri.trim() }
        : {
            unitType: type,
            name: name.trim(),
            cpuLimit: cpu,
            memoryLimit: memory,
            gpuLimit: gpu,
            jvmMemorySize: `${jvm}G`,
            shmSize: `${shm}${shmUnit}`,
            ...(imageId ? { iid: imageId } : {}),
          };
    void operation.run(() => resources.createComputingUnit(request), created, "Computing unit created.");
  }
  return (
    <Dialog
      title="Create computing unit"
      close={close}
      busy={operation.busy}>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error ?? imageError}
        message={operation.message}
      />
      {state.data && !state.data.types.length && <p>No computing unit types are available in this deployment.</p>}
      {state.data && type !== undefined && (
        <form
          onSubmit={event => {
            event.preventDefault();
            submit();
          }}>
          <fieldset
            disabled={operation.busy}
            className="resource-form-grid">
            <label>
              Computing unit type
              <select
                value={type ?? ""}
                onChange={event => setType(event.target.value as typeof type)}>
                {state.data.types.map(value => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <label>
              Computing unit name
              <input
                value={name}
                maxLength={128}
                onChange={event => setName(event.target.value)}
              />
            </label>
            {type === "local" ? (
              <label>
                Local computing unit URI
                <input
                  value={uri}
                  onChange={event => setUri(event.target.value)}
                />
              </label>
            ) : (
              <>
                <label>
                  CPU limit
                  <select
                    value={cpu}
                    onChange={event => setCpu(event.target.value)}>
                    {(state.data.limits.cpuLimitOptions.length ? state.data.limits.cpuLimitOptions : ["1"]).map(
                      value => (
                        <option key={value}>{value}</option>
                      )
                    )}
                  </select>
                </label>
                <label>
                  Memory limit
                  <select
                    value={memory}
                    onChange={event => {
                      const next = event.target.value;
                      const choices = jvmMemoryChoices(next);
                      setMemory(next);
                      setJvm(previous =>
                        previous >= 2 && choices.choices.includes(previous) ? previous : choices.selected
                      );
                    }}>
                    {(state.data.limits.memoryLimitOptions.length ? state.data.limits.memoryLimitOptions : ["1Gi"]).map(
                      value => (
                        <option key={value}>{value}</option>
                      )
                    )}
                  </select>
                </label>
                <label>
                  GPU limit
                  <select
                    value={gpu}
                    onChange={event => setGpu(event.target.value)}>
                    {(state.data.limits.gpuLimitOptions.length ? state.data.limits.gpuLimitOptions : ["0"]).map(
                      value => (
                        <option key={value}>{value}</option>
                      )
                    )}
                  </select>
                </label>
                {state.data.images.length > 0 && (
                  <label>
                    Container image
                    <select
                      value={imageId}
                      onChange={event => setImageId(Number(event.target.value))}>
                      <option value={0}>Deployment image</option>
                      {state.data.images.map(item => (
                        <option
                          key={item.iid}
                          value={item.iid}>
                          {item.name}
                        </option>
                      ))}
                    </select>
                  </label>
                )}
                <details>
                  <summary>Advanced settings</summary>
                  <label>
                    JVM memory (GiB)
                    <select
                      value={jvm}
                      onChange={event => setJvm(Number(event.target.value))}>
                      {jvmMemoryChoices(memory).choices.map(value => (
                        <option key={value}>{value}</option>
                      ))}
                    </select>
                  </label>
                  {jvm >= 4 && jvm === jvmMemoryChoices(memory).choices.at(-1) && (
                    <p>JVM memory is at the computing unit's maximum; leave room for other processes.</p>
                  )}
                  <label>
                    Shared memory size
                    <input
                      type="number"
                      min={1}
                      value={shm}
                      onChange={event => setShm(Number(event.target.value))}
                    />
                  </label>
                  <label>
                    Shared memory unit
                    <select
                      value={shmUnit}
                      onChange={event => setShmUnit(event.target.value as typeof shmUnit)}>
                      <option>Mi</option>
                      <option>Gi</option>
                    </select>
                  </label>
                </details>
              </>
            )}
          </fieldset>
          <footer>
            <button
              type="button"
              disabled={operation.busy}
              onClick={close}>
              Cancel
            </button>
            <button
              type="submit"
              disabled={operation.busy}>
              Create unit
            </button>
          </footer>
        </form>
      )}
    </Dialog>
  );
}

const imagesInProgress = (images: ComputingImage[]) =>
  images.some(item => item.status === "PENDING" || item.status === "VALIDATING");
export function ComputingImagesPage({
  resources,
  onNotice,
  refreshMs = 3000,
}: ResourcePageProps & { refreshMs?: number }) {
  const removed = useRef(new Set<number>());
  const load = useCallback(
    (signal: AbortSignal) =>
      resources
        .images({ signal })
        .then(rows =>
          rows
            .filter(item => !removed.current.has(item.iid))
            .sort((left, right) => right.creationTime - left.creationTime)
        ),
    [resources]
  );
  const state = usePollingResource(load, refreshMs, imagesInProgress, false);
  const operation = useOperation(onNotice);
  const [name, setName] = useState("");
  const [reference, setReference] = useState("");
  const [deleting, setDeleting] = useState<ComputingImage>();
  const [log, setLog] = useState<ComputingImage>();
  const featureDisabled = state.error instanceof ApiError && state.error.status === 503;
  return (
    <main className="resource-page">
      <header>
        <h1>Computing unit images</h1>
      </header>
      {featureDisabled ? (
        <p>Curated images are disabled in this deployment.</p>
      ) : (
        <>
          <LoadState
            loading={state.loading}
            error={state.error ? messageOf(state.error) : undefined}
            retry={state.refresh}
          />
          <Notices
            error={operation.error}
            message={operation.message}
          />
          <form
            className="resource-panel resource-input-grid"
            onSubmit={event => {
              event.preventDefault();
              if (!name.trim() || !reference.trim()) {
                operation.fail("Both a name and an image reference are required.");
                return;
              }
              void operation.run(
                () => resources.createImage(name.trim(), reference.trim()),
                () => {
                  setName("");
                  setReference("");
                  state.refresh();
                },
                "Image registered."
              );
            }}>
            <label>
              Image name
              <input
                value={name}
                disabled={operation.busy}
                onChange={event => setName(event.target.value)}
              />
            </label>
            <label>
              Image reference
              <input
                value={reference}
                disabled={operation.busy}
                onChange={event => setReference(event.target.value)}
              />
            </label>
            <button
              type="submit"
              disabled={operation.busy || state.loading}>
              Register image
            </button>
          </form>
          {state.data &&
            (state.data.length ? (
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Source</th>
                    <th>Digest</th>
                    <th>Status</th>
                    <th>Attempt</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {state.data.map(item => (
                    <tr key={item.iid}>
                      <td>{item.name}</td>
                      <td>{item.sourceRef}</td>
                      <td title={item.sourceDigest ?? ""}>{item.sourceDigest?.slice(0, 19) ?? "—"}</td>
                      <td>{item.status}</td>
                      <td>{item.attempt}</td>
                      <td className="resource-row-actions">
                        <button
                          type="button"
                          aria-label={`Refresh image ${item.name}`}
                          disabled={operation.busy}
                          onClick={() =>
                            void operation.run(
                              () => resources.refreshImage(item.iid),
                              () => state.refresh(),
                              "Image validation started."
                            )
                          }>
                          Refresh
                        </button>
                        <button
                          type="button"
                          aria-label={`Validation log for ${item.name}`}
                          onClick={() => setLog(item)}>
                          Log
                        </button>
                        <button
                          type="button"
                          aria-label={`Delete image ${item.name}`}
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
            ) : (
              <p>No curated images.</p>
            ))}
        </>
      )}
      {log && (
        <ImageLogDialog
          resources={resources}
          image={log}
          intervalMs={refreshMs}
          close={() => setLog(undefined)}
        />
      )}
      {deleting && (
        <Dialog
          title={`Delete image "${deleting.name}"?`}
          close={() => setDeleting(undefined)}
          busy={operation.busy}>
          <p>This removes the curated image registration.</p>
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
                  () => resources.deleteImage(deleting.iid),
                  () => {
                    removed.current.add(deleting.iid);
                    state.setData(current => current?.filter(item => item.iid !== deleting.iid));
                    setDeleting(undefined);
                  },
                  "Image deleted."
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

function ImageLogDialog({
  resources,
  image,
  intervalMs,
  close,
}: ResourcePageProps & { image: ComputingImage; intervalMs: number; close: () => void }) {
  const load = useCallback((_signal: AbortSignal) => resources.imageLog(image.iid), [resources, image.iid]);
  const poll = useCallback(
    (value: { status: ComputingImage["status"] }) => value.status === "PENDING" || value.status === "VALIDATING",
    []
  );
  const state = usePollingResource(load, intervalMs, poll);
  return (
    <Dialog
      title={`Validation log for ${image.name}`}
      close={close}>
      <LoadState
        loading={state.loading}
        error={state.error ? messageOf(state.error) : undefined}
        retry={state.refresh}
      />
      {state.data && (
        <>
          <p>
            {state.data.status} · attempt {state.data.attempt}
          </p>
          <pre>{state.data.log || "No validation output yet."}</pre>
        </>
      )}
      <button
        type="button"
        onClick={close}>
        Close log
      </button>
    </Dialog>
  );
}
