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
import { useCallback, useEffect, useId, useRef, useState, type ReactNode } from "react";
import { ResourceApi, type PythonEnvironment, type Warehouse } from "./api";
import "./resources.css";

export interface ResourcePageProps {
  resources: ResourceApi;
  onNotice?: (message: string, error: boolean) => void;
}
const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : "The request failed. Please try again.";

export function useResource<T>(load: (signal: AbortSignal) => Promise<T>) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    setData(undefined);
    setError(undefined);
    setLoading(true);
    void load(controller.signal)
      .then(
        result => {
          if (active) setData(result);
        },
        reason => {
          if (active && !controller.signal.aborted) setError(errorMessage(reason));
        }
      )
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [load, revision]);
  return { data, setData, error, loading, refresh: () => setRevision(value => value + 1) };
}

export function useOperation(onNotice?: ResourcePageProps["onNotice"]) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const running = useRef(false);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  async function run<T>(request: () => Promise<T>, success: (result: T) => void, notice: string) {
    if (running.current) return;
    running.current = true;
    setBusy(true);
    setError(undefined);
    setMessage(undefined);
    try {
      const result = await request();
      onNotice?.(notice, false);
      if (active.current) {
        success(result);
        setMessage(notice);
      }
    } catch (reason) {
      const text = errorMessage(reason);
      onNotice?.(text, true);
      if (active.current) setError(text);
    } finally {
      running.current = false;
      if (active.current) setBusy(false);
    }
  }
  return {
    busy,
    error,
    message,
    run,
    fail: (text: string) => {
      setError(text);
      setMessage(undefined);
    },
    clear: () => {
      setError(undefined);
      setMessage(undefined);
    },
  };
}

export function Notices({ error, message }: { error?: string; message?: string }) {
  return (
    <>
      {error && (
        <p
          role="alert"
          className="resource-error">
          {error}
        </p>
      )}
      {message && (
        <p
          role="status"
          className="resource-success">
          {message}
        </p>
      )}
    </>
  );
}
export function LoadState({ loading, error, retry }: { loading: boolean; error?: string; retry: () => void }) {
  return (
    <>
      {loading && <p role="status">Loading…</p>}
      {error && (
        <>
          <p
            role="alert"
            className="resource-error">
            {error}
          </p>
          <button
            type="button"
            onClick={retry}>
            Retry
          </button>
        </>
      )}
    </>
  );
}

export function Dialog({
  title,
  children,
  busy,
  close,
}: {
  title: string;
  children: ReactNode;
  busy?: boolean;
  close: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();
  useEffect(() => {
    const element = ref.current!;
    if (typeof element.showModal === "function") element.showModal();
    else element.setAttribute("open", "");
    return () => {
      if (typeof element.close === "function") element.close();
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className="resource-dialog"
      aria-labelledby={titleId}
      onCancel={event => {
        event.preventDefault();
        if (!busy) close();
      }}>
      <h2 id={titleId}>{title}</h2>
      {children}
    </dialog>
  );
}

export function WarehousePage({ resources, onNotice }: ResourcePageProps) {
  const load = useCallback((signal: AbortSignal) => resources.warehouseStatus({ signal }), [resources]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [deleting, setDeleting] = useState<Warehouse>();
  const validName = /^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(name.trim()) && name.trim().length <= 64;
  return (
    <main className="resource-page">
      <header>
        <h1>Warehouses</h1>
        {state.data?.enabled && (
          <button
            type="button"
            onClick={() => {
              setName("");
              operation.clear();
              setCreating(true);
            }}>
            Create warehouse
          </button>
        )}
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
      {state.data?.enabled === false && <p>Warehouses are disabled in this deployment.</p>}
      {state.data?.enabled &&
        (state.data.warehouses.length ? (
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Owner</th>
                <th>Storage</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {state.data.warehouses.map(warehouse => (
                <tr key={warehouse.whid}>
                  <td>{warehouse.name}</td>
                  <td>{warehouse.ownerName ?? "—"}</td>
                  <td>{warehouse.flavor}</td>
                  <td>{new Date(warehouse.createdAtMillis).toLocaleString()}</td>
                  <td>
                    <button
                      type="button"
                      aria-label={`Delete warehouse ${warehouse.name}`}
                      disabled={operation.busy}
                      onClick={() => {
                        operation.clear();
                        setDeleting(warehouse);
                      }}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p>No warehouses yet. Create a warehouse to store execution data.</p>
        ))}
      {creating && (
        <Dialog
          title="Create warehouse"
          close={() => setCreating(false)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              if (!validName) return;
              void operation.run(
                () => resources.createWarehouse(name.trim()),
                created => {
                  state.setData(current => current && { ...current, warehouses: [...current.warehouses, created] });
                  setCreating(false);
                },
                `Warehouse "${name.trim()}" created.`
              );
            }}>
            <label>
              Warehouse name
              <input
                value={name}
                onChange={event => setName(event.target.value)}
                maxLength={64}
                disabled={operation.busy}
                autoFocus
              />
            </label>
            <p>Use up to 64 letters, numbers, hyphens or underscores, beginning with a letter or number.</p>
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setCreating(false)}>
                Cancel
              </button>
              <button
                type="submit"
                disabled={!validName || operation.busy}>
                Create
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {deleting && (
        <Dialog
          title={`Delete warehouse "${deleting.name}"?`}
          close={() => setDeleting(undefined)}
          busy={operation.busy}>
          <p>This permanently deletes the warehouse and all data stored in it.</p>
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
                  () => resources.deleteWarehouse(deleting.whid),
                  () => {
                    state.setData(
                      current =>
                        current && {
                          ...current,
                          warehouses: current.warehouses.filter(item => item.whid !== deleting.whid),
                        }
                    );
                    setDeleting(undefined);
                  },
                  "Warehouse deleted."
                )
              }>
              {operation.busy ? "Deleting…" : "Delete"}
            </button>
          </footer>
        </Dialog>
      )}
    </main>
  );
}

interface PackageRow {
  name: string;
  versionOp: "==" | ">=" | "<=";
  version: string;
}
interface EnvironmentDraft {
  veid?: number;
  name: string;
  packages: PackageRow[];
}
function environmentDraft(record?: PythonEnvironment): EnvironmentDraft {
  return {
    veid: record?.veid,
    name: record?.name ?? "",
    packages: Object.entries(record?.packages ?? {}).map(([name, raw]) => {
      const match = raw?.match?.(/^(==|>=|<=)(.*)$/);
      return { name, versionOp: (match?.[1] ?? "==") as PackageRow["versionOp"], version: match?.[2] ?? raw ?? "" };
    }),
  };
}

export function PythonEnvironmentsPage({ resources, onNotice }: ResourcePageProps) {
  const load = useCallback((signal: AbortSignal) => resources.listEnvironments({ signal }), [resources]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [draft, setDraft] = useState<EnvironmentDraft>();
  const [deleting, setDeleting] = useState<PythonEnvironment>();
  function save() {
    if (!draft) return;
    const name = draft.name.trim();
    if (!name) {
      operation.fail("Environment name is required.");
      return;
    }
    if (!/^[a-zA-Z0-9]+$/.test(name)) {
      operation.fail("Environment name must contain only letters and numbers.");
      return;
    }
    if (state.data?.some(item => item.name.trim() === name && item.veid !== draft.veid)) {
      operation.fail(`An environment named "${name}" already exists.`);
      return;
    }
    const packages: Record<string, string> = Object.create(null);
    for (const row of draft.packages) {
      const key = row.name.trim();
      if (!key) continue;
      if (Object.hasOwn(packages, key)) {
        operation.fail(`Duplicate package "${key}".`);
        return;
      }
      const version = row.version.trim();
      packages[key] = version ? `${row.versionOp}${version}` : "";
    }
    void operation.run(
      () =>
        draft.veid === undefined
          ? resources.createEnvironment(name, packages)
          : resources.updateEnvironment(draft.veid, name, packages),
      () => {
        setDraft(undefined);
        state.refresh();
      },
      `Saved environment "${name}".`
    );
  }
  function updatePackage(index: number, patch: Partial<PackageRow>) {
    setDraft(
      current =>
        current && {
          ...current,
          packages: current.packages.map((row, item) => (item === index ? { ...row, ...patch } : row)),
        }
    );
  }
  return (
    <main className="resource-page">
      <header>
        <h1>Python environments</h1>
        <button
          type="button"
          disabled={state.loading || !!state.error}
          onClick={() => {
            operation.clear();
            setDraft(environmentDraft());
          }}>
          Create environment
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
      {state.data &&
        (state.data.length ? (
          <ul className="resource-list">
            {state.data.map(item => (
              <li key={item.veid}>
                <button
                  type="button"
                  className="resource-link"
                  aria-label={`Edit environment ${item.name}`}
                  onClick={() => {
                    operation.clear();
                    setDraft(environmentDraft(item));
                  }}>
                  {item.name || "(unnamed)"}
                </button>
                <span>{Object.keys(item.packages ?? {}).length} packages</span>
                <button
                  type="button"
                  disabled={operation.busy}
                  aria-label={`Delete environment ${item.name}`}
                  onClick={() => {
                    operation.clear();
                    setDeleting(item);
                  }}>
                  Delete
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p>No environments yet. Click Create environment to create one.</p>
        ))}
      {draft && (
        <Dialog
          title="Python environment"
          close={() => setDraft(undefined)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              save();
            }}>
            <label>
              Environment name
              <input
                value={draft.name}
                onChange={event => setDraft({ ...draft, name: event.target.value })}
                disabled={operation.busy}
              />
            </label>
            {draft.packages.map((row, index) => (
              <fieldset
                key={index}
                disabled={operation.busy}
                className="resource-package-row">
                <legend>Package {index + 1}</legend>
                <label>
                  Package {index + 1} name
                  <input
                    value={row.name}
                    onChange={event => updatePackage(index, { name: event.target.value })}
                  />
                </label>
                <label>
                  Package {index + 1} operator
                  <select
                    value={row.versionOp}
                    onChange={event =>
                      updatePackage(index, { versionOp: event.target.value as PackageRow["versionOp"] })
                    }>
                    {["==", ">=", "<="].map(operator => (
                      <option key={operator}>{operator}</option>
                    ))}
                  </select>
                </label>
                <label>
                  Package {index + 1} version
                  <input
                    value={row.version}
                    onChange={event => updatePackage(index, { version: event.target.value })}
                  />
                </label>
                <button
                  type="button"
                  aria-label={`Remove package ${index + 1}`}
                  onClick={() => setDraft({ ...draft, packages: draft.packages.filter((_, item) => item !== index) })}>
                  Remove
                </button>
              </fieldset>
            ))}
            <button
              type="button"
              disabled={operation.busy}
              onClick={() =>
                setDraft({ ...draft, packages: [...draft.packages, { name: "", versionOp: "==", version: "" }] })
              }>
              Add package
            </button>
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setDraft(undefined)}>
                Close
              </button>
              <button
                type="submit"
                disabled={operation.busy || !draft.name.trim()}>
                Save environment
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {deleting && (
        <Dialog
          title={`Delete environment "${deleting.name}"?`}
          close={() => setDeleting(undefined)}
          busy={operation.busy}>
          <p>This permanently removes the environment from the database.</p>
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
                  () => resources.deleteEnvironment(deleting.veid),
                  () => {
                    setDeleting(undefined);
                    state.refresh();
                  },
                  `Deleted environment "${deleting.name}".`
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

export function AdminMailPage({ resources, onNotice }: ResourcePageProps) {
  const load = useCallback((signal: AbortSignal) => resources.senderEmail({ signal }), [resources]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [email, setEmail] = useState("");
  const [subject, setSubject] = useState("");
  const [content, setContent] = useState("");
  return (
    <main className="resource-page">
      <header>
        <h1>Email administration</h1>
      </header>
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      {state.data !== undefined && (
        <p>
          Sender: <span>{state.data || "Not configured"}</span>
        </p>
      )}
      <Notices
        error={operation.error}
        message={operation.message}
      />
      <form
        className="resource-panel"
        noValidate
        onSubmit={event => {
          event.preventDefault();
          if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim()) || !subject.trim() || !content.trim()) {
            operation.fail("Enter a valid recipient email, subject and message.");
            return;
          }
          void operation.run(
            () => resources.sendEmail(subject, content, email.trim()),
            () => {},
            "Email sent successfully"
          );
        }}>
        <label>
          Recipient email
          <input
            type="email"
            value={email}
            onChange={event => setEmail(event.target.value)}
            disabled={operation.busy}
          />
        </label>
        <label>
          Subject
          <input
            value={subject}
            onChange={event => setSubject(event.target.value)}
            disabled={operation.busy}
          />
        </label>
        <label>
          Message
          <textarea
            rows={10}
            value={content}
            onChange={event => setContent(event.target.value)}
            disabled={operation.busy}
          />
        </label>
        <button
          type="submit"
          disabled={operation.busy}>
          Send test email
        </button>
      </form>
    </main>
  );
}

const SIDEBAR_TABS = [
  ["hub_enabled", "Hub"],
  ["home_enabled", "Home", "hub_enabled"],
  ["workflow_enabled", "Hub workflows", "hub_enabled"],
  ["dataset_enabled", "Hub datasets", "hub_enabled"],
  ["model_enabled", "Hub models", "hub_enabled"],
  ["your_work_enabled", "Your work"],
  ["workflows_enabled", "Workflows", "your_work_enabled"],
  ["compute_enabled", "Compute", "your_work_enabled"],
  ["datasets_enabled", "Datasets", "your_work_enabled"],
  ["models_enabled", "Models", "your_work_enabled"],
  ["quota_enabled", "Quota", "your_work_enabled"],
  ["forum_enabled", "Forum", "your_work_enabled"],
  ["about_enabled", "About"],
] as const;
const BRANDING = [
  ["logo", "Logo"],
  ["mini_logo", "Mini logo"],
  ["favicon", "Favicon"],
] as const;
interface UploadGroup {
  label: string;
  family: "dataset" | "model";
  files: number;
  fileSize: number;
  chunks: number;
  chunkSize: number;
}
const defaultUploads = (): UploadGroup[] => [
  { label: "Dataset", family: "dataset", files: 3, fileSize: 20, chunks: 10, chunkSize: 50 },
  { label: "Model", family: "model", files: 3, fileSize: 2048, chunks: 10, chunkSize: 50 },
];
function uploadEntries(group: UploadGroup): [string, number][] {
  return [
    [`${group.family}_max_number_of_concurrent_uploading_file`, group.files],
    [`${group.family}_single_file_upload_max_size_mib`, group.fileSize],
    [`${group.family}_max_number_of_concurrent_uploading_file_chunks`, group.chunks],
    [`${group.family}_multipart_upload_chunk_size_mib`, group.chunkSize],
  ];
}
function readInteger(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

export function AdminSettingsPage({
  resources,
  onNotice,
  onSettingsChanged = () => window.location.reload(),
}: ResourcePageProps & { onSettingsChanged?: () => void }) {
  const load = useCallback((signal: AbortSignal) => resources.settings({ signal }), [resources]);
  const state = useResource(load);
  const operation = useOperation(onNotice);
  const [tabs, setTabs] = useState<Record<string, boolean>>({});
  const [logos, setLogos] = useState<Record<string, string>>({});
  const [uploads, setUploads] = useState(defaultUploads);
  const [csvColumns, setCsvColumns] = useState(512);
  useEffect(() => {
    if (!state.data) return;
    const settings = state.data;
    setTabs(Object.fromEntries(SIDEBAR_TABS.map(([key]) => [key, settings[key] === "true"])));
    setLogos(Object.fromEntries(BRANDING.map(([key]) => [key, settings[key] ?? ""])));
    setUploads(
      defaultUploads().map(group => ({
        ...group,
        files: readInteger(settings[`${group.family}_max_number_of_concurrent_uploading_file`], group.files),
        fileSize: readInteger(settings[`${group.family}_single_file_upload_max_size_mib`], group.fileSize),
        chunks: readInteger(settings[`${group.family}_max_number_of_concurrent_uploading_file_chunks`], group.chunks),
        chunkSize: readInteger(settings[`${group.family}_multipart_upload_chunk_size_mib`], group.chunkSize),
      }))
    );
    setCsvColumns(readInteger(settings.csv_parser_max_columns, 512));
  }, [state.data]);
  const disabled = state.data === undefined || operation.busy;
  const save = (entries: [string, string][], message: string, reload = false) => {
    if (disabled) return;
    void operation.run(
      () => Promise.all(entries.map(([key, value]) => resources.updateSetting(key, value))),
      () => {
        if (reload) onSettingsChanged();
      },
      message
    );
  };
  const reset = (keys: string[], message: string) => {
    if (disabled) return;
    void operation.run(() => Promise.all(keys.map(key => resources.resetSetting(key))), onSettingsChanged, message);
  };
  function chooseImage(key: string, file?: File) {
    if (!file) return;
    if (!file.type.startsWith("image/")) {
      operation.fail("Please upload a valid image file.");
      return;
    }
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result === "string") setLogos(current => ({ ...current, [key]: reader.result as string }));
    };
    reader.onerror = () => operation.fail("Could not read the image file.");
    reader.readAsDataURL(file);
  }
  function saveUploads(group: UploadGroup) {
    if (uploadEntries(group).some(([, value]) => !Number.isSafeInteger(value) || value < 1)) {
      operation.fail("Please enter valid integer values.");
      return;
    }
    if (group.chunkSize < 5 || group.chunkSize > 5120 || group.fileSize > 5242880) {
      operation.fail("Part size must be between 5 and 5120 MiB; maximum file size is 5242880 MiB.");
      return;
    }
    if (Math.ceil(group.fileSize / group.chunkSize) > 10000) {
      operation.fail(
        `This setting exceeds the 10,000 parts limit. Increase part size to at least ${Math.max(5, Math.ceil(group.fileSize / 10000))} MiB or reduce file size.`
      );
      return;
    }
    save(
      uploadEntries(group).map(([key, value]) => [key, String(value)]),
      `${group.label} upload settings saved successfully.`
    );
  }
  return (
    <main className="resource-page">
      <header>
        <h1>General settings</h1>
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
      <section className="resource-panel">
        <h2>Branding</h2>
        {BRANDING.map(([key, label]) => (
          <div
            key={key}
            className="resource-branding">
            <label>
              {label} image
              <input
                type="file"
                accept="image/*"
                disabled={disabled}
                onChange={event => chooseImage(key, event.target.files?.[0])}
              />
            </label>
            {logos[key] && (
              <img
                src={logos[key]}
                alt={`${label} preview`}
              />
            )}
          </div>
        ))}
        <footer>
          <button
            type="button"
            disabled={disabled}
            onClick={() =>
              save(
                BRANDING.filter(([key]) => logos[key]).map(([key]) => [key, logos[key]]),
                "Branding saved successfully.",
                true
              )
            }>
            Save branding
          </button>
          <button
            type="button"
            disabled={disabled}
            onClick={() =>
              reset(
                BRANDING.map(([key]) => key),
                "Branding reset."
              )
            }>
            Reset branding
          </button>
        </footer>
      </section>
      <section className="resource-panel">
        <h2>Sidebar tabs</h2>
        <div className="resource-toggles">
          {SIDEBAR_TABS.map(tab => (
            <label key={tab[0]}>
              <input
                type="checkbox"
                checked={tabs[tab[0]] ?? false}
                disabled={disabled || (tab[2] !== undefined && !tabs[tab[2]])}
                onChange={event => setTabs({ ...tabs, [tab[0]]: event.target.checked })}
              />
              {tab[1]}
            </label>
          ))}
        </div>
        <footer>
          <button
            type="button"
            disabled={disabled}
            onClick={() =>
              save(
                SIDEBAR_TABS.map(([key]) => [key, String(tabs[key] ?? false)]),
                "Tabs saved successfully.",
                true
              )
            }>
            Save sidebar tabs
          </button>
          <button
            type="button"
            disabled={disabled}
            onClick={() =>
              reset(
                SIDEBAR_TABS.map(([key]) => key),
                "Sidebar tabs reset."
              )
            }>
            Reset sidebar tabs
          </button>
        </footer>
      </section>
      {uploads.map((group, index) => (
        <section
          key={group.family}
          className="resource-panel">
          <h2>{group.label} uploads</h2>
          <fieldset
            disabled={disabled}
            className="resource-input-grid">
            {(
              [
                ["files", "max concurrent files", 1, undefined],
                ["fileSize", "max file size (MiB)", 1, 5242880],
                ["chunks", "max concurrent chunks", 1, undefined],
                ["chunkSize", "part size (MiB)", 5, 5120],
              ] as const
            ).map(([key, label, min, max]) => (
              <label key={key}>
                {group.label} {label}
                <input
                  type="number"
                  min={min}
                  max={max}
                  step={1}
                  value={group[key]}
                  onChange={event =>
                    setUploads(current =>
                      current.map((item, position) =>
                        position === index ? { ...item, [key]: Number(event.target.value) } : item
                      )
                    )
                  }
                />
              </label>
            ))}
          </fieldset>
          <p>Parts at maximum file size: {Math.ceil(group.fileSize / group.chunkSize).toLocaleString()} / 10,000.</p>
          <footer>
            <button
              type="button"
              disabled={disabled}
              onClick={() => saveUploads(group)}>
              Save {group.family} upload settings
            </button>
            <button
              type="button"
              disabled={disabled}
              onClick={() =>
                reset(
                  uploadEntries(group).map(([key]) => key),
                  `${group.label} upload settings reset.`
                )
              }>
              Reset {group.family} upload settings
            </button>
          </footer>
        </section>
      ))}
      <section className="resource-panel">
        <h2>Result panel settings</h2>
        <label>
          Maximum CSV columns
          <input
            type="number"
            min={1}
            max={100000}
            step={1}
            value={csvColumns}
            disabled={disabled}
            onChange={event => setCsvColumns(Number(event.target.value))}
          />
        </label>
        <footer>
          <button
            type="button"
            disabled={disabled}
            onClick={() => {
              if (!Number.isSafeInteger(csvColumns) || csvColumns < 1 || csvColumns > 100000)
                operation.fail("Maximum CSV columns must be an integer between 1 and 100000.");
              else save([["csv_parser_max_columns", String(csvColumns)]], "Result panel settings saved.");
            }}>
            Save result panel settings
          </button>
          <button
            type="button"
            disabled={disabled}
            onClick={() => reset(["csv_parser_max_columns"], "Result panel settings reset.")}>
            Reset result panel settings
          </button>
        </footer>
      </section>
    </main>
  );
}
