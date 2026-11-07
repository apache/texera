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
import { useCallback, useEffect, useMemo, useState } from "react";
import Papa from "papaparse";
import readXlsxFile from "read-excel-file";
import type { ApiClient } from "../core/api-client";
import type { SessionUser } from "../core/auth";
import { readFileBytes } from "../workflows/workflow-repository";
import { RichText } from "../common/rich-text";
import { HubSearchPage } from "../hub/hub-pages";
import { HubRepository, activityKey, type HubItem } from "../hub/hub-repository";
import { Dialog, LoadState, Notices, useOperation, useResource, type ResourcePageProps } from "./pages";
import { formatQuotaSize } from "./quota";
import {
  StorageApi,
  MODEL_FORMATS,
  MODEL_FRAMEWORKS,
  validStorageName,
  type Contributor,
  type ResourceKind,
  type StorageCreate,
  type StorageEntry,
  type StorageFileNode,
} from "./storage-api";
import { ResourceUploadPanel } from "./storage-upload";
import { ResourceFileTree } from "./file-tree";
export { ResourceUploadPanel } from "./storage-upload";

export interface StoragePageProps {
  api: ApiClient;
  storage: StorageApi;
  kind: ResourceKind;
  user?: SessionUser;
  onNavigate?: (path: string) => void;
  onShare?: (kind: ResourceKind, id: number) => void;
  onNotice?: ResourcePageProps["onNotice"];
}
export function sanitizeStorageName(kind: ResourceKind, name: string): string {
  return name
    .trimStart()
    .replace(kind === "model" ? /[^a-zA-Z0-9_-]+/g : /[^a-zA-Z0-9]+/g, "-")
    .toLowerCase();
}
function saveBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
const titleCase = (kind: string) => kind[0].toUpperCase() + kind.slice(1);

export function ResourceCreatePage({
  storage,
  kind,
  onCreated,
  onNotice,
}: StoragePageProps & { onCreated?: (entry: StorageEntry) => void }) {
  const [form, setForm] = useState<StorageCreate>({
    name: "",
    description: "",
    isPublic: false,
    isDownloadable: false,
    framework: MODEL_FRAMEWORKS[0],
    format: MODEL_FORMATS[0],
  });
  const [contributors, setContributors] = useState<Contributor[]>([]);
  const operation = useOperation(onNotice);
  return (
    <main className="resource-page">
      <h1>Create {kind}</h1>
      <Notices
        error={operation.error}
        message={operation.message}
      />
      <form
        noValidate
        className="resource-panel"
        onSubmit={event => {
          event.preventDefault();
          const name = sanitizeStorageName(kind, form.name);
          if (!form.name.trim() || !validStorageName(name)) {
            operation.fail(
              "Resource name must contain letters, numbers, hyphens or underscores and be at most 128 characters."
            );
            return;
          }
          if (contributors.some(person => !person.name.trim())) {
            operation.fail("Every contributor needs a name.");
            return;
          }
          void operation.run(
            () => storage.create(kind, { ...form, name }, contributors),
            result => onCreated?.(result),
            `Created ${kind} "${name}"${name !== form.name ? " (name sanitized)" : ""}.`
          );
        }}>
        <MetadataFields
          kind={kind}
          form={form}
          change={setForm}
          disabled={operation.busy}
        />
        {kind === "dataset" && (
          <ContributorFields
            value={contributors}
            change={setContributors}
            disabled={operation.busy}
          />
        )}
        <footer>
          <button
            type="submit"
            disabled={operation.busy}>
            Create {kind}
          </button>
        </footer>
      </form>
    </main>
  );
}
function MetadataFields({
  kind,
  form,
  change,
  disabled,
}: {
  kind: ResourceKind;
  form: StorageCreate;
  change: (form: StorageCreate) => void;
  disabled: boolean;
}) {
  return (
    <fieldset
      disabled={disabled}
      className="resource-form-grid">
      <label>
        Resource name
        <input
          value={form.name}
          onChange={event => change({ ...form, name: event.target.value })}
        />
      </label>
      <label>
        Resource description
        <textarea
          value={form.description}
          rows={5}
          onChange={event => change({ ...form, description: event.target.value })}
        />
      </label>
      <label className="resource-checkbox">
        <input
          type="checkbox"
          checked={form.isPublic}
          onChange={event => change({ ...form, isPublic: event.target.checked })}
        />
        Public resource
      </label>
      <label className="resource-checkbox">
        <input
          type="checkbox"
          checked={form.isDownloadable}
          onChange={event => change({ ...form, isDownloadable: event.target.checked })}
        />
        Allow downloads
      </label>
      {kind === "model" && (
        <>
          <label>
            Model framework
            <select
              value={form.framework}
              onChange={event => change({ ...form, framework: event.target.value })}>
              {MODEL_FRAMEWORKS.map(value => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </label>
          <label>
            Model format
            <select
              value={form.format}
              onChange={event => change({ ...form, format: event.target.value })}>
              {MODEL_FORMATS.map(value => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </label>
        </>
      )}
    </fieldset>
  );
}
function ContributorFields({
  value,
  change,
  disabled,
}: {
  value: Contributor[];
  change: (value: Contributor[]) => void;
  disabled: boolean;
}) {
  return (
    <section>
      <h2>Contributors</h2>
      {value.map((person, index) => (
        <fieldset
          key={index}
          disabled={disabled}
          className="resource-panel">
          <legend>Contributor {index + 1}</legend>
          {(["name", "email", "affiliation", "comments"] as const).map(key => (
            <label key={key}>
              Contributor {index + 1} {key}
              <input
                value={person[key] ?? ""}
                onChange={event =>
                  change(
                    value.map((item, position) => (position === index ? { ...item, [key]: event.target.value } : item))
                  )
                }
              />
            </label>
          ))}
          <label className="resource-checkbox">
            <input
              type="checkbox"
              checked={person.creator}
              onChange={event =>
                change(
                  value.map((item, position) =>
                    position === index ? { ...item, creator: event.target.checked } : item
                  )
                )
              }
            />
            Creator
          </label>
          <button
            type="button"
            onClick={() => change(value.filter((_, position) => position !== index))}>
            Remove contributor {index + 1}
          </button>
        </fieldset>
      ))}
      <button
        type="button"
        disabled={disabled}
        onClick={() => change([...value, { name: "", creator: false }])}>
        Add contributor
      </button>
    </section>
  );
}

export function StorageResourceListPage(props: StoragePageProps) {
  const { api, storage, kind, user, onNavigate, onShare, onNotice } = props;
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState<HubItem>();
  const [revision, setRevision] = useState(0);
  const operation = useOperation(onNotice);
  return (
    <>
      <Notices
        error={operation.error}
        message={operation.message}
      />
      <HubSearchPage
        api={api}
        user={user}
        kind={kind}
        scope="private"
        refreshKey={revision}
        onOpen={(kind, id) => onNavigate?.(`/user/${kind}/${id}`)}
        actions={
          <button
            type="button"
            onClick={() => setCreating(true)}>
            Create {kind}
          </button>
        }
        renderActions={item => (
          <>
            {onShare && (
              <button
                type="button"
                onClick={() => onShare(kind, item.id)}>
                Share {item.name}
              </button>
            )}
            <button
              type="button"
              onClick={() =>
                void operation.run(
                  () => storage.versionZip(kind, item.id),
                  blob => saveBlob(blob, `${item.name}.zip`),
                  "Download ready."
                )
              }>
              Download {item.name}
            </button>
            {user?.uid === item.ownerId && (
              <button
                type="button"
                onClick={() => setDeleting(item)}>
                Delete {item.name}
              </button>
            )}
          </>
        )}
      />
      {creating && (
        <Dialog
          title={`Create ${kind}`}
          close={() => setCreating(false)}>
          <ResourceCreatePage
            {...props}
            onCreated={entry => {
              setCreating(false);
              setRevision(value => value + 1);
              onNavigate?.(`/user/${kind}/${entry.id}`);
            }}
          />
          <button
            type="button"
            onClick={() => setCreating(false)}>
            Cancel
          </button>
        </Dialog>
      )}
      {deleting && (
        <Dialog
          title={`Delete ${kind} "${deleting.name}"?`}
          close={() => setDeleting(undefined)}
          busy={operation.busy}>
          <p>This permanently deletes this {kind} and its versions.</p>
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
                  () => storage.delete(kind, deleting.id),
                  () => {
                    setDeleting(undefined);
                    setRevision(value => value + 1);
                  },
                  `${titleCase(kind)} deleted.`
                )
              }>
              Delete
            </button>
          </footer>
        </Dialog>
      )}
    </>
  );
}

export interface ResourceDetailProps extends StoragePageProps {
  id: number;
  publicView?: boolean;
  onSaveBlob?: (blob: Blob, name: string) => void;
}
export function ResourceDetailPage({
  api,
  storage,
  kind,
  id,
  user,
  publicView = false,
  onShare,
  onNotice,
  onSaveBlob = saveBlob,
}: ResourceDetailProps) {
  const load = useCallback(
    (signal: AbortSignal) => storage.get(kind, id, publicView, { signal }),
    [storage, kind, id, publicView]
  );
  const state = useResource(load);
  const loadVersions = useCallback(
    (signal: AbortSignal) => storage.versions(kind, id, publicView, { signal }),
    [storage, kind, id, publicView]
  );
  const versions = useResource(loadVersions);
  const [versionId, setVersionId] = useState<number>();
  useEffect(() => {
    if (versions.data)
      setVersionId(previous =>
        versions.data!.some(item => item.versionId === previous) ? previous : versions.data![0]?.versionId
      );
  }, [versions.data]);
  const latestVersion = versions.data?.[0];
  const selectedVersion = versions.data?.find(item => item.versionId === versionId);
  const loadLatestTree = useCallback(
    (signal: AbortSignal) =>
      latestVersion
        ? storage.fileTree(kind, id, latestVersion.versionId, publicView, { signal })
        : Promise.resolve({ fileNodes: [], size: 0 }),
    [storage, kind, id, publicView, latestVersion]
  );
  const latestTree = useResource(loadLatestTree);
  const loadTree = useCallback(
    (signal: AbortSignal) =>
      versionId === undefined
        ? Promise.resolve({ fileNodes: [], size: 0 })
        : versionId === latestVersion?.versionId && latestTree.data
          ? Promise.resolve(latestTree.data)
          : storage.fileTree(kind, id, versionId, publicView, { signal }),
    [storage, kind, id, publicView, versionId, latestVersion?.versionId, latestTree.data]
  );
  const tree = useResource(loadTree);
  const writable = !publicView && !!state.data && (state.data.isOwner || state.data.accessPrivilege === "WRITE");
  const loadDiff = useCallback(
    (signal: AbortSignal) => (writable ? storage.diff(kind, id, { signal }) : Promise.resolve([])),
    [storage, kind, id, writable]
  );
  const diff = useResource(loadDiff);
  const coverLoad = useCallback(
    (_signal: AbortSignal) => storage.coverUrl(kind, id).catch(() => ({ url: null })),
    [storage, kind, id]
  );
  const cover = useResource(coverLoad);
  const operation = useOperation(onNotice);
  const [editing, setEditing] = useState<StorageCreate>();
  const [contributors, setContributors] = useState<Contributor[] | undefined>();
  const [newVersion, setNewVersion] = useState("");
  const [selectedFile, setSelectedFile] = useState<StorageFileNode>();
  const [deletingFile, setDeletingFile] = useState<StorageFileNode>();
  const [uploadsBusy, setUploadsBusy] = useState(false);
  const hub = useMemo(() => new HubRepository(api), [api]);
  const [views, setViews] = useState(0);
  const [likes, setLikes] = useState(0);
  const [liked, setLiked] = useState(false);
  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    const item: HubItem = { kind, id, name: "", description: "", accessLevel: "NONE" };
    void Promise.all([hub.activity([item], user?.uid, controller.signal), hub.recordView(kind, id, user?.uid ?? 0)])
      .then(([activity, count]) => {
        if (active) {
          setViews(count);
          setLikes(activity[activityKey(item)]?.likes ?? 0);
          setLiked(activity[activityKey(item)]?.isLiked ?? false);
        }
      })
      .catch(() => {});
    return () => {
      active = false;
      controller.abort();
    };
  }, [hub, kind, id, user?.uid]);
  useEffect(() => {
    setSelectedFile(undefined);
  }, [versionId]);
  useEffect(() => {
    if (!tree.data) return;
    const files = leafFiles(tree.data.fileNodes);
    setSelectedFile(
      previous => files.find(file => previous && relativePath(file) === relativePath(previous)) ?? files[0]
    );
  }, [tree.data]);
  const entry = state.data;
  const canDownload = !!entry && (entry.resource.isDownloadable || entry.isOwner);
  function relativePath(node: StorageFileNode): string {
    return `${node.parentDir}/${node.name}`.split("/").filter(Boolean).slice(4).join("/");
  }
  function refreshVersions() {
    versions.refresh();
    latestTree.refresh();
    diff.refresh();
  }
  return (
    <main className="resource-page resource-detail">
      <LoadState
        loading={state.loading}
        error={state.error}
        retry={state.refresh}
      />
      <Notices
        error={operation.error}
        message={operation.message}
      />
      {entry && (
        <>
          <header>
            <div>
              <h1>
                {titleCase(kind)}: {entry.resource.name}
              </h1>
              <p>
                Created:{" "}
                {entry.resource.creationTime === undefined
                  ? "—"
                  : new Date(entry.resource.creationTime).toLocaleString()}
              </p>
              <p>
                {entry.resource.isPublic ? "Public" : "Private"} ·{" "}
                {canDownload ? "Downloadable" : "Download restricted"} · {views} views · {likes} likes
              </p>
              <button
                type="button"
                disabled={!user || operation.busy}
                aria-pressed={liked}
                onClick={() =>
                  void operation.run(
                    () => hub.toggleLike(kind, id, liked),
                    value => {
                      setLiked(value.liked);
                      setLikes(value.likeCount);
                    },
                    liked ? "Like removed." : "Liked."
                  )
                }>
                {liked ? "Unlike" : "Like"}
              </button>
            </div>
            {cover.data?.url && (
              <img
                className="resource-cover"
                src={cover.data.url}
                alt={`${titleCase(kind)} cover`}
              />
            )}
          </header>
          <section className="resource-panel">
            <h2>Description</h2>
            {entry.resource.description ? (
              <RichText
                content={entry.resource.description}
                collapsedHeight={280}
              />
            ) : (
              <p>No description provided.</p>
            )}
            <dl className="resource-input-grid">
              <div>
                <dt>Versions</dt>
                <dd>{versions.data?.length ?? "—"}</dd>
              </div>
              <div>
                <dt>Latest version</dt>
                <dd>{latestVersion?.name ?? "—"}</dd>
              </div>
              <div>
                <dt>Last updated</dt>
                <dd>
                  {latestVersion?.creationTime === undefined
                    ? "—"
                    : new Date(latestVersion.creationTime).toLocaleString()}
                </dd>
              </div>
              <div>
                <dt>Latest version size</dt>
                <dd>{formatQuotaSize(latestTree.data?.size ?? 0)}</dd>
              </div>
              <div>
                <dt>Latest file</dt>
                <dd>{leafFiles(latestTree.data?.fileNodes ?? [])[0]?.name ?? "—"}</dd>
              </div>
              {kind === "model" && (
                <>
                  <div>
                    <dt>Framework</dt>
                    <dd>{entry.resource.framework ?? "—"}</dd>
                  </div>
                  <div>
                    <dt>Format</dt>
                    <dd>{entry.resource.format ?? "—"}</dd>
                  </div>
                </>
              )}
            </dl>
            {kind === "dataset" && (
              <>
                <h3>Contributors</h3>
                {entry.contributors?.length ? (
                  <ul>
                    {entry.contributors.map((person, index) => (
                      <li key={index}>
                        {person.name}
                        {person.creator ? " · Creator" : ""}
                        {person.affiliation ? ` · ${person.affiliation}` : ""}
                        {person.email ? ` · ${person.email}` : ""}
                        {person.comments && <p>{person.comments}</p>}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p>No contributors listed.</p>
                )}
                {writable && (
                  <button
                    type="button"
                    onClick={() => setContributors((entry.contributors ?? []).map(person => ({ ...person })))}>
                    Edit contributors
                  </button>
                )}
              </>
            )}
            {writable && (
              <footer>
                <button
                  type="button"
                  disabled={operation.busy}
                  onClick={() =>
                    setEditing({
                      name: entry.resource.name,
                      description: entry.resource.description,
                      isPublic: entry.resource.isPublic,
                      isDownloadable: entry.resource.isDownloadable,
                      framework: entry.resource.framework ?? MODEL_FRAMEWORKS[0],
                      format: entry.resource.format ?? MODEL_FORMATS[0],
                    })
                  }>
                  Edit metadata
                </button>
                {onShare && (
                  <button
                    type="button"
                    onClick={() => onShare(kind, id)}>
                    Share {kind}
                  </button>
                )}
              </footer>
            )}
          </section>
          <section className="resource-panel">
            <h2>Versions and files</h2>
            <LoadState
              loading={versions.loading}
              error={versions.error}
              retry={versions.refresh}
            />
            {versions.data?.length ? (
              <>
                <label>
                  Resource version
                  <select
                    value={versionId ?? ""}
                    onChange={event => setVersionId(Number(event.target.value))}>
                    {versions.data.map(version => (
                      <option
                        key={version.versionId}
                        value={version.versionId}>
                        {version.name}
                      </option>
                    ))}
                  </select>
                </label>
                <p>
                  {selectedVersion?.creationTime === undefined
                    ? ""
                    : new Date(selectedVersion.creationTime).toLocaleString()}
                </p>
                {canDownload && (
                  <button
                    type="button"
                    disabled={operation.busy}
                    onClick={() =>
                      void operation.run(
                        () => storage.versionZip(kind, id, versionId),
                        blob => onSaveBlob(blob, `${entry.resource.name}-${selectedVersion?.name ?? "latest"}.zip`),
                        "Version download ready."
                      )
                    }>
                    Download version ZIP
                  </button>
                )}
                <LoadState
                  loading={tree.loading}
                  error={tree.error}
                  retry={tree.refresh}
                />
                <div className="resource-file-layout">
                  <ResourceFileTree
                    nodes={tree.data?.fileNodes ?? []}
                    onOpen={setSelectedFile}
                    selectedPath={selectedFile ? `${selectedFile.parentDir}/${selectedFile.name}` : undefined}
                  />
                  {selectedFile && (
                    <FilePreview
                      storage={storage}
                      kind={kind}
                      node={selectedFile}
                      publicView={publicView}
                      canDownload={canDownload}
                      onSaveBlob={onSaveBlob}
                    />
                  )}
                </div>
                {writable &&
                  selectedFile &&
                  selectedVersion &&
                  latestVersion &&
                  selectedVersion.versionId === latestVersion.versionId && (
                    <footer>
                      <button
                        type="button"
                        aria-label={`Stage deletion ${selectedFile.name}`}
                        onClick={() => setDeletingFile(selectedFile)}>
                        Stage file deletion
                      </button>
                      {/\.(png|jpe?g|gif|webp|svg)$/i.test(selectedFile.name) && (
                        <button
                          type="button"
                          onClick={() =>
                            void operation.run(
                              () =>
                                storage.updateCover(kind, id, `${selectedVersion.name}/${relativePath(selectedFile)}`),
                              cover.refresh,
                              "Cover image updated."
                            )
                          }>
                          Use as cover image
                        </button>
                      )}
                    </footer>
                  )}
              </>
            ) : !versions.loading && !versions.error ? (
              <p>No versions yet. Upload files and create the first version.</p>
            ) : null}
          </section>
          {writable && (
            <>
              <ResourceUploadPanel
                storage={storage}
                kind={kind}
                id={id}
                name={entry.resource.name}
                ownerEmail={entry.ownerEmail}
                onChanged={diff.refresh}
                onBusyChange={setUploadsBusy}
              />
              <section className="resource-panel">
                <h2>Staged changes</h2>
                <LoadState
                  loading={diff.loading}
                  error={diff.error}
                  retry={diff.refresh}
                />
                {diff.data?.length ? (
                  <table>
                    <thead>
                      <tr>
                        <th>Path</th>
                        <th>Change</th>
                        <th>Size</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {diff.data.map(item => (
                        <tr key={item.path}>
                          <td>{item.path}</td>
                          <td>{item.diffType}</td>
                          <td>{formatQuotaSize(item.sizeBytes ?? 0)}</td>
                          <td>
                            <button
                              type="button"
                              aria-label={`Revert staged file ${item.path}`}
                              disabled={operation.busy || uploadsBusy}
                              onClick={() =>
                                void operation.run(
                                  () => storage.resetFile(kind, id, item.path),
                                  diff.refresh,
                                  "Staged change reverted."
                                )
                              }>
                              Revert
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  !diff.loading && <p>No staged changes.</p>
                )}
                <form
                  onSubmit={event => {
                    event.preventDefault();
                    if (uploadsBusy || !diff.data?.length) return;
                    void operation.run(
                      () => storage.createVersion(kind, id, newVersion),
                      () => {
                        setNewVersion("");
                        refreshVersions();
                      },
                      "Version created."
                    );
                  }}>
                  <label>
                    New version name
                    <input
                      value={newVersion}
                      disabled={operation.busy || uploadsBusy}
                      onChange={event => setNewVersion(event.target.value)}
                      placeholder="Optional; the server can name it"
                    />
                  </label>
                  <button
                    type="submit"
                    disabled={operation.busy || uploadsBusy || !diff.data?.length}>
                    Create version
                  </button>
                </form>
              </section>
            </>
          )}
        </>
      )}
      {entry && editing && (
        <Dialog
          title="Edit resource metadata"
          close={() => setEditing(undefined)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              if (uploadsBusy && editing.name !== entry.resource.name) {
                operation.fail(`Finish or cancel the upload in progress before renaming this ${kind}.`);
                return;
              }
              if (!validStorageName(editing.name)) {
                operation.fail(
                  "Resource name must contain letters, numbers, hyphens or underscores and be at most 128 characters."
                );
                return;
              }
              const edits = (
                ["name", "description", ...(kind === "model" ? ["framework", "format"] : [])] as (
                  | "name"
                  | "description"
                  | "framework"
                  | "format"
                )[]
              )
                .filter(field => editing[field] !== entry.resource[field])
                .map(field => async () => {
                  const value = editing[field] ?? "";
                  await storage.update(kind, id, field, value);
                  state.setData(
                    current => current && { ...current, resource: { ...current.resource, [field]: value } }
                  );
                  if (field === "name") {
                    tree.refresh();
                    latestTree.refresh();
                    diff.refresh();
                    cover.refresh();
                  }
                });
              if (editing.isPublic !== entry.resource.isPublic)
                edits.push(async () => {
                  await storage.toggle(kind, id, "publicity");
                  state.setData(
                    current => current && { ...current, resource: { ...current.resource, isPublic: editing.isPublic } }
                  );
                });
              if (editing.isDownloadable !== entry.resource.isDownloadable)
                edits.push(async () => {
                  await storage.toggle(kind, id, "downloadable");
                  state.setData(
                    current =>
                      current && {
                        ...current,
                        resource: { ...current.resource, isDownloadable: editing.isDownloadable },
                      }
                  );
                });
              void operation.run(
                async () => {
                  for (const write of edits) await write();
                },
                () => {
                  setEditing(undefined);
                },
                "Metadata updated."
              );
            }}>
            <MetadataFields
              kind={kind}
              form={editing}
              change={setEditing}
              disabled={operation.busy}
            />
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setEditing(undefined)}>
                Cancel
              </button>
              <button
                type="submit"
                disabled={operation.busy}>
                Save metadata
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {contributors && (
        <Dialog
          title="Edit contributors"
          close={() => setContributors(undefined)}
          busy={operation.busy}>
          <form
            onSubmit={event => {
              event.preventDefault();
              if (contributors.some(person => !person.name.trim())) {
                operation.fail("Every contributor needs a name.");
                return;
              }
              void operation.run(
                () => storage.contributors(id, contributors),
                () => {
                  state.setData(current => current && { ...current, contributors });
                  setContributors(undefined);
                },
                "Contributors updated."
              );
            }}>
            <ContributorFields
              value={contributors}
              change={setContributors}
              disabled={operation.busy}
            />
            <footer>
              <button
                type="button"
                disabled={operation.busy}
                onClick={() => setContributors(undefined)}>
                Cancel
              </button>
              <button
                type="submit"
                disabled={operation.busy}>
                Save contributors
              </button>
            </footer>
          </form>
        </Dialog>
      )}
      {deletingFile && (
        <Dialog
          title={`Stage deletion of ${deletingFile.name}?`}
          close={() => setDeletingFile(undefined)}
          busy={operation.busy}>
          <p>The file is removed when the next version is committed. Existing versions remain available.</p>
          <footer>
            <button
              type="button"
              disabled={operation.busy}
              onClick={() => setDeletingFile(undefined)}>
              Cancel
            </button>
            <button
              type="button"
              disabled={operation.busy}
              onClick={() =>
                void operation.run(
                  () => storage.deleteFile(kind, id, relativePath(deletingFile)),
                  () => {
                    setDeletingFile(undefined);
                    diff.refresh();
                  },
                  "File deletion staged."
                )
              }>
              Stage deletion
            </button>
          </footer>
        </Dialog>
      )}
    </main>
  );
}
function blobText(blob: Blob): Promise<string> {
  if (typeof blob.text === "function") return blob.text();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = reject;
    reader.readAsText(blob);
  });
}
function leafFiles(nodes: StorageFileNode[]): StorageFileNode[] {
  return nodes.flatMap(node => (node.type === "directory" ? leafFiles(node.children ?? []) : [node]));
}
function previewLimit(extension: string): number | undefined {
  if (["png", "jpg", "jpeg", "webp", "svg", "bmp"].includes(extension)) return 5 * 1024 * 1024;
  if (["gif", "xlsx", "xls", "mp3", "wav", "flac", "m4a"].includes(extension)) return 10 * 1024 * 1024;
  if (["mp4", "webm", "ogg", "mov"].includes(extension)) return 50 * 1024 * 1024;
  if (["csv", "tsv"].includes(extension)) return 2 * 1024 * 1024;
  if (
    [
      "json",
      "jsonl",
      "txt",
      "md",
      "html",
      "htm",
      "py",
      "scala",
      "java",
      "js",
      "ts",
      "sql",
      "r",
      "xml",
      "yaml",
      "yml",
      "log",
    ].includes(extension)
  )
    return 1024 * 1024;
  return undefined;
}
function FilePreview({
  storage,
  kind,
  node,
  publicView,
  canDownload,
  onSaveBlob,
}: {
  storage: StorageApi;
  kind: ResourceKind;
  node: StorageFileNode;
  publicView: boolean;
  canDownload: boolean;
  onSaveBlob: (blob: Blob, name: string) => void;
}) {
  const extension = node.name.toLowerCase().split(".").at(-1) ?? "";
  const limit = previewLimit(extension);
  const tooLarge = limit !== undefined && node.size !== undefined && node.size > limit;
  const blocked = tooLarge || limit === undefined;
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (blocked) return undefined;
      const blob = await storage.file(kind, `${node.parentDir}/${node.name}`, publicView, signal);
      if (blob.size > limit!) throw new Error("This file is too large to preview.");
      return blob;
    },
    [storage, kind, node, publicView, blocked, limit]
  );
  const state = useResource(load);
  const operation = useOperation();
  const [text, setText] = useState<string>();
  const [url, setUrl] = useState<string>();
  const [spreadsheetRows, setSpreadsheetRows] = useState<string[][]>();
  const [previewError, setPreviewError] = useState<string>();
  const [previewPage, setPreviewPage] = useState(0);
  useEffect(() => {
    let active = true;
    setText(undefined);
    setSpreadsheetRows(undefined);
    setUrl(undefined);
    setPreviewError(undefined);
    setPreviewPage(0);
    if (!state.data) return;
    const objectUrl = typeof URL.createObjectURL === "function" ? URL.createObjectURL(state.data) : undefined;
    setUrl(objectUrl);
    if (["xlsx", "xls"].includes(extension))
      void readFileBytes(state.data)
        .then(bytes => readXlsxFile(bytes))
        .then(
          rows => {
            if (active) setSpreadsheetRows(rows.map(row => row.map(cell => (cell == null ? "" : String(cell)))));
          },
          () => {
            if (active) setPreviewError("The workbook could not be read. Retry with a valid Excel workbook.");
          }
        );
    if (
      [
        "csv",
        "tsv",
        "json",
        "jsonl",
        "txt",
        "md",
        "html",
        "htm",
        "py",
        "scala",
        "java",
        "js",
        "ts",
        "sql",
        "r",
        "xml",
        "yaml",
        "yml",
        "log",
      ].includes(extension)
    )
      void blobText(state.data).then(
        value => {
          if (active) setText(value);
        },
        error => {
          if (active) setPreviewError(error instanceof Error ? error.message : "Preview could not be read.");
        }
      );
    return () => {
      active = false;
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [state.data, extension]);
  const csvRows = useMemo(
    () =>
      text !== undefined && ["csv", "tsv"].includes(extension)
        ? Papa.parse<string[]>(text, {
            delimiter: extension === "tsv" ? "\t" : ",",
            skipEmptyLines: true,
          }).data
        : undefined,
    [text, extension]
  );
  const rows = spreadsheetRows ?? csvRows;
  const totalPages = Math.max(1, Math.ceil(((rows?.length ?? 1) - 1) / 50));
  const displayedRows = rows
    ? [rows[0], ...rows.slice(1 + previewPage * 50, 1 + (previewPage + 1) * 50)].filter(Boolean)
    : undefined;
  return (
    <section className="resource-file-preview">
      <h3>{node.name}</h3>
      <LoadState
        loading={state.loading}
        error={state.error ?? previewError}
        retry={state.refresh}
      />
      <Notices error={operation.error} />
      {tooLarge && <p>This file is too large to preview.</p>}
      {limit === undefined && <p>No inline preview for this file type.</p>}
      {canDownload && blocked && (
        <button
          type="button"
          disabled={operation.busy}
          onClick={() =>
            void operation.run(
              () => storage.file(kind, `${node.parentDir}/${node.name}`, publicView),
              blob => onSaveBlob(blob, node.name),
              "File downloaded."
            )
          }>
          Download file
        </button>
      )}
      {canDownload && state.data && (
        <button
          type="button"
          onClick={() => onSaveBlob(state.data!, node.name)}>
          Download file
        </button>
      )}
      {rows ? (
        <>
          <table>
            <tbody>
              {displayedRows!.map((row, index) => (
                <tr key={index}>
                  {row.map((cell, column) =>
                    index === 0 ? <th key={column}>{cell}</th> : <td key={column}>{cell}</td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
          <nav aria-label="File preview pages">
            <button
              type="button"
              aria-label="Previous preview page"
              disabled={previewPage === 0}
              onClick={() => setPreviewPage(page => Math.max(0, page - 1))}>
              Previous
            </button>
            <span>
              Page {previewPage + 1} of {totalPages} · {Math.max(0, rows.length - 1)} rows
            </span>
            <button
              type="button"
              aria-label="Next preview page"
              disabled={previewPage + 1 >= totalPages}
              onClick={() => setPreviewPage(page => Math.min(totalPages - 1, page + 1))}>
              Next
            </button>
          </nav>
        </>
      ) : text !== undefined ? (
        extension === "md" ? (
          <RichText content={text} />
        ) : ["html", "htm"].includes(extension) ? (
          <iframe
            title={`Preview ${node.name}`}
            sandbox=""
            srcDoc={text}
          />
        ) : (
          <pre>
            {extension === "json"
              ? (() => {
                  try {
                    return JSON.stringify(JSON.parse(text), null, 2);
                  } catch {
                    return text;
                  }
                })()
              : text}
          </pre>
        )
      ) : url && /^(png|jpg|jpeg|gif|webp|svg|bmp)$/.test(extension) ? (
        <img
          src={url}
          alt={node.name}
        />
      ) : url && /^(mp4|webm|ogg|mov)$/.test(extension) ? (
        <video
          controls
          src={url}
        />
      ) : url && /^(mp3|wav|flac|m4a)$/.test(extension) ? (
        <audio
          controls
          src={url}
        />
      ) : (
        state.data && <p>No inline preview for this file type.</p>
      )}
    </section>
  );
}
export function DatasetDetailPage(props: Omit<ResourceDetailProps, "kind">) {
  return (
    <ResourceDetailPage
      {...props}
      kind="dataset"
    />
  );
}
export function ModelDetailPage(props: Omit<ResourceDetailProps, "kind">) {
  return (
    <ResourceDetailPage
      {...props}
      kind="model"
    />
  );
}
