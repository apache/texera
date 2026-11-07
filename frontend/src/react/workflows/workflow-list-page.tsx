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

import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import type { ApiClient } from "../core/api-client";
import type { GuiConfig } from "../core/runtime-config";
import type { SessionUser } from "../core/auth";
import { RichText } from "../common/rich-text";
import { NotebookGenerationDialog } from "./notebook-generation";
import { ExecutionHistoryPage } from "../results/execution-history";
import {
  WorkflowRepository,
  emptyWorkflow,
  saveDownload,
  type WorkflowActivity,
  type WorkflowEntry,
  type WorkflowSearch,
  type WorkflowSharing,
  type WorkflowSort,
  type WorkflowVersionEntry,
  type WorkflowView,
} from "./workflow-repository";
import "./workflows.css";

export interface WorkflowListPageProps {
  api: ApiClient;
  user: SessionUser;
  config: Readonly<GuiConfig>;
  onOpen(wid: number, defaultView?: WorkflowView, autoLayout?: boolean): void;
  onOpenVersion?: (wid: number, versionId: number) => void;
}
type Dialog =
  | { kind: "generate" }
  | { kind: "edit"; entry: WorkflowEntry }
  | { kind: "share"; entry: WorkflowEntry }
  | { kind: "delete"; entries: WorkflowEntry[] }
  | { kind: "versions"; entry: WorkflowEntry }
  | { kind: "executions"; entry: WorkflowEntry }
  | undefined;
const VIEW_KEY = "texera.userWorkflow.viewMode";
const sortOptions: [WorkflowSort, string][] = [
  ["EditTimeDesc", "Recently edited"],
  ["CreateTimeDesc", "Recently created"],
  ["NameAsc", "Name A–Z"],
  ["NameDesc", "Name Z–A"],
  ["ExecutionTimeDesc", "Recently executed"],
];
function split(value: string): string[] {
  return value
    .split(/[,;\n]/)
    .map(part => part.trim())
    .filter(Boolean);
}
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "The workflow request failed.";
}

export function WorkflowListPage({ api, user, config, onOpen, onOpenVersion }: WorkflowListPageProps) {
  const repository = useMemo(() => new WorkflowRepository(api), [api]);
  const [entries, setEntries] = useState<WorkflowEntry[]>([]);
  const [activity, setActivity] = useState<Record<number, WorkflowActivity>>({});
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [search, setSearch] = useState("");
  const [owners, setOwners] = useState("");
  const [ids, setIds] = useState("");
  const [operators, setOperators] = useState("");
  const [createdFrom, setCreatedFrom] = useState("");
  const [createdTo, setCreatedTo] = useState("");
  const [editedFrom, setEditedFrom] = useState("");
  const [editedTo, setEditedTo] = useState("");
  const [orderBy, setOrderBy] = useState<WorkflowSort>("EditTimeDesc");
  const [includePublic, setIncludePublic] = useState(false);
  const [view, setView] = useState<"list" | "card">(() =>
    localStorage.getItem(VIEW_KEY) === "card" ? "card" : "list"
  );
  const [more, setMore] = useState(false);
  const nextStart = useRef(0);
  const [loading, setLoading] = useState(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [dialog, setDialog] = useState<Dialog>();
  const request = useRef(0);
  const abort = useRef<AbortController | undefined>(undefined);
  const activeQuery = useRef<WorkflowSearch>({});

  const load = useCallback(
    async (reset = true, query = activeQuery.current) => {
      abort.current?.abort();
      const controller = new AbortController();
      abort.current = controller;
      const sequence = ++request.current;
      if (reset) activeQuery.current = query;
      setLoading(true);
      setError(undefined);
      try {
        const page = await repository.search({ ...query, start: reset ? 0 : nextStart.current }, controller.signal);
        if (sequence !== request.current || controller.signal.aborted) return;
        setEntries(previous =>
          reset
            ? page.entries
            : [
                ...previous,
                ...page.entries.filter(row => !previous.some(existing => existing.workflow.wid === row.workflow.wid)),
              ]
        );
        setMore(page.more);
        nextStart.current = page.nextStart;
        if (reset) setSelected(new Set());
        if (reset) setActivity({});
        void repository
          .getActivity(page.entries, controller.signal)
          .then(next => {
            if (sequence === request.current && !controller.signal.aborted)
              setActivity(previous => (reset ? next : { ...previous, ...next }));
          })
          .catch(reason => {
            if (sequence === request.current && !controller.signal.aborted) setError(errorMessage(reason));
          });
      } catch (reason) {
        if (!controller.signal.aborted && sequence === request.current) setError(errorMessage(reason));
      } finally {
        if (sequence === request.current) setLoading(false);
      }
    },
    [repository]
  );
  useEffect(() => {
    void load(true, { orderBy: "EditTimeDesc" });
    return () => {
      abort.current?.abort();
    };
  }, [repository, user.uid, load]);

  async function perform(action: () => Promise<void>, refresh = true) {
    setPending(true);
    setError(undefined);
    setMessage(undefined);
    try {
      await action();
      if (refresh) await load();
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setPending(false);
    }
  }
  function submitSearch(event: FormEvent) {
    event.preventDefault();
    void load(true, {
      keywords: split(search),
      owners: split(owners),
      ids: split(ids),
      operators: split(operators),
      orderBy,
      includePublic,
      createDateStart: createdFrom || undefined,
      createDateEnd: createdTo || undefined,
      modifiedDateStart: editedFrom || undefined,
      modifiedDateEnd: editedTo || undefined,
    });
  }
  function chooseView(next: "list" | "card") {
    setView(next);
    localStorage.setItem(VIEW_KEY, next);
  }
  function open(entry: WorkflowEntry) {
    const wid = entry.workflow.wid;
    if (wid) onOpen(wid, config.formViewEnabled && entry.workflow.defaultView === "FORM" ? "FORM" : "CANVAS");
  }
  async function exportEntry(entry: WorkflowEntry) {
    saveDownload(await repository.exportJson(entry.workflow.wid!), `${entry.workflow.name}.json`);
  }
  const checked = entries.filter(entry => selected.has(entry.workflow.wid!));
  const allSelected = entries.length > 0 && checked.length === entries.length;

  return (
    <main className="workflow-page">
      <header className="workflow-page-heading">
        <div>
          <h1>Workflows</h1>
          <p>Create, organize and share your analysis workflows.</p>
        </div>
        <button
          className="primary"
          type="button"
          disabled={pending}
          onClick={() =>
            void perform(async () => {
              const entry = await repository.create(emptyWorkflow(config));
              if (!entry.workflow.wid) throw new Error("Workflow creation failed.");
              onOpen(entry.workflow.wid, "CANVAS");
            }, false)
          }>
          Create workflow
        </button>
      </header>
      <form
        className="workflow-search"
        onSubmit={submitSearch}>
        <label>
          Search workflows
          <input
            value={search}
            onChange={event => setSearch(event.target.value)}
            placeholder="Keywords, separated by commas"
          />
        </label>
        <label>
          Sort workflows
          <select
            value={orderBy}
            onChange={event => setOrderBy(event.target.value as WorkflowSort)}>
            {sortOptions.map(([value, label]) => (
              <option
                key={value}
                value={value}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <button
          type="submit"
          disabled={loading}>
          Search
        </button>
        <details className="workflow-filters">
          <summary>Filters</summary>
          <div>
            <label>
              Owners
              <input
                value={owners}
                onChange={event => setOwners(event.target.value)}
                placeholder="Names or emails, separated by commas"
              />
            </label>
            <label>
              Workflow IDs
              <input
                value={ids}
                onChange={event => setIds(event.target.value)}
              />
            </label>
            <label>
              Operator types
              <input
                value={operators}
                onChange={event => setOperators(event.target.value)}
              />
            </label>
            <label>
              Created from
              <input
                type="date"
                value={createdFrom}
                onChange={event => setCreatedFrom(event.target.value)}
              />
            </label>
            <label>
              Created until
              <input
                type="date"
                value={createdTo}
                onChange={event => setCreatedTo(event.target.value)}
              />
            </label>
            <label>
              Edited from
              <input
                type="date"
                value={editedFrom}
                onChange={event => setEditedFrom(event.target.value)}
              />
            </label>
            <label>
              Edited until
              <input
                type="date"
                value={editedTo}
                onChange={event => setEditedTo(event.target.value)}
              />
            </label>
            <label className="workflow-check">
              <input
                type="checkbox"
                checked={includePublic}
                onChange={event => setIncludePublic(event.target.checked)}
              />
              Include public workflows
            </label>
          </div>
        </details>
      </form>
      <div className="workflow-toolbar">
        {config.pythonNotebookMigrationEnabled && (
          <button
            type="button"
            disabled={pending}
            onClick={() => setDialog({ kind: "generate" })}>
            Generate from Python
          </button>
        )}
        <label className="workflow-import">
          Import JSON or ZIP
          <input
            type="file"
            accept=".json,.zip"
            multiple
            disabled={pending}
            onChange={event => {
              const files = Array.from(event.target.files ?? []);
              event.target.value = "";
              if (files.length)
                void perform(async () => {
                  const imported = await repository.importFiles(files);
                  setMessage(`Imported ${imported.length} workflow(s).`);
                });
            }}
          />
        </label>
        <button
          type="button"
          aria-pressed={view === "list"}
          onClick={() => chooseView("list")}>
          List view
        </button>
        <button
          type="button"
          aria-pressed={view === "card"}
          onClick={() => chooseView("card")}>
          Card view
        </button>
        <button
          type="button"
          onClick={() => setSelected(allSelected ? new Set() : new Set(entries.map(entry => entry.workflow.wid!)))}
          disabled={!entries.length}>
          {allSelected ? "Unselect all" : "Select all"}
        </button>
        {checked.length > 0 && (
          <>
            <span>{checked.length} selected</span>
            <button
              type="button"
              disabled={pending}
              onClick={() =>
                void perform(async () => {
                  await repository.duplicate(checked.map(entry => entry.workflow.wid!));
                })
              }>
              Duplicate selected
            </button>
            <button
              type="button"
              disabled={pending}
              onClick={() =>
                void perform(async () => {
                  const bytes = await repository.exportZip(
                    checked.map(entry => ({ wid: entry.workflow.wid!, name: entry.workflow.name }))
                  );
                  saveDownload(bytes as BlobPart, `workflowExports-${new Date().toISOString()}.zip`, "application/zip");
                }, false)
              }>
              Export selected
            </button>
            {checked.every(entry => entry.isOwner) && (
              <button
                type="button"
                disabled={pending}
                onClick={() => setDialog({ kind: "delete", entries: checked })}>
                Delete selected
              </button>
            )}
          </>
        )}
      </div>
      {error && (
        <p
          role="alert"
          className="error-message">
          {error}
        </p>
      )}
      {message && <p role="status">{message}</p>}
      {loading && <p role="status">Loading workflows…</p>}
      {!loading && !entries.length && !error && <p className="workflow-empty">No workflows match your search.</p>}
      <section
        className={`workflow-results workflow-results-${view}`}
        aria-label="Saved workflows"
        aria-busy={loading}>
        {entries.map(entry => {
          const workflow = entry.workflow;
          const details = activity[workflow.wid!];
          const canWrite = entry.accessLevel === "WRITE";
          return (
            <article
              className="workflow-entry"
              key={workflow.wid}>
              {view === "card" && (
                <img
                  className="workflow-cover"
                  src={entry.coverImage || "/assets/card_background.svg"}
                  alt=""
                />
              )}
              <label className="workflow-row-selection">
                <input
                  type="checkbox"
                  aria-label={`Select ${workflow.name}`}
                  checked={selected.has(workflow.wid!)}
                  onChange={event =>
                    setSelected(previous => {
                      const next = new Set(previous);
                      if (event.target.checked) next.add(workflow.wid!);
                      else next.delete(workflow.wid!);
                      return next;
                    })
                  }
                />
              </label>
              <div className="workflow-entry-description">
                <button
                  type="button"
                  className="workflow-name"
                  aria-label={`Open ${workflow.name}`}
                  onClick={() => open(entry)}>
                  {workflow.name}
                </button>
                <RichText
                  content={workflow.description || "No description"}
                  collapsedHeight={120}
                />
                <small>
                  {details?.ownerName || entry.ownerName || `Owner #${entry.ownerId}`} · {entry.accessLevel} ·{" "}
                  {workflow.isPublished ? "Public" : "Private"}
                  {config.formViewEnabled && workflow.defaultView === "FORM" ? " · Opens as form" : ""}
                </small>
                {details && (
                  <small className="workflow-activity">
                    {details.views} views · {details.clones} clones · {details.size.toLocaleString()} bytes ·{" "}
                    <span>{details.likes} likes</span>
                  </small>
                )}
              </div>
              <div className="workflow-row-actions">
                {details && (
                  <button
                    type="button"
                    aria-label={`${details.isLiked ? "Unlike" : "Like"} ${workflow.name}`}
                    disabled={pending}
                    onClick={() =>
                      void perform(async () => {
                        const next = await repository.toggleLike(workflow.wid!, details.isLiked);
                        setActivity(previous => ({
                          ...previous,
                          [workflow.wid!]: { ...details, isLiked: next.liked, likes: next.likeCount },
                        }));
                      }, false)
                    }>
                    {details.isLiked ? "Unlike" : "Like"}
                  </button>
                )}
                {canWrite && (
                  <button
                    type="button"
                    aria-label={`Edit ${workflow.name}`}
                    disabled={pending}
                    onClick={() => setDialog({ kind: "edit", entry })}>
                    Edit
                  </button>
                )}
                <button
                  type="button"
                  aria-label={`Duplicate ${workflow.name}`}
                  disabled={pending}
                  onClick={() =>
                    void perform(async () => {
                      await repository.duplicate([workflow.wid!]);
                    })
                  }>
                  Duplicate
                </button>
                <button
                  type="button"
                  aria-label={`Export ${workflow.name}`}
                  disabled={pending}
                  onClick={() => void perform(() => exportEntry(entry), false)}>
                  Export
                </button>
                <button
                  type="button"
                  aria-label={`Share ${workflow.name}`}
                  onClick={() => setDialog({ kind: "share", entry })}>
                  Share
                </button>
                <button
                  type="button"
                  aria-label={`Versions of ${workflow.name}`}
                  onClick={() => setDialog({ kind: "versions", entry })}>
                  History
                </button>
                {config.workflowExecutionsTrackingEnabled && (
                  <button
                    type="button"
                    aria-label={`Executions of ${workflow.name}`}
                    onClick={() => setDialog({ kind: "executions", entry })}>
                    Executions
                  </button>
                )}
                {entry.isOwner && (
                  <button
                    type="button"
                    aria-label={`Delete ${workflow.name}`}
                    disabled={pending}
                    onClick={() => setDialog({ kind: "delete", entries: [entry] })}>
                    Delete
                  </button>
                )}
              </div>
            </article>
          );
        })}
      </section>
      {more && (
        <button
          className="workflow-load-more"
          type="button"
          disabled={loading}
          onClick={() => void load(false)}>
          Load more
        </button>
      )}
      {dialog?.kind === "generate" && (
        <NotebookGenerationDialog
          api={api}
          config={config}
          onCreated={wid => {
            setDialog(undefined);
            onOpen(wid, "CANVAS", true);
          }}
          onClose={() => setDialog(undefined)}
          onWarning={setMessage}
        />
      )}
      {dialog?.kind === "edit" && (
        <WorkflowEditDialog
          entry={dialog.entry}
          repository={repository}
          config={config}
          close={() => setDialog(undefined)}
          saved={() => {
            setDialog(undefined);
            void load();
          }}
        />
      )}
      {dialog?.kind === "share" && (
        <WorkflowShareDialog
          entry={dialog.entry}
          repository={repository}
          user={user}
          close={() => {
            setDialog(undefined);
            void load();
          }}
        />
      )}
      {dialog?.kind === "executions" && (
        <div className="workflow-modal-backdrop">
          <section
            role="dialog"
            aria-modal="true"
            aria-label="Execution history"
            className="workflow-modal">
            <ExecutionHistoryPage
              api={api}
              wid={dialog.entry.workflow.wid!}
              readonly={!dialog.entry.isOwner && dialog.entry.accessLevel !== "WRITE"}
              onOpenWorkflow={(wid, vid) =>
                vid !== undefined && onOpenVersion ? onOpenVersion(wid, vid) : onOpen(wid)
              }
            />
            <button
              type="button"
              onClick={() => setDialog(undefined)}>
              Close executions
            </button>
          </section>
        </div>
      )}
      {dialog?.kind === "versions" && (
        <WorkflowVersionsDialog
          entry={dialog.entry}
          repository={repository}
          close={() => setDialog(undefined)}
          onOpen={onOpen}
          onOpenVersion={onOpenVersion}
        />
      )}
      {dialog?.kind === "delete" && (
        <div className="workflow-modal-backdrop">
          <section
            role="dialog"
            aria-modal="true"
            aria-label="Delete workflows"
            className="workflow-modal">
            <h2>
              Delete{" "}
              {dialog.entries.length === 1 ? dialog.entries[0].workflow.name : `${dialog.entries.length} workflows`}?
            </h2>
            <p>This permanently deletes the selected workflows.</p>
            <footer>
              <button
                type="button"
                disabled={pending}
                onClick={() => setDialog(undefined)}>
                Cancel
              </button>
              <button
                type="button"
                disabled={pending}
                onClick={() =>
                  void perform(async () => {
                    const wids = dialog.entries.map(entry => entry.workflow.wid!);
                    await repository.delete(wids);
                    if (config.pythonNotebookMigrationEnabled)
                      for (const wid of wids) void repository.cleanupNotebook(wid);
                    setDialog(undefined);
                  })
                }>
                Confirm delete
              </button>
            </footer>
          </section>
        </div>
      )}
    </main>
  );
}

function WorkflowEditDialog({
  entry,
  repository,
  config,
  close,
  saved,
}: {
  entry: WorkflowEntry;
  repository: WorkflowRepository;
  config: Readonly<GuiConfig>;
  close(): void;
  saved(): void;
}) {
  const [name, setName] = useState(entry.workflow.name);
  const [description, setDescription] = useState(entry.workflow.description ?? "");
  const [view, setView] = useState<WorkflowView>(entry.workflow.defaultView === "FORM" ? "FORM" : "CANVAS");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string>();
  const [cover, setCover] = useState<File>();
  const [removeCover, setRemoveCover] = useState(false);
  return (
    <div className="workflow-modal-backdrop">
      <form
        role="dialog"
        aria-modal="true"
        aria-label="Edit workflow"
        className="workflow-modal"
        onSubmit={event => {
          event.preventDefault();
          setPending(true);
          setError(undefined);
          void (async () => {
            try {
              if (name !== entry.workflow.name) await repository.rename(entry.workflow.wid!, name);
              if (description !== (entry.workflow.description ?? ""))
                await repository.updateDescription(entry.workflow.wid!, description);
              if (config.formViewEnabled && view !== (entry.workflow.defaultView ?? "CANVAS"))
                await repository.setDefaultView(entry.workflow.wid!, view);
              if (entry.isOwner && cover) {
                const image = await new Promise<string>((resolve, reject) => {
                  const reader = new FileReader();
                  reader.onload = () => resolve(String(reader.result));
                  reader.onerror = () => reject(new Error("The cover image could not be read."));
                  reader.readAsDataURL(cover);
                });
                await repository.setCover(entry.workflow.wid!, image);
              } else if (entry.isOwner && removeCover) await repository.deleteCover(entry.workflow.wid!);
              saved();
            } catch (reason) {
              setError(errorMessage(reason));
            } finally {
              setPending(false);
            }
          })();
        }}>
        <h2>Edit workflow</h2>
        {error && <p role="alert">{error}</p>}
        <label>
          Workflow name
          <input
            value={name}
            onChange={event => setName(event.target.value)}
            required
          />
        </label>
        <label>
          Description
          <textarea
            value={description}
            onChange={event => setDescription(event.target.value)}
            rows={6}
          />
        </label>
        {entry.isOwner && (
          <label>
            Cover image
            <input
              type="file"
              accept="image/*"
              onChange={event => {
                const file = event.target.files?.[0];
                if (file && !file.type.startsWith("image/")) {
                  setError("Choose an image file for the workflow cover.");
                  return;
                }
                setCover(file);
                setRemoveCover(false);
                setError(undefined);
              }}
            />
          </label>
        )}
        {entry.isOwner && entry.coverImage && (
          <button
            type="button"
            disabled={pending}
            onClick={() => {
              setCover(undefined);
              setRemoveCover(true);
            }}>
            Remove cover
          </button>
        )}
        {config.formViewEnabled && (
          <label>
            Default view
            <select
              value={view}
              onChange={event => setView(event.target.value as WorkflowView)}>
              <option value="CANVAS">Canvas</option>
              <option value="FORM">Form</option>
            </select>
          </label>
        )}
        <footer>
          <button
            type="button"
            disabled={pending}
            onClick={close}>
            Cancel
          </button>
          <button
            type="submit"
            disabled={pending}>
            Save changes
          </button>
        </footer>
      </form>
    </div>
  );
}

export function WorkflowShareDialog({
  entry,
  repository,
  user,
  close,
}: {
  entry: WorkflowEntry;
  repository: WorkflowRepository;
  user: SessionUser;
  close(): void;
}) {
  const [sharing, setSharing] = useState<WorkflowSharing>();
  const [emails, setEmails] = useState("");
  const [privilege, setPrivilege] = useState<"READ" | "WRITE">("WRITE");
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [pending, setPending] = useState(false);
  const [revoke, setRevoke] = useState<string>();
  const [downgrade, setDowngrade] = useState<string>();
  const wid = entry.workflow.wid!;
  useEffect(() => {
    const controller = new AbortController();
    void repository
      .getAccess(wid, controller.signal)
      .then(setSharing)
      .catch(reason => {
        if (!controller.signal.aborted) setError(errorMessage(reason));
      });
    return () => controller.abort();
  }, [repository, wid]);
  const canWrite = Boolean(
    sharing &&
      (sharing.owner === user.email ||
        sharing.entries.some(access => access.email === user.email && access.privilege === "WRITE"))
  );
  async function change(action: () => Promise<void>) {
    setPending(true);
    setError(undefined);
    setMessage(undefined);
    try {
      await action();
      setSharing(await repository.getAccess(wid));
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="workflow-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label={`Share ${entry.workflow.name}`}
        className="workflow-modal">
        <h2>Share {entry.workflow.name}</h2>
        {error && <p role="alert">{error}</p>}
        {message && <p role="status">{message}</p>}
        {!sharing ? (
          <p role="status">Loading sharing settings…</p>
        ) : (
          <>
            <p>Owner: {sharing.owner}</p>
            <p>{sharing.isPublished ? "Anyone can discover this public workflow." : "This workflow is private."}</p>
            {canWrite && (
              <button
                type="button"
                disabled={pending}
                onClick={() => void change(() => repository.setPublished(wid, !sharing.isPublished))}>
                {sharing.isPublished ? "Make private" : "Publish workflow"}
              </button>
            )}
            <ul className="workflow-access-list">
              {sharing.entries.map(access => (
                <li key={access.email}>
                  <span>
                    {access.name || access.email}
                    <small>{access.email}</small>
                  </span>
                  {canWrite ? (
                    <>
                      <select
                        aria-label={`Access for ${access.email}`}
                        value={access.privilege}
                        disabled={pending}
                        onChange={event => {
                          const next = event.target.value as "READ" | "WRITE";
                          if (access.email === user.email && next === "READ") {
                            setDowngrade(access.email);
                            return;
                          }
                          void change(() => repository.grantAccess(wid, access.email, next));
                        }}>
                        <option value="READ">Read</option>
                        <option value="WRITE">Write</option>
                      </select>
                      <button
                        type="button"
                        disabled={pending}
                        onClick={() => setRevoke(access.email)}>
                        Revoke {access.email}
                      </button>
                    </>
                  ) : (
                    <span>{access.privilege}</span>
                  )}
                </li>
              ))}
            </ul>
            {canWrite && (
              <form
                onSubmit={event => {
                  event.preventDefault();
                  void change(async () => {
                    const recipients = [...new Set(split(emails))];
                    if (!recipients.length) throw new Error("Enter an email address.");
                    for (const email of recipients) {
                      await repository.grantAccess(wid, email, privilege);
                      try {
                        await repository.notifyShare(wid, user.email ?? user.name, email);
                      } catch {
                        setMessage("Access was granted, but the email notification could not be sent.");
                      }
                    }
                    setEmails("");
                  });
                }}>
                <label>
                  Email addresses
                  <input
                    value={emails}
                    onChange={event => setEmails(event.target.value)}
                    placeholder="Separate multiple addresses with commas"
                  />
                </label>
                <label>
                  Access level
                  <select
                    value={privilege}
                    onChange={event => setPrivilege(event.target.value as "READ" | "WRITE")}>
                    <option value="WRITE">Write</option>
                    <option value="READ">Read</option>
                  </select>
                </label>
                <button
                  type="submit"
                  disabled={pending}>
                  Grant access
                </button>
              </form>
            )}
            {(revoke || downgrade) && (
              <div className="workflow-confirm">
                <p>
                  {revoke
                    ? `Revoke ${revoke === user.email ? "your" : revoke + "'s"} access?`
                    : "Downgrade your access to read only?"}
                </p>
                <button
                  type="button"
                  disabled={pending}
                  onClick={() => {
                    setRevoke(undefined);
                    setDowngrade(undefined);
                  }}>
                  Cancel change
                </button>
                <button
                  type="button"
                  disabled={pending}
                  onClick={() =>
                    void change(async () => {
                      if (revoke) {
                        await repository.revokeAccess(wid, revoke);
                        if (revoke === user.email) close();
                      } else if (downgrade) await repository.grantAccess(wid, downgrade, "READ");
                      setRevoke(undefined);
                      setDowngrade(undefined);
                    })
                  }>
                  Confirm access change
                </button>
              </div>
            )}
          </>
        )}
        <footer>
          <button
            type="button"
            disabled={pending}
            onClick={close}>
            Close
          </button>
        </footer>
      </section>
    </div>
  );
}

function WorkflowVersionsDialog({
  entry,
  repository,
  close,
  onOpen,
  onOpenVersion,
}: {
  entry: WorkflowEntry;
  repository: WorkflowRepository;
  close(): void;
  onOpen: WorkflowListPageProps["onOpen"];
  onOpenVersion?: WorkflowListPageProps["onOpenVersion"];
}) {
  const [versions, setVersions] = useState<WorkflowVersionEntry[]>();
  const [error, setError] = useState<string>();
  const [pending, setPending] = useState(false);
  const wid = entry.workflow.wid!;
  useEffect(() => {
    const controller = new AbortController();
    void repository
      .getVersions(wid, controller.signal)
      .then(setVersions)
      .catch(reason => {
        if (!controller.signal.aborted) setError(errorMessage(reason));
      });
    return () => controller.abort();
  }, [repository, wid]);
  return (
    <div className="workflow-modal-backdrop">
      <section
        role="dialog"
        aria-modal="true"
        aria-label="Workflow history"
        className="workflow-modal">
        <h2>History of {entry.workflow.name}</h2>
        {error && <p role="alert">{error}</p>}
        {!versions ? (
          <p role="status">Loading history…</p>
        ) : versions.length ? (
          <ul>
            {versions.map((version, index) => (
              <li key={version.vId}>
                <span>
                  Version {versions.length - index} · {new Date(version.creationTime).toLocaleString()}
                </span>
                {onOpenVersion && (
                  <button
                    type="button"
                    disabled={pending}
                    onClick={() => onOpenVersion(wid, version.vId)}>
                    View version {version.vId}
                  </button>
                )}
                <button
                  type="button"
                  disabled={pending}
                  onClick={() => {
                    setPending(true);
                    setError(undefined);
                    void repository
                      .cloneVersion(version.vId, versions.length - index)
                      .then(clone => onOpen(clone, entry.workflow.defaultView === "FORM" ? "FORM" : "CANVAS"))
                      .catch(reason => setError(errorMessage(reason)))
                      .finally(() => setPending(false));
                  }}>
                  Clone version {version.vId}
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p>No saved versions.</p>
        )}
        <footer>
          <button
            type="button"
            disabled={pending}
            onClick={close}>
            Close
          </button>
        </footer>
      </section>
    </div>
  );
}
