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
import { useEffect, useRef, useState } from "react";
import type { Subscription } from "rxjs";
import type { MultipartUploadProgress } from "./multipart";
import { Dialog, Notices } from "./pages";
import { formatQuotaSize } from "./quota";
import { StorageApi, type ResourceKind } from "./storage-api";

interface DroppedEntry {
  name: string;
  isFile: boolean;
  isDirectory: boolean;
  file?: (success: (file: File) => void, error: (error: unknown) => void) => void;
  createReader?: () => {
    readEntries: (success: (entries: DroppedEntry[]) => void, error: (error: unknown) => void) => void;
  };
}
export async function filesFromDrop(transfer: DataTransfer): Promise<{ file: File; path: string }[]> {
  async function read(entry: DroppedEntry, prefix = ""): Promise<{ file: File; path: string }[]> {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isFile && entry.file) {
      const file = await new Promise<File>((resolve, reject) => entry.file!(resolve, reject));
      return [{ file, path }];
    }
    if (!entry.isDirectory || !entry.createReader) return [];
    const reader = entry.createReader();
    const children: DroppedEntry[] = [];
    let batch: DroppedEntry[];
    do {
      batch = await new Promise<DroppedEntry[]>((resolve, reject) => reader.readEntries(resolve, reject));
      children.push(...batch);
    } while (batch.length);
    return (await Promise.all(children.map(child => read(child, path)))).flat();
  }
  const items = [...(transfer.items ?? [])].filter(item => item.kind === "file");
  if (!items.length) return [...transfer.files].map(file => ({ file, path: file.webkitRelativePath || file.name }));
  return (
    await Promise.all(
      items.map(async item => {
        const entry = item.webkitGetAsEntry?.() as DroppedEntry | null;
        if (entry) return read(entry);
        const file = item.getAsFile();
        return file ? [{ file, path: file.webkitRelativePath || file.name }] : [];
      })
    )
  ).flat();
}

interface UploadJob {
  path: string;
  file: File;
  restart: boolean;
  status: MultipartUploadProgress["status"] | "queued" | "paused";
  percentage: number;
  speed?: number;
  remaining?: number;
  error?: string;
}
export interface ResourceUploadProps {
  storage: StorageApi;
  kind: ResourceKind;
  id: number;
  name: string;
  ownerEmail: string;
  onChanged: () => void;
  onBusyChange?: (busy: boolean) => void;
}
export function ResourceUploadPanel({
  storage,
  kind,
  id,
  name,
  ownerEmail,
  onChanged,
  onBusyChange,
}: ResourceUploadProps) {
  const [limits, setLimits] = useState<Awaited<ReturnType<StorageApi["uploadSettings"]>>>();
  const [error, setError] = useState<string>();
  const [jobs, setJobs] = useState<UploadJob[]>([]);
  const [checking, setChecking] = useState(false);
  const [directory, setDirectory] = useState("");
  const [conflicts, setConflicts] = useState<{ file: File; path: string; choice: "skip" | "resume" | "restart" }[]>();
  const subscriptions = useRef(new Map<string, Subscription>());
  const active = useRef(true);
  const queuedStarts = useRef(new Set<string>());
  useEffect(() => {
    active.current = true;
    const liveSubscriptions = subscriptions.current;
    const starts = queuedStarts.current;
    let valid = true;
    void storage.uploadSettings(kind).then(
      value => {
        if (valid) setLimits(value);
      },
      reason => {
        if (valid) setError(reason instanceof Error ? reason.message : "Upload settings could not be loaded.");
      }
    );
    return () => {
      valid = false;
      active.current = false;
      liveSubscriptions.forEach(item => item.unsubscribe());
      liveSubscriptions.clear();
      starts.clear();
    };
  }, [storage, kind]);
  const busy = checking || jobs.some(item => ["queued", "initializing", "uploading"].includes(item.status));
  useEffect(() => {
    onBusyChange?.(busy);
    return () => onBusyChange?.(false);
  }, [busy, onBusyChange]);
  const update = (path: string, values: Partial<UploadJob>) => {
    if (active.current) setJobs(current => current.map(item => (item.path === path ? { ...item, ...values } : item)));
  };
  useEffect(() => {
    if (!limits) return;
    for (const job of jobs) {
      if (subscriptions.current.size + queuedStarts.current.size >= limits.concurrentFiles) break;
      if (job.status !== "queued" || subscriptions.current.has(job.path) || queuedStarts.current.has(job.path))
        continue;
      queuedStarts.current.add(job.path);
      update(job.path, { status: "initializing", error: undefined });
      const subscription = storage
        .upload(kind, ownerEmail, name, job.path, job.file, limits.partSize, limits.concurrency, job.restart)
        .subscribe({
          next: value =>
            update(job.path, {
              status: value.status,
              percentage: value.percentage,
              speed: value.uploadSpeed,
              remaining: value.estimatedTimeRemaining,
            }),
          error: reason => {
            subscriptions.current.delete(job.path);
            queuedStarts.current.delete(job.path);
            update(job.path, { status: "failed", error: reason instanceof Error ? reason.message : "Upload failed." });
          },
          complete: () => {
            subscriptions.current.delete(job.path);
            queuedStarts.current.delete(job.path);
            update(job.path, { status: "finished", percentage: 100 });
            if (active.current) onChanged();
          },
        });
      queuedStarts.current.delete(job.path);
      if (!subscription.closed) subscriptions.current.set(job.path, subscription);
    }
  }, [jobs, limits, storage, kind, ownerEmail, name, onChanged]);
  async function addFiles(files: File[], paths?: Map<File, string>) {
    if (!limits || checking || !files.length) return;
    setError(undefined);
    setChecking(true);
    try {
      const pending = files.map(file => ({
        file,
        path: [directory.trim().replace(/^\/+|\/+$/g, ""), paths?.get(file) ?? (file.webkitRelativePath || file.name)]
          .filter(Boolean)
          .join("/"),
      }));
      if (
        pending.some(item =>
          item.path
            .split("/")
            .some(segment => !segment || segment === "." || segment === ".." || segment.includes("\\"))
        )
      )
        throw new Error("Upload paths must stay inside the resource.");
      const oversized = pending.find(item => item.file.size > limits.maxFileBytes);
      if (oversized)
        throw new Error(`${oversized.path} exceeds the maximum file size of ${formatQuotaSize(limits.maxFileBytes)}.`);
      const names = new Set<string>();
      if (pending.some(item => names.has(item.path) || !names.add(item.path)))
        throw new Error("The selection contains duplicate file paths.");
      const [existing, unfinished] = await Promise.all([
        storage.existingFiles(
          kind,
          id,
          pending.map(item => ({ path: item.path, sizeBytes: item.file.size }))
        ),
        storage.pendingUploads(kind, ownerEmail, name),
      ]);
      const colliding = new Set([
        ...existing,
        ...unfinished,
        ...jobs.filter(item => item.status !== "finished" && item.status !== "aborted").map(item => item.path),
      ]);
      const fresh = pending.filter(item => !colliding.has(item.path));
      if (active.current) {
        setJobs(current => [
          ...current.filter(item => !fresh.some(file => file.path === item.path)),
          ...fresh.map(item => ({ ...item, restart: false, status: "queued" as const, percentage: 0 })),
        ]);
        const conflicts = pending
          .filter(item => colliding.has(item.path))
          .map(item => ({ ...item, choice: "skip" as const }));
        if (conflicts.length) setConflicts(conflicts);
      }
    } catch (reason) {
      if (active.current) setError(reason instanceof Error ? reason.message : "Could not prepare upload.");
    } finally {
      if (active.current) setChecking(false);
    }
  }
  return (
    <section className="resource-panel resource-uploads">
      <h2>Upload files</h2>
      <Notices error={error} />
      {!limits && !error && <p>Loading upload limits…</p>}
      <label>
        Upload directory
        <input
          value={directory}
          onChange={event => setDirectory(event.target.value)}
          disabled={checking}
          placeholder="Optional relative directory"
        />
      </label>
      <div
        className="resource-upload-inputs"
        onDragOver={event => event.preventDefault()}
        onDrop={event => {
          event.preventDefault();
          void filesFromDrop(event.dataTransfer).then(
            entries =>
              addFiles(
                entries.map(item => item.file),
                new Map(entries.map(item => [item.file, item.path]))
              ),
            reason => {
              if (active.current) setError(reason instanceof Error ? reason.message : "Could not read dropped files.");
            }
          );
        }}>
        <label>
          Upload files
          <input
            type="file"
            multiple
            disabled={!limits || checking}
            onChange={event => {
              void addFiles([...(event.target.files ?? [])]);
              event.target.value = "";
            }}
          />
        </label>
        <label>
          Upload folder
          <input
            type="file"
            multiple
            {...{ webkitdirectory: "" }}
            disabled={!limits || checking}
            onChange={event => {
              void addFiles([...(event.target.files ?? [])]);
              event.target.value = "";
            }}
          />
        </label>
        <p>Choose files or a folder, or drop files here. Uploads can be paused and resumed.</p>
      </div>
      {checking && <p role="status">Checking existing uploads…</p>}
      {limits && (
        <p>
          Per-file limit: {formatQuotaSize(limits.maxFileBytes)} · Up to {limits.concurrentFiles} files and{" "}
          {limits.concurrency} chunks per file.
        </p>
      )}
      {jobs.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>File</th>
              <th>Progress</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map(job => (
              <tr key={job.path}>
                <td>{job.path}</td>
                <td>
                  <progress
                    max={100}
                    value={job.percentage}
                    aria-label={`Upload progress ${job.path}`}
                  />
                  {job.percentage}%
                  {job.speed ? (
                    <p>
                      {formatQuotaSize(job.speed)}/s · {job.remaining ?? 0}s remaining
                    </p>
                  ) : null}
                </td>
                <td>
                  {job.status}
                  {job.error && <p role="alert">{job.error}</p>}
                </td>
                <td>
                  {["initializing", "uploading", "queued"].includes(job.status) && (
                    <button
                      type="button"
                      onClick={() => {
                        subscriptions.current.get(job.path)?.unsubscribe();
                        subscriptions.current.delete(job.path);
                        update(job.path, { status: "paused" });
                      }}>
                      Pause {job.path}
                    </button>
                  )}
                  {["paused", "failed"].includes(job.status) && (
                    <button
                      type="button"
                      onClick={() => update(job.path, { status: "queued", restart: false, error: undefined })}>
                      Resume {job.path}
                    </button>
                  )}
                  {!["finished", "aborted"].includes(job.status) && (
                    <button
                      type="button"
                      onClick={() => {
                        subscriptions.current.get(job.path)?.unsubscribe();
                        subscriptions.current.delete(job.path);
                        update(job.path, { status: "paused" });
                        void storage.finalizeUpload(kind, ownerEmail, name, job.path, true).then(
                          () => update(job.path, { status: "aborted" }),
                          reason =>
                            update(job.path, {
                              status: "failed",
                              error: reason instanceof Error ? reason.message : "Could not abort upload.",
                            })
                        );
                      }}>
                      Abort {job.path}
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {conflicts && (
        <Dialog
          title="Conflicting files"
          close={() => setConflicts(undefined)}>
          <p>Choose whether to keep the existing file, resume a partial upload, or restart and replace it.</p>
          <label>
            Apply to all
            <select
              defaultValue=""
              onChange={event => {
                const choice = event.target.value as "skip" | "resume" | "restart";
                if (choice) setConflicts(current => current?.map(item => ({ ...item, choice })));
              }}>
              <option value="">Choose an action</option>
              <option value="skip">Skip</option>
              <option value="resume">Resume</option>
              <option value="restart">Restart / replace</option>
            </select>
          </label>
          {conflicts.map((item, index) => (
            <label key={item.path}>
              {item.path}
              <select
                value={item.choice}
                onChange={event =>
                  setConflicts(current =>
                    current?.map((file, position) =>
                      position === index ? { ...file, choice: event.target.value as typeof file.choice } : file
                    )
                  )
                }>
                <option value="skip">Skip</option>
                <option value="resume">Resume</option>
                <option value="restart">Restart / replace</option>
              </select>
            </label>
          ))}
          <footer>
            <button
              type="button"
              onClick={() => setConflicts(undefined)}>
              Cancel
            </button>
            <button
              type="button"
              onClick={() => {
                const selected = conflicts.filter(item => item.choice !== "skip");
                setJobs(current => [
                  ...current.filter(job => !selected.some(item => item.path === job.path)),
                  ...selected.map(item => ({
                    path: item.path,
                    file: item.file,
                    restart: item.choice === "restart",
                    status: "queued" as const,
                    percentage: 0,
                  })),
                ]);
                setConflicts(undefined);
              }}>
              Apply upload choices
            </button>
          </footer>
        </Dialog>
      )}
    </section>
  );
}
