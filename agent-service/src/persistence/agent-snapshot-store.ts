/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { mkdir, readdir, readFile, rename, rm, writeFile } from "fs/promises";
import { join } from "path";
import type { AgentSnapshot } from "../types/agent";
import { createLogger } from "../logger";

const log = createLogger("SnapshotStore");

const DEFAULT_DEBOUNCE_MS = 500;
const FILE_SUFFIX = ".agent.json";

/**
 * Disk-backed store of agent snapshots, one JSON file per agent.
 *
 * Writes are atomic (write to a temp file then rename) and can be coalesced via
 * `scheduleSave` so a burst of updates results in a single write. `loadAll`
 * rehydrates surviving agents on startup and skips any unreadable/invalid file.
 */
export class AgentSnapshotStore {
  private readonly dir: string;
  private readonly debounceMs: number;
  private readonly pending = new Map<string, { snapshot: AgentSnapshot; timer: ReturnType<typeof setTimeout> }>();

  constructor(dir: string, options: { debounceMs?: number } = {}) {
    this.dir = dir;
    this.debounceMs = options.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  }

  // agentIds are server-generated, but sanitize anyway to keep writes inside dir.
  private fileFor(agentId: string): string {
    const safe = agentId.replace(/[^a-zA-Z0-9_-]/g, "_");
    return join(this.dir, `${safe}${FILE_SUFFIX}`);
  }

  async save(snapshot: AgentSnapshot): Promise<void> {
    await mkdir(this.dir, { recursive: true });
    const file = this.fileFor(snapshot.agentId);
    const tmp = `${file}.${process.pid}.tmp`;
    await writeFile(tmp, JSON.stringify(snapshot), "utf-8");
    await rename(tmp, file);
  }

  /** Debounced save; coalesces rapid updates for the same agent into one write. */
  scheduleSave(snapshot: AgentSnapshot): void {
    const existing = this.pending.get(snapshot.agentId);
    if (existing) clearTimeout(existing.timer);

    const timer = setTimeout(() => {
      const entry = this.pending.get(snapshot.agentId);
      if (!entry) return;
      this.pending.delete(snapshot.agentId);
      void this.save(entry.snapshot).catch(err =>
        log.error({ err, agentId: snapshot.agentId }, "failed to persist agent snapshot")
      );
    }, this.debounceMs);

    // A pending snapshot write should not keep the process alive on its own.
    if (typeof timer.unref === "function") timer.unref();
    this.pending.set(snapshot.agentId, { snapshot, timer });
  }

  async load(agentId: string): Promise<AgentSnapshot | null> {
    try {
      const raw = await readFile(this.fileFor(agentId), "utf-8");
      return JSON.parse(raw) as AgentSnapshot;
    } catch {
      return null;
    }
  }

  async loadAll(): Promise<AgentSnapshot[]> {
    let files: string[];
    try {
      files = await readdir(this.dir);
    } catch {
      return []; // directory does not exist yet -> nothing persisted
    }

    const snapshots: AgentSnapshot[] = [];
    for (const file of files) {
      if (!file.endsWith(FILE_SUFFIX)) continue;
      try {
        const raw = await readFile(join(this.dir, file), "utf-8");
        const snapshot = JSON.parse(raw) as AgentSnapshot;
        if (snapshot?.agentId && snapshot.version === 1) {
          snapshots.push(snapshot);
        } else {
          log.warn({ file }, "skipping snapshot with missing id or unsupported version");
        }
      } catch (err) {
        log.warn({ file, err }, "skipping unreadable agent snapshot");
      }
    }
    return snapshots;
  }

  async remove(agentId: string): Promise<void> {
    const existing = this.pending.get(agentId);
    if (existing) {
      clearTimeout(existing.timer);
      this.pending.delete(agentId);
    }
    await rm(this.fileFor(agentId), { force: true });
  }

  /** Persist all pending debounced writes immediately (shutdown / tests). */
  async flush(): Promise<void> {
    const entries = Array.from(this.pending.values());
    this.pending.clear();
    await Promise.all(
      entries.map(({ snapshot, timer }) => {
        clearTimeout(timer);
        return this.save(snapshot).catch(err =>
          log.error({ err, agentId: snapshot.agentId }, "failed to flush agent snapshot")
        );
      })
    );
  }
}
