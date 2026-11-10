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

import { Version } from "../../environments/version";

export const VERSION_MANIFEST_URL = "assets/version.json";
export const VERSION_POLL_INTERVAL_MS = 5 * 60 * 1000;
export interface DeploymentVersionOptions {
  fetch?: typeof globalThis.fetch;
  buildNumber?: string;
  reload?: () => void;
}

/** Same cache-busted public manifest and delayed polling contract as DeploymentVersionService. */
export class DeploymentVersionMonitor {
  private available = false;
  private reloaded = false;
  private readonly listeners = new Set<() => void>();
  private readonly fetcher: typeof globalThis.fetch;
  private readonly runningBuild: string;
  private readonly reloadPage: () => void;
  constructor(options: DeploymentVersionOptions = {}) {
    this.fetcher = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.runningBuild = options.buildNumber ?? Version.buildNumber;
    this.reloadPage = options.reload ?? (() => window.location.reload());
  }
  getSnapshot = (): boolean => this.available;
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  async checkForUpdate(signal?: AbortSignal): Promise<boolean> {
    if (signal?.aborted) return false;
    try {
      const response = await this.fetcher(`${VERSION_MANIFEST_URL}?t=${Date.now()}`, { method: "GET", signal });
      if (!response.ok || signal?.aborted) return false;
      const manifest: unknown = await response.json();
      const deployed =
        manifest && typeof manifest === "object" ? (manifest as { buildNumber?: unknown }).buildNumber : undefined;
      return !signal?.aborted && typeof deployed === "string" && deployed.length > 0 && deployed !== this.runningBuild;
    } catch {
      return false;
    }
  }
  startPollingForUpdates(intervalMs = VERSION_POLL_INTERVAL_MS): { unsubscribe(): void } {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0)
      throw new Error("The version polling interval must be positive.");
    let stopped = false;
    let controller: AbortController | undefined;
    const unsubscribe = () => {
      stopped = true;
      clearInterval(timer);
      controller?.abort();
    };
    const timer = setInterval(() => {
      controller?.abort();
      const request = new AbortController();
      controller = request;
      void this.checkForUpdate(request.signal).then(updated => {
        if (stopped || request.signal.aborted || !updated) return;
        unsubscribe();
        if (!this.available) {
          this.available = true;
          this.listeners.forEach(listener => listener());
        }
      });
    }, intervalMs);
    return { unsubscribe };
  }
  reload = (): void => {
    if (this.reloaded) return;
    this.reloaded = true;
    this.reloadPage();
  };
}
