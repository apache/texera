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

import { useEffect, useMemo, useState, useSyncExternalStore } from "react";
import { DeploymentVersionMonitor, type DeploymentVersionOptions } from "./deployment-version";

export interface DeploymentUpdateNoticeProps extends DeploymentVersionOptions {
  enabled: boolean;
  intervalMs?: number;
}

/** Sticky and dismissible; clicking the update performs at most one reload. */
export function DeploymentUpdateNotice({
  enabled,
  intervalMs,
  fetch,
  buildNumber,
  reload,
}: DeploymentUpdateNoticeProps) {
  const monitor = useMemo(
    () => new DeploymentVersionMonitor({ fetch, buildNumber, reload }),
    [fetch, buildNumber, reload]
  );
  const available = useSyncExternalStore(monitor.subscribe, monitor.getSnapshot, monitor.getSnapshot);
  const [dismissed, setDismissed] = useState(false);
  useEffect(() => {
    if (!enabled) return;
    const subscription = monitor.startPollingForUpdates(intervalMs);
    return () => subscription.unsubscribe();
  }, [monitor, enabled, intervalMs]);
  if (!enabled || !available || dismissed) return null;
  return (
    <aside
      role="status"
      className="deployment-update-notice"
      style={{
        position: "fixed",
        bottom: "1rem",
        right: "1rem",
        zIndex: 10000,
        maxWidth: "24rem",
        padding: "1rem",
        border: "1px solid #a9bfdc",
        borderRadius: ".5rem",
        background: "#fff",
        color: "#17385f",
        boxShadow: "0 4px 18px #0003",
      }}>
      <button
        type="button"
        onClick={monitor.reload}>
        New version available
      </button>
      <p>A new version of Texera is available. Click here to refresh, or reload the page when convenient.</p>
      <button
        type="button"
        aria-label="Dismiss version notification"
        onClick={() => setDismissed(true)}>
        Dismiss
      </button>
    </aside>
  );
}
