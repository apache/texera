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

import { localGetObject, localSetObject } from "../../../../common/util/storage";

/**
 * Per-panel persistence of the observability filter forms in localStorage,
 * so an operator's chosen filters survive a reload / re-open.
 *
 * The time range is intentionally NOT persisted (callers omit it): an
 * observability window should default to "now" each session rather than
 * restoring a stale absolute window. Everything else (process/comm, service
 * scope, level, step, page size, auto-refresh, user) is remembered.
 */
const PREFS_PREFIX = "texera-observability-prefs-";

/** Save a panel's form snapshot, dropping any `omit` keys (e.g. the time range). */
export function savePanelPrefs<T extends object>(panel: string, value: T, omit: ReadonlyArray<string> = []): void {
  const snapshot: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (!omit.includes(k)) snapshot[k] = v;
  }
  localSetObject(PREFS_PREFIX + panel, snapshot);
}

/** Load a panel's saved form snapshot, or undefined when nothing was saved. */
export function loadPanelPrefs<T>(panel: string): Partial<T> | undefined {
  return localGetObject<Partial<T>>(PREFS_PREFIX + panel);
}
