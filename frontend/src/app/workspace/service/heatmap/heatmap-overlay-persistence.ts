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

import { localGetObject, localSetObject } from "../../../common/util/storage";
import { HeatmapView } from "./heatmap-scoring";

/** localStorage key for the persisted heat-map overlay state. */
export const HEATMAP_OVERLAY_STORAGE_KEY = "heatmap-overlay";

/** Persisted shape: the active view, or null when the overlay is off. */
export interface PersistedHeatmapOverlay {
  readonly view: HeatmapView | null;
}

/** Persists the overlay state (a null view means the overlay is off). */
export function savePersistedHeatmapView(view: HeatmapView | null): void {
  localSetObject<PersistedHeatmapOverlay>(HEATMAP_OVERLAY_STORAGE_KEY, { view });
}

/**
 * Reads the persisted overlay state back. Returns null (overlay off) when
 * nothing was persisted or the stored value is not a valid HeatmapView —
 * a corrupted or foreign value must not leak into the view stream.
 */
export function loadPersistedHeatmapView(): HeatmapView | null {
  try {
    // localGetObject JSON.parses the stored string, which throws on a
    // corrupted value; treat that the same as nothing persisted.
    const persisted = localGetObject<PersistedHeatmapOverlay>(HEATMAP_OVERLAY_STORAGE_KEY);
    const view = persisted?.view;
    return Object.values(HeatmapView).includes(view as HeatmapView) ? (view as HeatmapView) : null;
  } catch {
    return null;
  }
}
