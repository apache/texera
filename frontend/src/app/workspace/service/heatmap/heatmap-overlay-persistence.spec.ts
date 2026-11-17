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

import {
  HEATMAP_OVERLAY_STORAGE_KEY,
  loadPersistedHeatmapView,
  savePersistedHeatmapView,
} from "./heatmap-overlay-persistence";
import { HeatmapView } from "./heatmap-scoring";

describe("heatmap overlay persistence", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => localStorage.clear());

  it("round-trips an active view", () => {
    savePersistedHeatmapView(HeatmapView.TimePerRow);
    expect(loadPersistedHeatmapView()).toBe(HeatmapView.TimePerRow);
  });

  it("round-trips the overlay-off state (null view)", () => {
    savePersistedHeatmapView(HeatmapView.Runtime);
    savePersistedHeatmapView(null);
    expect(loadPersistedHeatmapView()).toBeNull();
  });

  it("reads as off when nothing was ever persisted", () => {
    expect(loadPersistedHeatmapView()).toBeNull();
  });

  it("reads a corrupted or foreign stored value as off", () => {
    localStorage.setItem(HEATMAP_OVERLAY_STORAGE_KEY, "not-json{");
    expect(loadPersistedHeatmapView()).toBeNull();

    // Valid JSON, but not a HeatmapView member (e.g. persisted by a future or
    // older build): must not leak an unknown value into the view stream.
    localStorage.setItem(HEATMAP_OVERLAY_STORAGE_KEY, JSON.stringify({ view: "no-such-view" }));
    expect(loadPersistedHeatmapView()).toBeNull();

    localStorage.setItem(HEATMAP_OVERLAY_STORAGE_KEY, JSON.stringify({ wrongShape: true }));
    expect(loadPersistedHeatmapView()).toBeNull();
  });
});
