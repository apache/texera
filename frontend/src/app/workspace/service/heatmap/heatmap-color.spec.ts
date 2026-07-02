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

import { HEATMAP_NO_DATA_COLOR, scoreToColor } from "./heatmap-color";

describe("scoreToColor", () => {
  it("maps 0 to the cold stop (blue)", () => {
    expect(scoreToColor(0)).toBe("#2c7bb6");
  });

  it("maps 0.5 to the mid stop (pale yellow)", () => {
    expect(scoreToColor(0.5)).toBe("#ffffbf");
  });

  it("maps 1 to the hot stop (red)", () => {
    expect(scoreToColor(1)).toBe("#d7191c");
  });

  it("clamps values below 0 to the cold stop", () => {
    expect(scoreToColor(-0.5)).toBe(scoreToColor(0));
  });

  it("clamps values above 1 to the hot stop", () => {
    expect(scoreToColor(2)).toBe(scoreToColor(1));
  });

  it("returns a valid hex color for interpolated values", () => {
    for (const score of [0.1, 0.25, 0.5, 0.75, 0.9]) {
      expect(scoreToColor(score)).toMatch(/^#[0-9a-f]{6}$/);
    }
  });

  it("interpolates between stops rather than snapping to an endpoint", () => {
    const mid = scoreToColor(0.25);
    expect(mid).not.toBe(scoreToColor(0));
    expect(mid).not.toBe(scoreToColor(0.5));
  });

  it("exposes a distinct neutral no-data color", () => {
    expect(HEATMAP_NO_DATA_COLOR).toMatch(/^#[0-9a-f]{6}$/);
    expect(HEATMAP_NO_DATA_COLOR).not.toBe(scoreToColor(0));
  });
});
