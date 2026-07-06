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

type Rgb = readonly [number, number, number];

/**
 * Cold -> hot ramp (ColorBrewer RdYlBu, reversed): blue -> pale yellow -> red.
 * Chosen because it is colorblind-safer than a rainbow ramp.
 */
const COLD: Rgb = [91, 155, 213]; // #5b9bd5 — bright blue (still light enough for readable labels)
const MID: Rgb = [255, 255, 191]; // #ffffbf — pale yellow
const HOT: Rgb = [224, 90, 82]; // #e05a52 — bright red

/** Neutral fill for an operator that has no score (no metrics captured yet). */
export const HEATMAP_NO_DATA_COLOR = "#eeeeee";

function clamp01(value: number): number {
  // Guard NaN (which would otherwise produce "#NaNNaNNaN"); it maps to 0 (cold).
  // Infinity still clamps to 1 (hot) via the min/max below.
  const safe = Number.isNaN(value) ? 0 : value;
  return Math.min(1, Math.max(0, safe));
}

function lerpChannel(from: number, to: number, t: number): number {
  return Math.round(from + (to - from) * t);
}

function toHex(channel: number): string {
  return channel.toString(16).padStart(2, "0");
}

function mix(from: Rgb, to: Rgb, t: number): string {
  const r = lerpChannel(from[0], to[0], t);
  const g = lerpChannel(from[1], to[1], t);
  const b = lerpChannel(from[2], to[2], t);
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
}

/**
 * Map a normalized [0, 1] heat score to a hex color on the cold -> hot ramp.
 * Values outside [0, 1] are clamped. 0 -> cold (blue), 0.5 -> pale yellow,
 * 1 -> hot (red).
 */
export function scoreToColor(score: number): string {
  const t = clamp01(score);
  return t <= 0.5 ? mix(COLD, MID, t / 0.5) : mix(MID, HOT, (t - 0.5) / 0.5);
}
