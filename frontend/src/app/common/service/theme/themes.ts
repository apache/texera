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

/**
 * A theme is a flat map from CSS custom property name (without the leading
 * "--") to its value. Anything not specified inherits from the defaults in
 * styles/_tokens.scss, so a theme can override as few or as many tokens as it
 * likes.
 */
export type Theme = {
  /** Stable identifier (used for persistence). Kebab-case. */
  readonly id: string;
  /** Human-readable name shown in the picker. */
  readonly name: string;
  /** One-line description shown in the picker. */
  readonly description: string;
  /** Either "light" or "dark" — used to set color-scheme on :root. */
  readonly mode: "light" | "dark";
  /** Token overrides. */
  readonly tokens: Readonly<Record<string, string>>;
};

/* -- shared semantic palette helpers ------------------------------------- */
// Most themes reuse the same green/orange/red triad; only the bg/text/accent
// columns truly differ between themes. Defining these once keeps each theme
// declaration tight.
const STATUS_OK_LIGHT  = "#52c41a";
const STATUS_WARN_LIGHT = "#faad14";
const STATUS_BAD_LIGHT  = "#ff4d4f";

const STATUS_OK_DARK  = "#73d13d";
const STATUS_WARN_DARK = "#ffc53d";
const STATUS_BAD_DARK  = "#ff7875";

// ---- LIGHT (current Texera look, our default) ---------------------------
const LIGHT: Theme = {
  id: "light",
  name: "Light",
  description: "The classic Texera look.",
  mode: "light",
  tokens: {
    "tx-bg-base":        "#ffffff",
    "tx-bg-surface":     "#fafafa",
    "tx-bg-elevated":    "#ffffff",
    "tx-bg-canvas":      "#f5f7fa",
    "tx-bg-hover":       "rgba(0, 0, 0, 0.04)",
    "tx-bg-active":      "rgba(0, 0, 0, 0.08)",
    "tx-bg-overlay":     "rgba(0, 0, 0, 0.45)",
    "tx-text-primary":   "rgba(0, 0, 0, 0.88)",
    "tx-text-secondary": "rgba(0, 0, 0, 0.65)",
    "tx-text-tertiary":  "rgba(0, 0, 0, 0.45)",
    "tx-text-disabled":  "rgba(0, 0, 0, 0.25)",
    "tx-text-inverse":   "#ffffff",
    "tx-border-subtle":  "#f0f0f0",
    "tx-border-default": "#d9d9d9",
    "tx-border-strong":  "#bfbfbf",
    "tx-primary":        "#1890ff",
    "tx-primary-hover":  "#40a9ff",
    "tx-primary-active": "#096dd9",
    "tx-primary-soft":   "rgba(24, 144, 255, 0.10)",
    "tx-primary-fg":     "#ffffff",
    "tx-success":        STATUS_OK_LIGHT,
    "tx-success-soft":   "rgba(82, 196, 26, 0.12)",
    "tx-warning":        STATUS_WARN_LIGHT,
    "tx-warning-soft":   "rgba(250, 173, 20, 0.12)",
    "tx-danger":         STATUS_BAD_LIGHT,
    "tx-danger-soft":    "rgba(255, 77, 79, 0.12)",
    "tx-info":           "#1890ff",
    "tx-info-soft":      "rgba(24, 144, 255, 0.10)",
    // workspace canvas
    "tx-canvas-bg":      "#f6f6f6",
    "tx-canvas-grid":    "rgba(0, 0, 0, 0.18)",
    "tx-op-body-bg":     "#ffffff",
    "tx-op-body-stroke": "#cfcfcf",
    "tx-op-label":       "#595959",
    "tx-op-sublabel":    "#888888",
    "tx-op-port":        "#a0a0a0",
    "tx-link-stroke":    "#919191",
    "tx-link-handle":    "#919191",
    "tx-minimap-border": "#797a79",
    "tx-monaco-theme":   "vs",
  },
};

// ---- DARK ---------------------------------------------------------------
const DARK: Theme = {
  id: "dark",
  name: "Dark",
  description: "Easy on the eyes after sundown.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#141414",
    "tx-bg-surface":     "#1f1f1f",
    "tx-bg-elevated":    "#262626",
    "tx-bg-canvas":      "#181818",
    "tx-bg-hover":       "rgba(255, 255, 255, 0.06)",
    "tx-bg-active":      "rgba(255, 255, 255, 0.10)",
    "tx-bg-overlay":     "rgba(0, 0, 0, 0.65)",
    "tx-text-primary":   "rgba(255, 255, 255, 0.92)",
    "tx-text-secondary": "rgba(255, 255, 255, 0.70)",
    "tx-text-tertiary":  "rgba(255, 255, 255, 0.45)",
    "tx-text-disabled":  "rgba(255, 255, 255, 0.25)",
    "tx-text-inverse":   "#141414",
    "tx-border-subtle":  "#303030",
    "tx-border-default": "#434343",
    "tx-border-strong":  "#595959",
    "tx-primary":        "#1890ff",
    "tx-primary-hover":  "#40a9ff",
    "tx-primary-active": "#69c0ff",
    "tx-primary-soft":   "rgba(24, 144, 255, 0.20)",
    "tx-primary-fg":     "#ffffff",
    "tx-success":        STATUS_OK_DARK,
    "tx-success-soft":   "rgba(115, 209, 61, 0.18)",
    "tx-warning":        STATUS_WARN_DARK,
    "tx-warning-soft":   "rgba(255, 197, 61, 0.18)",
    "tx-danger":         STATUS_BAD_DARK,
    "tx-danger-soft":    "rgba(255, 120, 117, 0.18)",
    "tx-info":           "#1890ff",
    "tx-info-soft":      "rgba(24, 144, 255, 0.20)",
    "tx-shadow-sm":      "0 1px 2px rgba(0, 0, 0, 0.4)",
    "tx-shadow-md":      "0 2px 8px rgba(0, 0, 0, 0.5)",
    "tx-shadow-lg":      "0 8px 24px rgba(0, 0, 0, 0.6)",
    // workspace canvas
    "tx-canvas-bg":      "#181818",
    "tx-canvas-grid":    "rgba(255, 255, 255, 0.10)",
    "tx-op-body-bg":     "#262626",
    "tx-op-body-stroke": "#434343",
    "tx-op-label":       "rgba(255, 255, 255, 0.92)",
    "tx-op-sublabel":    "rgba(255, 255, 255, 0.55)",
    "tx-op-port":        "#7a7a7a",
    "tx-link-stroke":    "#a3a3a3",
    "tx-link-handle":    "#a3a3a3",
    "tx-minimap-border": "#888888",
    "tx-monaco-theme":   "vs-dark",
  },
};

// ---- SEPIA (warm, low-blue, paper-like) ---------------------------------
const SEPIA: Theme = {
  id: "sepia",
  name: "Sepia",
  description: "Warm, paper-feel. Low blue light.",
  mode: "light",
  tokens: {
    "tx-bg-base":        "#f4ecd8",
    "tx-bg-surface":     "#ebe1c9",
    "tx-bg-elevated":    "#f8f2e2",
    "tx-bg-canvas":      "#ede2c8",
    "tx-bg-hover":       "rgba(99, 65, 35, 0.06)",
    "tx-bg-active":      "rgba(99, 65, 35, 0.10)",
    "tx-text-primary":   "#5b4636",
    "tx-text-secondary": "rgba(91, 70, 54, 0.75)",
    "tx-text-tertiary":  "rgba(91, 70, 54, 0.55)",
    "tx-text-disabled":  "rgba(91, 70, 54, 0.35)",
    "tx-text-inverse":   "#f4ecd8",
    "tx-border-subtle":  "#d8cba8",
    "tx-border-default": "#c5b58e",
    "tx-border-strong":  "#a3936a",
    "tx-primary":        "#a0522d",
    "tx-primary-hover":  "#b86a3f",
    "tx-primary-active": "#874523",
    "tx-primary-soft":   "rgba(160, 82, 45, 0.15)",
    // workspace canvas
    "tx-canvas-bg":      "#ede2c8",
    "tx-canvas-grid":    "rgba(91, 70, 54, 0.20)",
    "tx-op-body-bg":     "#f8f2e2",
    "tx-op-body-stroke": "#c5b58e",
    "tx-op-label":       "#5b4636",
    "tx-op-sublabel":    "rgba(91, 70, 54, 0.65)",
    "tx-op-port":        "#a3936a",
    "tx-link-stroke":    "#876b4a",
    "tx-link-handle":    "#876b4a",
    "tx-minimap-border": "#a3936a",
    "tx-monaco-theme":   "vs",
  },
};

// ---- SOLARIZED DARK (Ethan Schoonover's palette) ------------------------
const SOLARIZED_DARK: Theme = {
  id: "solarized-dark",
  name: "Solarized Dark",
  description: "The Ethan Schoonover classic.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#002b36", // base03
    "tx-bg-surface":     "#073642", // base02
    "tx-bg-elevated":    "#0a3b46",
    "tx-bg-canvas":      "#00252e",
    "tx-bg-hover":       "rgba(147, 161, 161, 0.08)",
    "tx-bg-active":      "rgba(147, 161, 161, 0.14)",
    "tx-text-primary":   "#93a1a1", // base1
    "tx-text-secondary": "#839496", // base0
    "tx-text-tertiary":  "#586e75", // base01
    "tx-text-inverse":   "#002b36",
    "tx-border-subtle":  "#073642",
    "tx-border-default": "#586e75",
    "tx-border-strong":  "#657b83",
    "tx-primary":        "#268bd2", // blue
    "tx-primary-hover":  "#3a9bd9",
    "tx-primary-active": "#1e6fa8",
    "tx-primary-soft":   "rgba(38, 139, 210, 0.20)",
    "tx-success":        "#859900", // green
    "tx-warning":        "#b58900", // yellow
    "tx-danger":         "#dc322f", // red
    "tx-info":           "#2aa198", // cyan
    // workspace canvas
    "tx-canvas-bg":      "#00252e",
    "tx-canvas-grid":    "rgba(147, 161, 161, 0.18)",
    "tx-op-body-bg":     "#073642",
    "tx-op-body-stroke": "#586e75",
    "tx-op-label":       "#93a1a1",
    "tx-op-sublabel":    "#657b83",
    "tx-op-port":        "#586e75",
    "tx-link-stroke":    "#839496",
    "tx-link-handle":    "#839496",
    "tx-minimap-border": "#657b83",
    "tx-monaco-theme":   "vs-dark",
  },
};

// ---- GRUVBOX DARK (Pavel Pertsev's palette) -----------------------------
const GRUVBOX: Theme = {
  id: "gruvbox",
  name: "Gruvbox",
  description: "Retro warm dark. Heavy contrast.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#282828",
    "tx-bg-surface":     "#3c3836",
    "tx-bg-elevated":    "#504945",
    "tx-bg-canvas":      "#1d2021",
    "tx-bg-hover":       "rgba(235, 219, 178, 0.06)",
    "tx-bg-active":      "rgba(235, 219, 178, 0.12)",
    "tx-text-primary":   "#ebdbb2",
    "tx-text-secondary": "#d5c4a1",
    "tx-text-tertiary":  "#a89984",
    "tx-text-inverse":   "#282828",
    "tx-border-subtle":  "#3c3836",
    "tx-border-default": "#665c54",
    "tx-border-strong":  "#7c6f64",
    "tx-primary":        "#fabd2f", // yellow
    "tx-primary-hover":  "#fac440",
    "tx-primary-active": "#d79921",
    "tx-primary-soft":   "rgba(250, 189, 47, 0.18)",
    "tx-primary-fg":     "#282828",
    "tx-success":        "#b8bb26",
    "tx-warning":        "#fe8019",
    "tx-danger":         "#fb4934",
    "tx-info":           "#83a598",
    // workspace canvas
    "tx-canvas-bg":      "#1d2021",
    "tx-canvas-grid":    "rgba(235, 219, 178, 0.10)",
    "tx-op-body-bg":     "#3c3836",
    "tx-op-body-stroke": "#665c54",
    "tx-op-label":       "#ebdbb2",
    "tx-op-sublabel":    "#a89984",
    "tx-op-port":        "#7c6f64",
    "tx-link-stroke":    "#d5c4a1",
    "tx-link-handle":    "#fabd2f",
    "tx-minimap-border": "#a89984",
    "tx-monaco-theme":   "vs-dark",
  },
};

// ---- SYNTHWAVE (Outrun aesthetic — neon purple/pink on near-black) ------
const SYNTHWAVE: Theme = {
  id: "synthwave",
  name: "Synthwave",
  description: "Neon dreams. The 1984 future.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#1a1033",
    "tx-bg-surface":     "#241447",
    "tx-bg-elevated":    "#2d1b5e",
    "tx-bg-canvas":      "#13092a",
    "tx-bg-hover":       "rgba(255, 113, 206, 0.10)",
    "tx-bg-active":      "rgba(255, 113, 206, 0.18)",
    "tx-text-primary":   "#f8f8f2",
    "tx-text-secondary": "#c5c1e8",
    "tx-text-tertiary":  "#8b86c5",
    "tx-text-inverse":   "#1a1033",
    "tx-border-subtle":  "#352561",
    "tx-border-default": "#5a3aa3",
    "tx-border-strong":  "#7e57c2",
    "tx-primary":        "#ff71ce", // hot pink
    "tx-primary-hover":  "#ff8fd9",
    "tx-primary-active": "#e055b2",
    "tx-primary-soft":   "rgba(255, 113, 206, 0.20)",
    "tx-primary-fg":     "#1a1033",
    "tx-success":        "#05ffa1",
    "tx-warning":        "#fffb96",
    "tx-danger":         "#ff3864",
    "tx-info":           "#01cdfe",
    "tx-shadow-md":      "0 0 12px rgba(255, 113, 206, 0.18)",
    "tx-shadow-lg":      "0 0 32px rgba(255, 113, 206, 0.30)",
    // workspace canvas
    "tx-canvas-bg":      "#13092a",
    "tx-canvas-grid":    "rgba(255, 113, 206, 0.18)",
    "tx-op-body-bg":     "#241447",
    "tx-op-body-stroke": "#ff71ce",
    "tx-op-label":       "#f8f8f2",
    "tx-op-sublabel":    "#8b86c5",
    "tx-op-port":        "#01cdfe",
    "tx-link-stroke":    "#ff71ce",
    "tx-link-handle":    "#01cdfe",
    "tx-minimap-border": "#ff71ce",
    "tx-monaco-theme":   "vs-dark",
  },
};

// ---- FOREST (mossy greens, warm wood accents) ---------------------------
const FOREST: Theme = {
  id: "forest",
  name: "Forest",
  description: "Mossy greens, warm wood accents.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#1b2a1f",
    "tx-bg-surface":     "#243a2a",
    "tx-bg-elevated":    "#2d4a35",
    "tx-bg-canvas":      "#152019",
    "tx-bg-hover":       "rgba(184, 207, 168, 0.07)",
    "tx-bg-active":      "rgba(184, 207, 168, 0.14)",
    "tx-text-primary":   "#e8f0d8",
    "tx-text-secondary": "#b8cfa8",
    "tx-text-tertiary":  "#8aa67a",
    "tx-text-inverse":   "#1b2a1f",
    "tx-border-subtle":  "#2d4a35",
    "tx-border-default": "#4a6b50",
    "tx-border-strong":  "#6b8e6b",
    "tx-primary":        "#a3c585", // moss
    "tx-primary-hover":  "#b3d195",
    "tx-primary-active": "#88a86b",
    "tx-primary-soft":   "rgba(163, 197, 133, 0.18)",
    "tx-primary-fg":     "#1b2a1f",
    "tx-success":        "#7cb88c",
    "tx-warning":        "#d4a574",
    "tx-danger":         "#c97064",
    "tx-info":           "#8bb8a8",
    // workspace canvas
    "tx-canvas-bg":      "#152019",
    "tx-canvas-grid":    "rgba(184, 207, 168, 0.14)",
    "tx-op-body-bg":     "#243a2a",
    "tx-op-body-stroke": "#4a6b50",
    "tx-op-label":       "#e8f0d8",
    "tx-op-sublabel":    "#8aa67a",
    "tx-op-port":        "#6b8e6b",
    "tx-link-stroke":    "#b8cfa8",
    "tx-link-handle":    "#a3c585",
    "tx-minimap-border": "#6b8e6b",
    "tx-monaco-theme":   "vs-dark",
  },
};

// ---- CYBERPUNK (high-saturation neons on near-black) --------------------
const CYBERPUNK: Theme = {
  id: "cyberpunk",
  name: "Cyberpunk",
  description: "Yellow on cyan on black. Loud.",
  mode: "dark",
  tokens: {
    "tx-bg-base":        "#0a0e1a",
    "tx-bg-surface":     "#141a2e",
    "tx-bg-elevated":    "#1c2440",
    "tx-bg-canvas":      "#050810",
    "tx-bg-hover":       "rgba(255, 233, 0, 0.08)",
    "tx-bg-active":      "rgba(255, 233, 0, 0.16)",
    "tx-text-primary":   "#fcee0a", // electric yellow
    "tx-text-secondary": "#00f0ff", // cyan
    "tx-text-tertiary":  "#7aaab0",
    "tx-text-inverse":   "#0a0e1a",
    "tx-border-subtle":  "#1c2440",
    "tx-border-default": "#00f0ff",
    "tx-border-strong":  "#fcee0a",
    "tx-primary":        "#ff003c", // hot red
    "tx-primary-hover":  "#ff2050",
    "tx-primary-active": "#cc0030",
    "tx-primary-soft":   "rgba(255, 0, 60, 0.18)",
    "tx-primary-fg":     "#fcee0a",
    "tx-success":        "#00ff9f",
    "tx-warning":        "#ff9100",
    "tx-danger":         "#ff003c",
    "tx-info":           "#00f0ff",
    "tx-shadow-md":      "0 0 14px rgba(0, 240, 255, 0.25)",
    "tx-shadow-lg":      "0 0 36px rgba(252, 238, 10, 0.35)",
    // workspace canvas
    "tx-canvas-bg":      "#050810",
    "tx-canvas-grid":    "rgba(0, 240, 255, 0.20)",
    "tx-op-body-bg":     "#141a2e",
    "tx-op-body-stroke": "#00f0ff",
    "tx-op-label":       "#fcee0a",
    "tx-op-sublabel":    "#00f0ff",
    "tx-op-port":        "#ff003c",
    "tx-link-stroke":    "#00f0ff",
    "tx-link-handle":    "#fcee0a",
    "tx-minimap-border": "#fcee0a",
    "tx-monaco-theme":   "vs-dark",
  },
};

/**
 * All built-in themes, in the order they appear in the picker.
 */
export const BUILTIN_THEMES: ReadonlyArray<Theme> = [
  LIGHT,
  DARK,
  SEPIA,
  SOLARIZED_DARK,
  GRUVBOX,
  SYNTHWAVE,
  FOREST,
  CYBERPUNK,
];

/**
 * Lookup by id with a sensible default.
 */
export function findTheme(id: string | null | undefined): Theme {
  if (!id) return LIGHT;
  return BUILTIN_THEMES.find(t => t.id === id) ?? LIGHT;
}

/**
 * The theme to use the very first time a user visits, before they've made
 * a choice. Mirrors the system color scheme when available.
 */
export function defaultThemeForSystem(): Theme {
  if (
    typeof window !== "undefined" &&
    window.matchMedia &&
    window.matchMedia("(prefers-color-scheme: dark)").matches
  ) {
    return DARK;
  }
  return LIGHT;
}

export const DEFAULT_THEME: Theme = LIGHT;
