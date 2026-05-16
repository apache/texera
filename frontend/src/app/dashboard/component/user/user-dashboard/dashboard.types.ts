/**
 * Type definitions for the Dashboard Visualizer feature.
 */

export type WidgetType = "metric" | "bar" | "donut" | "hbar" | "text" | "table" | "html";

export interface MetricConfig {
  title: string;
  value: string;
  caption?: string;
  color?: string;
  icon?: string;
}

export interface BarSeries {
  name: string;
  color: string;
  values: number[];
}

export interface BarConfig {
  title: string;
  categories: string[];
  series: BarSeries[];
  yAxisLabel?: string;
  yMax?: number;
}

export interface DonutSegment {
  label: string;
  value: number;
  color: string;
}

export interface DonutConfig {
  title: string;
  segments: DonutSegment[];
  centerLabel?: string;
}

export interface HBarItem {
  label: string;
  value: number;
}

export interface HBarConfig {
  title: string;
  items: HBarItem[];
  color?: string;
  xMax?: number;
}

export interface TextConfig {
  title: string;
  body: string;
}

export interface TableConfig {
  title: string;
  columns: string[];
  rows: (string | number)[][];
}

export interface HtmlConfig {
  title: string;
  /** Raw HTML — usually a self-contained Plotly document from a visualization
   *  operator. Rendered into a sandboxed iframe via srcdoc. */
  htmlContent: string;
}

export type WidgetConfig =
  | { type: "metric"; config: MetricConfig }
  | { type: "bar"; config: BarConfig }
  | { type: "donut"; config: DonutConfig }
  | { type: "hbar"; config: HBarConfig }
  | { type: "text"; config: TextConfig }
  | { type: "table"; config: TableConfig }
  | { type: "html"; config: HtmlConfig };

/**
 * Position and size of a widget on the dashboard canvas.
 * All values are in pixels — free-form, not grid-snapped.
 */
export interface WidgetLayout {
  x: number;
  y: number;
  width: number;
  height: number;
}

/**
 * Where a widget's data came from. Used to label widgets on the dashboard.
 * Manual widgets carry no label. Workflow-sourced widgets show "From <name>"
 * so a viewer can see which run produced the number.
 */
export type WidgetSource =
  | { kind: "manual" }
  | {
      kind: "workflow";
      wid: number;
      workflowName: string;
      operatorName?: string;
      /** Human-readable label of what was pulled (e.g. "Accuracy", "Output rows"). */
      dataLabel?: string;
    };

export interface DashboardWidget {
  id: string;
  layout: WidgetLayout;
  widget: WidgetConfig;
  source?: WidgetSource;
}

export interface Dashboard {
  id: string;
  name: string;
  description?: string;
  createdAt: number;
  updatedAt: number;
  widgets: DashboardWidget[];
}

export const WIDGET_TYPE_LABELS: Record<WidgetType, string> = {
  metric: "Metric Card",
  bar: "Bar Chart",
  donut: "Donut Chart",
  hbar: "Horizontal Bar",
  text: "Text / Notes",
  table: "Table",
  html: "HTML Chart",
};

export const WIDGET_TYPE_DESCRIPTIONS: Record<WidgetType, string> = {
  metric: "A single large number with a label — great for headline stats.",
  bar: "Grouped bars for comparing values across categories.",
  donut: "Proportional segments for class/category distributions.",
  hbar: "Ranked horizontal bars — great for feature importance.",
  text: "Free-form notes, key findings, or callouts.",
  table: "A data table for detailed comparisons.",
  html: "Renders an HTML/Plotly chart from a visualization operator.",
};
