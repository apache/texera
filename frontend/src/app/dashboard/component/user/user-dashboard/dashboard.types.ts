/**
 * Type definitions for the Dashboard Visualizer feature.
 */

export type WidgetType = "metric" | "bar" | "donut" | "hbar" | "text" | "table";

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

export type WidgetConfig =
  | { type: "metric"; config: MetricConfig }
  | { type: "bar"; config: BarConfig }
  | { type: "donut"; config: DonutConfig }
  | { type: "hbar"; config: HBarConfig }
  | { type: "text"; config: TextConfig }
  | { type: "table"; config: TableConfig };

export interface WidgetLayout {
  x: number;
  y: number;
  w: number;
  h: number;
}

/**
 * Where the widget's data came from.
 * - `manual`: user entered values directly
 * - `workflow`: pulled from a workflow's runtime statistics. `scope` decides
 *    whether the widget is one operator's metric ("single-operator", e.g. a
 *    Metric Card) or every operator's metric ("all-operators", e.g. a bar
 *    chart across operators).
 */
export type WidgetSource =
  | { kind: "manual" }
  | {
      kind: "workflow";
      wid: number;
      workflowName: string;
      scope: "single-operator" | "all-operators";
      operatorId?: string;
      operatorName?: string;
      metric: string;
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
  hbar: "Horizontal Bar Chart",
  text: "Text / Notes",
  table: "Table",
};

export const WIDGET_TYPE_DESCRIPTIONS: Record<WidgetType, string> = {
  metric: "A single large number with a label — great for headline stats.",
  bar: "Grouped bars for comparing values across categories.",
  donut: "Proportional segments for class/category distributions.",
  hbar: "Ranked horizontal bars — great for feature importance.",
  text: "Free-form notes, key findings, or callouts.",
  table: "A data table for detailed comparisons.",
};
