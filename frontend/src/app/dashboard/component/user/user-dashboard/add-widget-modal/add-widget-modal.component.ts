/**
 * Add Widget modal — two tabs.
 *
 *   Tab 1 "From Workflow": pick a workflow, see what data is available
 *     (snapshot results from localStorage + persisted runtime stats from
 *     REST), pick a data point, pick a widget type, add. Widgets are
 *     tagged with their source so the dashboard can show "From <workflow>".
 *
 *   Tab 2 "Manual Input": pick widget type, fill in data, add.
 *
 * The modal stays open after each Add so users can compose several widgets
 * in one session. Adds are dispatched via a callback passed in NZ_MODAL_DATA.
 */

import { Component, OnInit, inject } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { NzEmptyComponent } from "ng-zorro-antd/empty";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  BarConfig,
  DonutConfig,
  HBarConfig,
  MetricConfig,
  TableConfig,
  TextConfig,
  WidgetConfig,
  WidgetSource,
  WidgetType,
} from "../dashboard.types";
import {
  formatStatValue,
  OperatorBundle,
  WorkflowDataBundle,
  WorkflowDataService,
  WorkflowSummary,
} from "../workflow-data.service";

/** Callback contract — passed in via NZ_MODAL_DATA. */
export interface AddWidgetModalData {
  onAdd: (widget: WidgetConfig, source: WidgetSource) => void;
}

/**
 * A "data point" the user can pick from a workflow — represents one value
 * (or one operator output) that can be turned into a widget.
 */
type WorkflowDataPoint =
  | { kind: "metric"; opId: string; opName: string; name: string; value: number | string }
  | { kind: "stat"; opId: string; opName: string; name: string; value: number; valueLabel: string }
  | {
      kind: "rows";
      opId: string;
      opName: string;
      columns: string[];
      rows: (string | number | null)[][];
    };

@UntilDestroy()
@Component({
  selector: "texera-add-widget-modal",
  templateUrl: "./add-widget-modal.component.html",
  styleUrls: ["./add-widget-modal.component.scss"],
  imports: [
    CommonModule,
    FormsModule,
    NzButtonComponent,
    NzInputDirective,
    NzIconDirective,
    NzSelectComponent,
    NzOptionComponent,
    NzSpinComponent,
    NzTabsComponent,
    NzTabComponent,
    NzEmptyComponent,
  ],
})
export class AddWidgetModalComponent implements OnInit {
  private modalRef = inject(NzModalRef);
  private data = inject<AddWidgetModalData>(NZ_MODAL_DATA);

  addedCount = 0;
  justAddedFlash = false;
  /** 0 = From Workflow, 1 = Manual Input */
  activeTab = 0;

  // --- Tab 1: From Workflow ---------------------------------------------
  workflows: WorkflowSummary[] = [];
  loadingWorkflows = false;
  selectedWid: number | null = null;
  bundle: WorkflowDataBundle | null = null;
  loadingBundle = false;
  selectedDataPointKey: string | null = null;
  selectedWfWidgetType: WidgetType | null = null;

  // --- Tab 2: Manual ----------------------------------------------------
  manualType: WidgetType = "metric";
  metric: MetricConfig = { title: "Accuracy", value: "0.95", caption: "", color: "#3aa676" };
  bar: BarConfig = {
    title: "Bar Chart",
    categories: ["A", "B", "C"],
    series: [{ name: "Series 1", color: "#5b8def", values: [10, 20, 15] }],
  };
  donut: DonutConfig = {
    title: "Donut Chart",
    segments: [
      { label: "Group A", value: 60, color: "#5b8def" },
      { label: "Group B", value: 40, color: "#f06292" },
    ],
  };
  hbar: HBarConfig = {
    title: "Horizontal Bar",
    color: "#7c5cff",
    items: [
      { label: "Item 1", value: 0.5 },
      { label: "Item 2", value: 0.3 },
    ],
  };
  text: TextConfig = { title: "Notes", body: "" };
  table: TableConfig = {
    title: "Table",
    columns: ["Name", "Score"],
    rows: [
      ["Row 1", 0.9],
      ["Row 2", 0.7],
    ],
  };
  barCategoriesRaw = "A, B, C";
  barSeriesRaw = "Series 1 | #5b8def | 10, 20, 15";
  donutSegmentsRaw = "Group A | 60 | #5b8def\nGroup B | 40 | #f06292";
  hbarItemsRaw = "Item 1 | 0.5\nItem 2 | 0.3";
  tableColumnsRaw = "Name, Score";
  tableRowsRaw = "Row 1, 0.9\nRow 2, 0.7";

  readonly manualTypes: { type: WidgetType; icon: string; label: string }[] = [
    { type: "metric", icon: "field-number", label: "Metric Card" },
    { type: "bar", icon: "bar-chart", label: "Bar Chart" },
    { type: "donut", icon: "pie-chart", label: "Donut" },
    { type: "table", icon: "table", label: "Table" },
    { type: "text", icon: "file-text", label: "Text" },
  ];

  constructor(private workflowData: WorkflowDataService) {}

  ngOnInit(): void {
    this.loadingWorkflows = true;
    this.workflowData
      .listWorkflows()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ws => {
          this.workflows = ws;
          this.loadingWorkflows = false;
        },
        error: () => (this.loadingWorkflows = false),
      });
  }

  // --- Workflow tab logic ----------------------------------------------

  onWorkflowChange(wid: number | null): void {
    this.selectedWid = wid;
    this.bundle = null;
    this.selectedDataPointKey = null;
    this.selectedWfWidgetType = null;
    if (!wid) return;
    const wf = this.workflows.find(w => w.wid === wid);
    if (!wf) return;
    this.loadingBundle = true;
    this.workflowData
      .getWorkflowData(wf)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: b => {
          this.bundle = b;
          this.loadingBundle = false;
        },
        error: () => (this.loadingBundle = false),
      });
  }

  /** Every selectable data point in the current bundle, with a stable key. */
  get dataPoints(): Array<{ key: string; point: WorkflowDataPoint; section: "results" | "stats" }> {
    const out: Array<{ key: string; point: WorkflowDataPoint; section: "results" | "stats" }> = [];
    if (!this.bundle) return out;

    // Results (from localStorage snapshot)
    for (const ob of this.bundle.operators) {
      if (ob.metrics) {
        for (const [name, value] of Object.entries(ob.metrics)) {
          out.push({
            key: `metric|${ob.operator.operatorID}|${name}`,
            section: "results",
            point: {
              kind: "metric",
              opId: ob.operator.operatorID,
              opName: ob.operator.displayName,
              name,
              value,
            },
          });
        }
      }
      if (ob.snapshot && ob.snapshot.rows.length > 0) {
        out.push({
          key: `rows|${ob.operator.operatorID}`,
          section: "results",
          point: {
            kind: "rows",
            opId: ob.operator.operatorID,
            opName: ob.operator.displayName,
            columns: ob.snapshot.columns,
            rows: ob.snapshot.rows,
          },
        });
      }
    }

    // Stats (from REST)
    const statSpec: Array<{ key: keyof any; label: string; kind: "int" | "bytes" | "nanos" }> = [
      { key: "outputTupleCount", label: "Output rows", kind: "int" },
      { key: "inputTupleCount", label: "Input rows", kind: "int" },
      { key: "outputTupleSize", label: "Output size", kind: "bytes" },
      { key: "totalDataProcessingTime", label: "Processing time", kind: "nanos" },
    ];
    for (const ob of this.bundle.operators) {
      if (!ob.stats) continue;
      for (const s of statSpec) {
        const raw = Number((ob.stats as any)[s.key] ?? 0);
        out.push({
          key: `stat|${ob.operator.operatorID}|${String(s.key)}`,
          section: "stats",
          point: {
            kind: "stat",
            opId: ob.operator.operatorID,
            opName: ob.operator.displayName,
            name: s.label,
            value: raw,
            valueLabel: formatStatValue(raw, s.kind),
          },
        });
      }
    }
    return out;
  }

  get resultPoints() {
    return this.dataPoints.filter(p => p.section === "results");
  }
  get statPoints() {
    return this.dataPoints.filter(p => p.section === "stats");
  }
  get selectedPoint(): WorkflowDataPoint | undefined {
    return this.dataPoints.find(p => p.key === this.selectedDataPointKey)?.point;
  }

  /** Widget types that make sense for the current selected data point. */
  get availableWidgetTypes(): WidgetType[] {
    const p = this.selectedPoint;
    if (!p) return [];
    if (p.kind === "metric" || p.kind === "stat") return ["metric"];
    if (p.kind === "rows") return ["table", "bar", "donut"];
    return [];
  }

  pickDataPoint(key: string): void {
    this.selectedDataPointKey = key;
    const avail = this.availableWidgetTypes;
    this.selectedWfWidgetType = avail[0] ?? null;
  }

  pickWfWidgetType(t: WidgetType): void {
    this.selectedWfWidgetType = t;
  }

  get canAddFromWorkflow(): boolean {
    return !!this.selectedPoint && !!this.selectedWfWidgetType;
  }

  addFromWorkflow(): void {
    if (!this.canAddFromWorkflow || !this.bundle) return;
    const p = this.selectedPoint!;
    const t = this.selectedWfWidgetType!;
    const wf = this.bundle.workflow;

    const widget = buildWidgetFromPoint(t, p);
    if (!widget) return;
    const source: WidgetSource = {
      kind: "workflow",
      wid: wf.wid,
      workflowName: wf.name,
      operatorName: p.kind === "metric" || p.kind === "stat" || p.kind === "rows" ? p.opName : undefined,
      dataLabel: dataPointLabel(p),
    };
    this.data.onAdd(widget, source);
    this.flashAdded();
  }

  // --- Manual tab logic -------------------------------------------------

  pickManualType(t: WidgetType): void {
    this.manualType = t;
  }

  addManual(): void {
    let widget: WidgetConfig;
    switch (this.manualType) {
      case "metric":
        widget = { type: "metric", config: { ...this.metric } };
        break;
      case "bar":
        widget = {
          type: "bar",
          config: {
            ...this.bar,
            categories: parseList(this.barCategoriesRaw),
            series: parseBarSeries(this.barSeriesRaw),
          },
        };
        break;
      case "donut":
        widget = {
          type: "donut",
          config: { ...this.donut, segments: parseDonutSegments(this.donutSegmentsRaw) },
        };
        break;
      case "hbar":
        widget = {
          type: "hbar",
          config: { ...this.hbar, items: parseHBarItems(this.hbarItemsRaw) },
        };
        break;
      case "text":
        widget = { type: "text", config: { ...this.text } };
        break;
      case "table":
        widget = {
          type: "table",
          config: {
            ...this.table,
            columns: parseList(this.tableColumnsRaw),
            rows: parseTableRows(this.tableRowsRaw),
          },
        };
        break;
      default:
        return;
    }
    this.data.onAdd(widget, { kind: "manual" });
    this.flashAdded();
  }

  // --- Common ----------------------------------------------------------

  done(): void {
    this.modalRef.close({ addedCount: this.addedCount });
  }
  cancel(): void {
    this.modalRef.close(null);
  }

  private flashAdded(): void {
    this.addedCount += 1;
    this.justAddedFlash = true;
    setTimeout(() => (this.justAddedFlash = false), 1400);
  }
}

// --- Widget construction from a data point -------------------------------

function dataPointLabel(p: WorkflowDataPoint): string {
  if (p.kind === "metric") return p.name;
  if (p.kind === "stat") return p.name;
  return "Output rows";
}

function buildWidgetFromPoint(type: WidgetType, p: WorkflowDataPoint): WidgetConfig | undefined {
  if (p.kind === "metric") {
    const v = typeof p.value === "number" ? formatScalar(p.value) : String(p.value);
    return {
      type: "metric",
      config: {
        title: `${p.opName} · ${p.name}`,
        value: v,
        caption: "",
        color: "#3aa676",
      },
    };
  }
  if (p.kind === "stat") {
    return {
      type: "metric",
      config: {
        title: `${p.opName} · ${p.name}`,
        value: p.valueLabel,
        caption: "Latest run",
        color: "#1677ff",
      },
    };
  }
  if (p.kind === "rows") {
    if (type === "table") {
      return {
        type: "table",
        config: {
          title: `${p.opName} · output`,
          columns: p.columns,
          rows: p.rows.map(row => row.map(cell => (cell === null ? "" : cell))),
        },
      };
    }
    if (type === "bar" || type === "donut") {
      // Treat the first column as labels, the first numeric column as values.
      const labels = p.rows.map(row => String(row[0] ?? ""));
      const valueColIndex = findFirstNumericColumn(p.rows);
      const values = p.rows.map(row =>
        valueColIndex >= 0 ? Number(row[valueColIndex] ?? 0) : 0
      );
      const palette = ["#5b8def", "#3aa676", "#f06292", "#7c5cff", "#f0b429", "#36cfc9"];
      if (type === "bar") {
        return {
          type: "bar",
          config: {
            title: `${p.opName} · ${p.columns[valueColIndex] ?? "values"}`,
            categories: labels,
            series: [{ name: p.columns[valueColIndex] ?? "value", color: "#5b8def", values }],
          },
        };
      }
      return {
        type: "donut",
        config: {
          title: `${p.opName} · ${p.columns[valueColIndex] ?? "values"}`,
          segments: labels.map((label, i) => ({
            label,
            value: values[i],
            color: palette[i % palette.length],
          })),
        },
      };
    }
  }
  return undefined;
}

function findFirstNumericColumn(rows: (string | number | null)[][]): number {
  if (rows.length === 0) return -1;
  const ncols = Math.max(...rows.map(r => r.length));
  for (let c = 0; c < ncols; c++) {
    const allNumeric = rows.every(r => typeof r[c] === "number" || (typeof r[c] === "string" && !isNaN(Number(r[c]))));
    if (allNumeric) return c;
  }
  return -1;
}

function formatScalar(n: number): string {
  if (Number.isInteger(n)) return n.toString();
  if (Math.abs(n) <= 1) return n.toFixed(3).replace(/0+$/, "").replace(/\.$/, "");
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// --- Parsers (manual flow) ---------------------------------------------

function parseList(raw: string): string[] {
  return raw
    .split(",")
    .map(s => s.trim())
    .filter(s => s.length > 0);
}

function parseBarSeries(raw: string) {
  return raw
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const [name, color, values] = line.split("|").map(s => s.trim());
      return {
        name: name || "Series",
        color: color || "#5b8def",
        values: (values || "")
          .split(",")
          .map(v => Number(v.trim()))
          .filter(n => !isNaN(n)),
      };
    });
}

function parseDonutSegments(raw: string) {
  return raw
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const [label, value, color] = line.split("|").map(s => s.trim());
      return {
        label: label || "Segment",
        value: Number(value) || 0,
        color: color || "#5b8def",
      };
    });
}

function parseHBarItems(raw: string) {
  return raw
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const [label, value] = line.split("|").map(s => s.trim());
      return {
        label: label || "Item",
        value: Number(value) || 0,
      };
    });
}

function parseTableRows(raw: string): (string | number)[][] {
  return raw
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean)
    .map(line =>
      line.split(",").map(cell => {
        const t = cell.trim();
        const n = Number(t);
        return isNaN(n) || t === "" ? t : n;
      })
    );
}
