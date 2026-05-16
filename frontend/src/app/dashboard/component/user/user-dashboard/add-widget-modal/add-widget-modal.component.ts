/**
 * Add Widget modal. Two entry paths:
 *
 *   1. "From a Workflow" — pull real runtime stats from a workflow's latest
 *      execution. The user picks a workflow, a widget type, then a metric
 *      (and operator for a Metric Card). The widget is built from live stats.
 *
 *   2. "Manual Entry" — user types/pastes values directly. Useful for text
 *      callouts or when stats aren't a fit.
 *
 * Returned via NzModalRef.close(): { widget: WidgetConfig; source: WidgetSource }
 */

import { Component, OnInit, inject } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSelectComponent, NzOptionComponent } from "ng-zorro-antd/select";
import { NzSpinComponent } from "ng-zorro-antd/spin";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "../../../../type/workflow-runtime-statistics";
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
  WIDGET_TYPE_DESCRIPTIONS,
  WIDGET_TYPE_LABELS,
} from "../dashboard.types";
import {
  buildWidgetFromStats,
  METRIC_KEYS,
  METRIC_LABELS,
  MetricKey,
  OperatorSummary,
  WorkflowDataService,
  WorkflowSummary,
} from "../workflow-data.service";

export interface AddWidgetResult {
  widget: WidgetConfig;
  source: WidgetSource;
}

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
  ],
})
export class AddWidgetModalComponent implements OnInit {
  private modalRef = inject(NzModalRef);

  // --- High-level flow state ---------------------------------------------
  step: "choose-source" | "from-workflow" | "manual-pick" | "manual-configure" = "choose-source";

  // --- From-Workflow state -----------------------------------------------
  workflows: WorkflowSummary[] = [];
  loadingWorkflows = false;
  selectedWid: number | null = null;
  operators: OperatorSummary[] = [];
  stats = new Map<string, WorkflowRuntimeStatistics>();
  loadingSnapshot = false;

  /**
   * Widget types that work with workflow stats. Text is intentionally
   * excluded — text widgets don't render numbers.
   */
  workflowWidgetTypes: { type: WidgetType; icon: string; label: string; description: string; scope: "single-operator" | "all-operators" }[] = [
    { type: "metric", icon: "field-number", label: "Metric Card", description: "One operator, one metric — a single big number.", scope: "single-operator" },
    { type: "bar", icon: "bar-chart", label: "Bar Chart", description: "Compare one metric across every operator.", scope: "all-operators" },
    { type: "hbar", icon: "menu", label: "Horizontal Bar Chart", description: "Ranked operators by one metric.", scope: "all-operators" },
    { type: "donut", icon: "pie-chart", label: "Donut Chart", description: "Each operator's share of a metric.", scope: "all-operators" },
    { type: "table", icon: "table", label: "Stats Table", description: "Every operator × every metric.", scope: "all-operators" },
  ];

  selectedWfWidgetType: WidgetType | null = null;
  selectedScope: "single-operator" | "all-operators" = "single-operator";
  selectedOpId: string | null = null;
  selectedMetric: MetricKey = "outputTupleCount";

  readonly metricOptions = METRIC_KEYS.map(k => ({ value: k, label: METRIC_LABELS[k] }));

  // --- Manual flow state -------------------------------------------------
  readonly allWidgetTypes: { type: WidgetType; icon: string; label: string; description: string }[] = [
    { type: "metric", icon: "field-number", label: WIDGET_TYPE_LABELS.metric, description: WIDGET_TYPE_DESCRIPTIONS.metric },
    { type: "bar", icon: "bar-chart", label: WIDGET_TYPE_LABELS.bar, description: WIDGET_TYPE_DESCRIPTIONS.bar },
    { type: "donut", icon: "pie-chart", label: WIDGET_TYPE_LABELS.donut, description: WIDGET_TYPE_DESCRIPTIONS.donut },
    { type: "hbar", icon: "menu", label: WIDGET_TYPE_LABELS.hbar, description: WIDGET_TYPE_DESCRIPTIONS.hbar },
    { type: "text", icon: "file-text", label: WIDGET_TYPE_LABELS.text, description: WIDGET_TYPE_DESCRIPTIONS.text },
    { type: "table", icon: "table", label: WIDGET_TYPE_LABELS.table, description: WIDGET_TYPE_DESCRIPTIONS.table },
  ];

  manualType: WidgetType | null = null;
  metric: MetricConfig = { title: "Metric", value: "0", caption: "", color: "#4cc9f0" };
  bar: BarConfig = {
    title: "Bar Chart",
    categories: ["A", "B", "C"],
    series: [{ name: "Series 1", color: "#4cc9f0", values: [10, 20, 15] }],
  };
  donut: DonutConfig = {
    title: "Donut Chart",
    segments: [
      { label: "Group A", value: 60, color: "#4cc9f0" },
      { label: "Group B", value: 40, color: "#f5587b" },
    ],
  };
  hbar: HBarConfig = {
    title: "Horizontal Bar",
    color: "#7c5cff",
    items: [
      { label: "Item 1", value: 0.5 },
      { label: "Item 2", value: 0.3 },
      { label: "Item 3", value: 0.2 },
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
  barSeriesRaw = "Series 1 | #4cc9f0 | 10, 20, 15";
  donutSegmentsRaw = "Group A | 60 | #4cc9f0\nGroup B | 40 | #f5587b";
  hbarItemsRaw = "Item 1 | 0.5\nItem 2 | 0.3\nItem 3 | 0.2";
  tableColumnsRaw = "Name, Score";
  tableRowsRaw = "Row 1, 0.9\nRow 2, 0.7";

  constructor(private workflowData: WorkflowDataService) {}

  ngOnInit(): void {
    this.loadWorkflowsList();
  }

  private loadWorkflowsList(): void {
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

  // --- Navigation --------------------------------------------------------

  goWorkflow(): void {
    this.step = "from-workflow";
  }

  goManual(): void {
    this.step = "manual-pick";
  }

  back(): void {
    if (this.step === "manual-configure") this.step = "manual-pick";
    else if (this.step === "manual-pick" || this.step === "from-workflow") this.step = "choose-source";
  }

  cancel(): void {
    this.modalRef.close(null);
  }

  // --- From Workflow -----------------------------------------------------

  onWorkflowChange(wid: number): void {
    this.selectedWid = wid;
    this.operators = [];
    this.stats = new Map();
    this.selectedOpId = null;
    if (!wid) return;
    this.loadingSnapshot = true;
    this.workflowData
      .getWorkflowSnapshot(wid)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: snap => {
          this.operators = snap.operators;
          this.stats = snap.stats;
          this.loadingSnapshot = false;
        },
        error: () => (this.loadingSnapshot = false),
      });
  }

  pickWfWidgetType(t: WidgetType): void {
    this.selectedWfWidgetType = t;
    const def = this.workflowWidgetTypes.find(x => x.type === t);
    if (def) this.selectedScope = def.scope;
    if (this.selectedScope === "all-operators") {
      this.selectedOpId = null;
    } else if (!this.selectedOpId && this.operators.length > 0) {
      this.selectedOpId = this.operators[0].operatorID;
    }
  }

  get currentWorkflow(): WorkflowSummary | undefined {
    return this.workflows.find(w => w.wid === this.selectedWid);
  }

  get statsCount(): number {
    return this.stats.size;
  }

  get canSubmitWorkflow(): boolean {
    if (!this.selectedWid || !this.selectedWfWidgetType) return false;
    if (this.selectedScope === "single-operator" && !this.selectedOpId) return false;
    return this.operators.length > 0;
  }

  submitWorkflow(): void {
    if (!this.canSubmitWorkflow || !this.selectedWid || !this.selectedWfWidgetType) return;
    const wf = this.currentWorkflow!;
    const op = this.operators.find(o => o.operatorID === this.selectedOpId);
    const source: WidgetSource = {
      kind: "workflow",
      wid: this.selectedWid,
      workflowName: wf.name,
      scope: this.selectedScope,
      operatorId: this.selectedScope === "single-operator" ? op?.operatorID : undefined,
      operatorName: this.selectedScope === "single-operator" ? op?.displayName : undefined,
      metric: this.selectedMetric,
    };
    const widget = buildWidgetFromStats(
      this.selectedWfWidgetType,
      source,
      this.operators,
      this.stats
    );
    if (!widget) return;
    const result: AddWidgetResult = { widget, source };
    this.modalRef.close(result);
  }

  // --- Manual ------------------------------------------------------------

  pickManualType(t: WidgetType): void {
    this.manualType = t;
    this.step = "manual-configure";
  }

  submitManual(): void {
    if (!this.manualType) return;
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
    const result: AddWidgetResult = { widget, source: { kind: "manual" } };
    this.modalRef.close(result);
  }
}

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
        color: color || "#4cc9f0",
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
        color: color || "#4cc9f0",
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
