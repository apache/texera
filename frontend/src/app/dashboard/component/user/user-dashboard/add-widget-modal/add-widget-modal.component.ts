/**
 * Modal for adding a widget to a dashboard.
 * Step 1: pick a widget type.
 * Step 2: fill in data (form differs per type).
 */

import { Component, inject } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import {
  BarConfig,
  DonutConfig,
  HBarConfig,
  MetricConfig,
  TableConfig,
  TextConfig,
  WidgetConfig,
  WidgetType,
  WIDGET_TYPE_DESCRIPTIONS,
  WIDGET_TYPE_LABELS,
} from "../dashboard.types";

@Component({
  selector: "texera-add-widget-modal",
  templateUrl: "./add-widget-modal.component.html",
  styleUrls: ["./add-widget-modal.component.scss"],
  imports: [CommonModule, FormsModule, NzButtonComponent, NzInputDirective, NzIconDirective],
})
export class AddWidgetModalComponent {
  private modalRef = inject(NzModalRef);

  readonly widgetTypes: { type: WidgetType; icon: string; label: string; description: string }[] = [
    { type: "metric", icon: "field-number", label: WIDGET_TYPE_LABELS.metric, description: WIDGET_TYPE_DESCRIPTIONS.metric },
    { type: "bar", icon: "bar-chart", label: WIDGET_TYPE_LABELS.bar, description: WIDGET_TYPE_DESCRIPTIONS.bar },
    { type: "donut", icon: "pie-chart", label: WIDGET_TYPE_LABELS.donut, description: WIDGET_TYPE_DESCRIPTIONS.donut },
    { type: "hbar", icon: "menu", label: WIDGET_TYPE_LABELS.hbar, description: WIDGET_TYPE_DESCRIPTIONS.hbar },
    { type: "text", icon: "file-text", label: WIDGET_TYPE_LABELS.text, description: WIDGET_TYPE_DESCRIPTIONS.text },
    { type: "table", icon: "table", label: WIDGET_TYPE_LABELS.table, description: WIDGET_TYPE_DESCRIPTIONS.table },
  ];

  step: "pick" | "configure" = "pick";
  selectedType: WidgetType | null = null;

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

  pick(type: WidgetType): void {
    this.selectedType = type;
    this.step = "configure";
  }

  back(): void {
    this.step = "pick";
  }

  cancel(): void {
    this.modalRef.close(null);
  }

  submit(): void {
    if (!this.selectedType) return;
    let widget: WidgetConfig;
    switch (this.selectedType) {
      case "metric":
        widget = { type: "metric", config: { ...this.metric } };
        break;
      case "bar":
        widget = {
          type: "bar",
          config: {
            ...this.bar,
            categories: this.parseList(this.barCategoriesRaw),
            series: this.parseBarSeries(this.barSeriesRaw),
          },
        };
        break;
      case "donut":
        widget = {
          type: "donut",
          config: { ...this.donut, segments: this.parseDonutSegments(this.donutSegmentsRaw) },
        };
        break;
      case "hbar":
        widget = {
          type: "hbar",
          config: { ...this.hbar, items: this.parseHBarItems(this.hbarItemsRaw) },
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
            columns: this.parseList(this.tableColumnsRaw),
            rows: this.parseTableRows(this.tableRowsRaw),
          },
        };
        break;
      default:
        return;
    }
    this.modalRef.close(widget);
  }

  private parseList(raw: string): string[] {
    return raw
      .split(",")
      .map(s => s.trim())
      .filter(s => s.length > 0);
  }

  private parseBarSeries(raw: string) {
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

  private parseDonutSegments(raw: string) {
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

  private parseHBarItems(raw: string) {
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

  private parseTableRows(raw: string): (string | number)[][] {
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
}
