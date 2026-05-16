/**
 * Add Widget modal — multi-select.
 *
 * UX: user checks one or more widget templates from a gallery, fills in the
 * data inline for each, clicks "Add These". Widgets are added immediately to
 * the dashboard via a callback supplied through NZ_MODAL_DATA, and the modal
 * stays open so users can compose more in another pass. "Done" closes it.
 *
 * Manual entry only — there's no REST endpoint that returns operator output
 * data, so we don't pretend. Templates (Accuracy, F1, Model Comparison etc.)
 * pre-fill plausible defaults so the form feels like quick data entry rather
 * than a long configuration.
 */

import { Component, inject } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import {
  BarConfig,
  DonutConfig,
  HBarConfig,
  MetricConfig,
  TableConfig,
  TextConfig,
  WidgetConfig,
  WidgetType,
} from "../dashboard.types";

export interface AddWidgetModalData {
  /** Called every time the user clicks "Add These". The modal stays open. */
  onAdd: (widgets: WidgetConfig[]) => void;
}

interface WidgetEntry<T> {
  enabled: boolean;
  config: T;
  /** Free-form text for fields that the user enters as a string. */
  raw?: Record<string, string>;
}

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
    NzCheckboxComponent,
  ],
})
export class AddWidgetModalComponent {
  private modalRef = inject(NzModalRef);
  private data = inject<AddWidgetModalData>(NZ_MODAL_DATA);

  /** Tracks how many widgets the user has added in this modal session. */
  addedCount = 0;
  justAddedFlash = false;

  metric: WidgetEntry<MetricConfig> = {
    enabled: false,
    config: { title: "Accuracy", value: "96.7%", caption: "On held-out test set", color: "#3aa676" },
  };

  bar: WidgetEntry<BarConfig> = {
    enabled: false,
    config: {
      title: "Model Comparison",
      categories: ["Logistic Reg.", "Random Forest", "Gradient Boost"],
      series: [
        { name: "Accuracy", color: "#3aa676", values: [0.91, 0.94, 0.95] },
        { name: "F1", color: "#5b8def", values: [0.89, 0.93, 0.94] },
      ],
    },
    raw: {
      categoriesRaw: "Logistic Reg., Random Forest, Gradient Boost",
      seriesRaw: "Accuracy | #3aa676 | 0.91, 0.94, 0.95\nF1 | #5b8def | 0.89, 0.93, 0.94",
    },
  };

  donut: WidgetEntry<DonutConfig> = {
    enabled: false,
    config: {
      title: "Class Distribution",
      segments: [
        { label: "Class A", value: 65, color: "#5b8def" },
        { label: "Class B", value: 35, color: "#f06292" },
      ],
    },
    raw: {
      segmentsRaw: "Class A | 65 | #5b8def\nClass B | 35 | #f06292",
    },
  };

  hbar: WidgetEntry<HBarConfig> = {
    enabled: false,
    config: {
      title: "Feature Importance",
      color: "#7c5cff",
      items: [
        { label: "Feature 1", value: 0.4 },
        { label: "Feature 2", value: 0.25 },
        { label: "Feature 3", value: 0.15 },
      ],
    },
    raw: {
      itemsRaw: "Feature 1 | 0.40\nFeature 2 | 0.25\nFeature 3 | 0.15",
    },
  };

  text: WidgetEntry<TextConfig> = {
    enabled: false,
    config: {
      title: "Key Findings",
      body:
        "• Best model: Gradient Boosting (F1=0.94)\n" +
        "• Top feature: Feature 1 (40% importance)\n" +
        "• Next step: validate on held-out cohort",
    },
  };

  table: WidgetEntry<TableConfig> = {
    enabled: false,
    config: {
      title: "Model Metrics",
      columns: ["Model", "Accuracy", "Precision", "F1"],
      rows: [
        ["Logistic Reg.", 0.91, 0.9, 0.89],
        ["Random Forest", 0.94, 0.93, 0.93],
        ["Gradient Boost", 0.95, 0.94, 0.94],
      ],
    },
    raw: {
      columnsRaw: "Model, Accuracy, Precision, F1",
      rowsRaw: "Logistic Reg., 0.91, 0.9, 0.89\nRandom Forest, 0.94, 0.93, 0.93\nGradient Boost, 0.95, 0.94, 0.94",
    },
  };

  /**
   * Templates shown in the picker. Each one toggles `enabled` on its entry.
   * The metric template is parameterized so we can offer several presets
   * (Accuracy/F1/Precision/Recall) that all create a single Metric Card —
   * picking one pre-fills `metric.config` then enables it.
   */
  metricPresets = [
    { key: "accuracy", label: "Accuracy", value: "96.7%", color: "#3aa676" },
    { key: "f1", label: "F1 Score", value: "0.94", color: "#5b8def" },
    { key: "precision", label: "Precision", value: "0.93", color: "#7c5cff" },
    { key: "recall", label: "Recall", value: "0.92", color: "#f0b429" },
  ];

  get selectedCount(): number {
    return (
      Number(this.metric.enabled) +
      Number(this.bar.enabled) +
      Number(this.donut.enabled) +
      Number(this.hbar.enabled) +
      Number(this.text.enabled) +
      Number(this.table.enabled)
    );
  }

  applyMetricPreset(p: { label: string; value: string; color: string }): void {
    this.metric.config = {
      title: p.label,
      value: p.value,
      caption: "",
      color: p.color,
    };
    this.metric.enabled = true;
  }

  done(): void {
    this.modalRef.close({ addedCount: this.addedCount });
  }

  cancel(): void {
    this.modalRef.close(null);
  }

  addSelected(): void {
    const widgets: WidgetConfig[] = [];
    if (this.metric.enabled) {
      widgets.push({ type: "metric", config: { ...this.metric.config } });
    }
    if (this.bar.enabled) {
      widgets.push({
        type: "bar",
        config: {
          ...this.bar.config,
          categories: parseList(this.bar.raw!["categoriesRaw"]),
          series: parseBarSeries(this.bar.raw!["seriesRaw"]),
        },
      });
    }
    if (this.donut.enabled) {
      widgets.push({
        type: "donut",
        config: {
          ...this.donut.config,
          segments: parseDonutSegments(this.donut.raw!["segmentsRaw"]),
        },
      });
    }
    if (this.hbar.enabled) {
      widgets.push({
        type: "hbar",
        config: {
          ...this.hbar.config,
          items: parseHBarItems(this.hbar.raw!["itemsRaw"]),
        },
      });
    }
    if (this.text.enabled) {
      widgets.push({ type: "text", config: { ...this.text.config } });
    }
    if (this.table.enabled) {
      widgets.push({
        type: "table",
        config: {
          ...this.table.config,
          columns: parseList(this.table.raw!["columnsRaw"]),
          rows: parseTableRows(this.table.raw!["rowsRaw"]),
        },
      });
    }
    if (widgets.length === 0) return;

    this.data.onAdd(widgets);
    this.addedCount += widgets.length;
    this.justAddedFlash = true;
    setTimeout(() => (this.justAddedFlash = false), 1400);
    // Uncheck so the user can build a new batch without clearing manually
    this.metric.enabled = false;
    this.bar.enabled = false;
    this.donut.enabled = false;
    this.hbar.enabled = false;
    this.text.enabled = false;
    this.table.enabled = false;
  }
}

// --- Parsers shared with the configure forms -----------------------------

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
