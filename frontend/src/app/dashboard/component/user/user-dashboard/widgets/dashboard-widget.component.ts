/**
 * Renders a single dashboard widget. All charts are hand-rolled SVG —
 * no external charting library, no plotly, no d3.
 */

import { ChangeDetectionStrategy, Component, Input } from "@angular/core";
import { CommonModule } from "@angular/common";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import {
  BarConfig,
  DonutConfig,
  HBarConfig,
  HtmlConfig,
  MetricConfig,
  TableConfig,
  TextConfig,
  WidgetConfig,
} from "../dashboard.types";

interface DonutSlice {
  label: string;
  value: number;
  pct: number;
  color: string;
  dashArray: string;
  dashOffset: number;
}

@Component({
  selector: "texera-dashboard-widget",
  templateUrl: "./dashboard-widget.component.html",
  styleUrls: ["./dashboard-widget.component.scss"],
  imports: [CommonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class DashboardWidgetComponent {
  private _widget!: WidgetConfig;
  /** Cached SafeHtml — recomputed only when the underlying htmlContent
   *  string actually changes. Without this, the getter returned a fresh
   *  SafeHtml object on every CD cycle and the iframe's [srcdoc] binding
   *  thrashed, causing the embedded Plotly chart to flicker. */
  private _cachedHtml: string | null = null;
  private _safeHtmlContent: SafeHtml | null = null;

  @Input() set widget(value: WidgetConfig) {
    this._widget = value;
    if (value && value.type === "html") {
      const next = value.config.htmlContent ?? "";
      if (next !== this._cachedHtml) {
        this._cachedHtml = next;
        this._safeHtmlContent = this.sanitizer.bypassSecurityTrustHtml(next);
      }
    } else {
      this._cachedHtml = null;
      this._safeHtmlContent = null;
    }
  }
  get widget(): WidgetConfig {
    return this._widget;
  }

  constructor(private sanitizer: DomSanitizer) {}

  get metric(): MetricConfig {
    return this.widget.config as MetricConfig;
  }
  get bar(): BarConfig {
    return this.widget.config as BarConfig;
  }
  get donut(): DonutConfig {
    return this.widget.config as DonutConfig;
  }
  get hbar(): HBarConfig {
    return this.widget.config as HBarConfig;
  }
  get text(): TextConfig {
    return this.widget.config as TextConfig;
  }
  get table(): TableConfig {
    return this.widget.config as TableConfig;
  }
  get html(): HtmlConfig {
    return this.widget.config as HtmlConfig;
  }

  /**
   * The iframe's srcdoc bypasses Angular sanitization so inline Plotly
   * scripts in the HTML actually execute. Returns the cached SafeHtml
   * computed in the widget setter — never recomputes per CD cycle.
   */
  get safeHtmlContent(): SafeHtml | null {
    return this._safeHtmlContent;
  }

  // --- Bar chart helpers ---------------------------------------------------

  readonly BAR_CHART_WIDTH = 600;
  readonly BAR_CHART_HEIGHT = 280;
  readonly BAR_PAD = { top: 20, right: 20, bottom: 40, left: 50 };

  get barPlotWidth(): number {
    return this.BAR_CHART_WIDTH - this.BAR_PAD.left - this.BAR_PAD.right;
  }
  get barPlotHeight(): number {
    return this.BAR_CHART_HEIGHT - this.BAR_PAD.top - this.BAR_PAD.bottom;
  }
  get barYMax(): number {
    const cfg = this.bar;
    if (cfg.yMax !== undefined) return cfg.yMax;
    let max = 0;
    for (const s of cfg.series) for (const v of s.values) max = Math.max(max, v);
    return max === 0 ? 1 : max;
  }

  get barGroups(): Array<{ x: number; bars: Array<{ x: number; y: number; w: number; h: number; color: string }> }> {
    const cfg = this.bar;
    const groupCount = cfg.categories.length;
    const seriesCount = cfg.series.length;
    if (groupCount === 0 || seriesCount === 0) return [];

    const groupGap = 0.2;
    const groupWidth = this.barPlotWidth / groupCount;
    const innerWidth = groupWidth * (1 - groupGap);
    const barWidth = innerWidth / seriesCount;
    const yMax = this.barYMax;

    const groups = [];
    for (let g = 0; g < groupCount; g++) {
      const groupX = this.BAR_PAD.left + g * groupWidth + (groupWidth - innerWidth) / 2;
      const bars = [];
      for (let s = 0; s < seriesCount; s++) {
        const v = cfg.series[s].values[g] ?? 0;
        const h = (v / yMax) * this.barPlotHeight;
        bars.push({
          x: groupX + s * barWidth,
          y: this.BAR_PAD.top + this.barPlotHeight - h,
          w: barWidth - 2,
          h,
          color: cfg.series[s].color,
        });
      }
      groups.push({ x: groupX + innerWidth / 2, bars });
    }
    return groups;
  }

  get barYTicks(): Array<{ y: number; label: string }> {
    const yMax = this.barYMax;
    return [0, 0.25, 0.5, 0.75, 1].map(t => {
      const value = t * yMax;
      return {
        y: this.BAR_PAD.top + this.barPlotHeight - t * this.barPlotHeight,
        label: this.formatNumber(value),
      };
    });
  }

  // --- Donut helpers -------------------------------------------------------

  readonly DONUT_SIZE = 220;
  readonly DONUT_RADIUS = 70;
  readonly DONUT_STROKE = 32;

  get donutCircumference(): number {
    return 2 * Math.PI * this.DONUT_RADIUS;
  }

  get donutSlices(): DonutSlice[] {
    const cfg = this.donut;
    const total = cfg.segments.reduce((s, x) => s + x.value, 0) || 1;
    const C = this.donutCircumference;
    let offset = 0;
    return cfg.segments.map(seg => {
      const pct = seg.value / total;
      const len = pct * C;
      const slice: DonutSlice = {
        label: seg.label,
        value: seg.value,
        pct,
        color: seg.color,
        dashArray: `${len} ${C - len}`,
        dashOffset: -offset,
      };
      offset += len;
      return slice;
    });
  }

  // --- Horizontal bar helpers ----------------------------------------------

  readonly HBAR_ROW_HEIGHT = 28;
  readonly HBAR_LABEL_WIDTH = 130;
  readonly HBAR_RIGHT_PAD = 50;
  readonly HBAR_WIDTH = 480;

  get hbarMax(): number {
    const cfg = this.hbar;
    if (cfg.xMax !== undefined) return cfg.xMax;
    let max = 0;
    for (const it of cfg.items) max = Math.max(max, it.value);
    return max === 0 ? 1 : max;
  }
  get hbarPlotWidth(): number {
    return this.HBAR_WIDTH - this.HBAR_LABEL_WIDTH - this.HBAR_RIGHT_PAD;
  }
  get hbarRows(): Array<{ label: string; value: number; barWidth: number; valueLabel: string }> {
    const cfg = this.hbar;
    const max = this.hbarMax;
    return cfg.items.map(it => ({
      label: it.label,
      value: it.value,
      barWidth: (it.value / max) * this.hbarPlotWidth,
      valueLabel: this.formatNumber(it.value),
    }));
  }
  get hbarSvgHeight(): number {
    return this.hbar.items.length * this.HBAR_ROW_HEIGHT + 16;
  }

  formatNumber(n: number): string {
    if (Number.isInteger(n)) return n.toString();
    if (Math.abs(n) < 1) return n.toFixed(2);
    if (Math.abs(n) < 10) return n.toFixed(2);
    return n.toFixed(1);
  }
}
