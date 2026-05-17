/**
 * Storage and CRUD for user-built dashboards. localStorage-backed for the
 * hackathon — no backend required.
 */

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { Dashboard, DashboardWidget, WidgetConfig, WidgetLayout, WidgetType } from "./dashboard.types";

const STORAGE_KEY = "texera.dashboards.v1";
/** Old IDs we created in earlier iterations — purge them on load so old
 *  installs don't keep showing fake demo data. */
const LEGACY_SEED_IDS = new Set(["seed-diabetes", "seed-empty"]);

@Injectable({ providedIn: "root" })
export class DashboardService {
  private dashboards$ = new BehaviorSubject<Dashboard[]>([]);

  constructor() {
    this.load();
  }

  list(): Observable<Dashboard[]> {
    return this.dashboards$.asObservable();
  }

  snapshot(): Dashboard[] {
    return this.dashboards$.value;
  }

  get(id: string): Dashboard | undefined {
    return this.dashboards$.value.find(d => d.id === id);
  }

  create(name: string, description?: string): Dashboard {
    const now = Date.now();
    const dashboard: Dashboard = {
      id: this.genId(),
      name: name.trim() || "Untitled Dashboard",
      description: description?.trim(),
      createdAt: now,
      updatedAt: now,
      widgets: [],
    };
    this.dashboards$.next([...this.dashboards$.value, dashboard]);
    this.persist();
    return dashboard;
  }

  rename(id: string, name: string): void {
    const next = this.dashboards$.value.map(d =>
      d.id === id ? { ...d, name: name.trim() || d.name, updatedAt: Date.now() } : d
    );
    this.dashboards$.next(next);
    this.persist();
  }

  remove(id: string): void {
    this.dashboards$.next(this.dashboards$.value.filter(d => d.id !== id));
    this.persist();
  }

  saveDashboard(dashboard: Dashboard): void {
    const next = this.dashboards$.value.map(d =>
      d.id === dashboard.id ? { ...dashboard, updatedAt: Date.now() } : d
    );
    this.dashboards$.next(next);
    this.persist();
  }

  addWidget(
    dashboardId: string,
    widget: WidgetConfig,
    source?: DashboardWidget["source"]
  ): DashboardWidget | undefined {
    const dash = this.get(dashboardId);
    if (!dash) {
      return undefined;
    }
    const layout = this.nextLayout(dash.widgets, widget.type);
    const dw: DashboardWidget = {
      id: this.genId(),
      layout,
      widget,
      source,
    };
    const updated: Dashboard = { ...dash, widgets: [...dash.widgets, dw], updatedAt: Date.now() };
    this.saveDashboard(updated);
    return dw;
  }


  updateWidget(dashboardId: string, widgetId: string, widget: WidgetConfig): void {
    const dash = this.get(dashboardId);
    if (!dash) return;
    const updated: Dashboard = {
      ...dash,
      widgets: dash.widgets.map(w => (w.id === widgetId ? { ...w, widget } : w)),
      updatedAt: Date.now(),
    };
    this.saveDashboard(updated);
  }

  updateLayout(dashboardId: string, widgetId: string, layout: WidgetLayout): void {
    const dash = this.get(dashboardId);
    if (!dash) return;
    const updated: Dashboard = {
      ...dash,
      widgets: dash.widgets.map(w => (w.id === widgetId ? { ...w, layout } : w)),
      updatedAt: Date.now(),
    };
    this.saveDashboard(updated);
  }

  removeWidget(dashboardId: string, widgetId: string): void {
    const dash = this.get(dashboardId);
    if (!dash) return;
    const updated: Dashboard = {
      ...dash,
      widgets: dash.widgets.filter(w => w.id !== widgetId),
      updatedAt: Date.now(),
    };
    this.saveDashboard(updated);
  }

  /**
   * Default pixel size per widget type when adding a new widget. The widget
   * is placed below all existing widgets so it never overlaps and never
   * appears under the cursor.
   */
  private nextLayout(existing: DashboardWidget[], type: WidgetType): WidgetLayout {
    const sizes: Record<WidgetType, { width: number; height: number }> = {
      metric: { width: 320, height: 180 },
      bar: { width: 580, height: 360 },
      donut: { width: 360, height: 320 },
      hbar: { width: 520, height: 320 },
      text: { width: 320, height: 240 },
      table: { width: 640, height: 320 },
      html: { width: 600, height: 400 },
    };
    const size = sizes[type];
    let maxBottom = 0;
    for (const w of existing) {
      maxBottom = Math.max(maxBottom, w.layout.y + w.layout.height);
    }
    const GAP = 16;
    return {
      x: 16,
      y: existing.length === 0 ? 16 : maxBottom + GAP,
      width: size.width,
      height: size.height,
    };
  }

  private load(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed: Dashboard[] = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          const cleaned = parsed
            .filter(d => !LEGACY_SEED_IDS.has(d.id))
            .map(migrateLegacyLayout);
          this.dashboards$.next(cleaned);
          if (cleaned.length !== parsed.length) {
            this.persist();
          }
          return;
        }
      }
    } catch (e) {
      console.warn("Failed to load dashboards from localStorage", e);
    }
    this.dashboards$.next([]);
  }

  private persist(): void {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(this.dashboards$.value));
    } catch (e) {
      console.warn("Failed to persist dashboards", e);
    }
  }

  private genId(): string {
    return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }
}

/**
 * Earlier versions of this code used 12-col grid-unit layouts
 * ({x, y, w, h}) instead of pixel layouts ({x, y, width, height}). When we
 * load a dashboard saved by the old code, convert the layout in-place so
 * the editor's pixel-based code works unchanged.
 */
function migrateLegacyLayout(dashboard: Dashboard): Dashboard {
  const COLS = 12;
  const PX_PER_COL = 100; // approximation — fine for one-off migration
  const PX_PER_ROW = 80;
  const GUTTER = 12;
  const widgets = dashboard.widgets.map(w => {
    const layout: any = w.layout ?? {};
    if ("width" in layout && "height" in layout) {
      return w; // already in pixel format
    }
    if ("w" in layout && "h" in layout) {
      const gridX: number = layout.x ?? 0;
      const gridY: number = layout.y ?? 0;
      const gridW: number = layout.w ?? 4;
      const gridH: number = layout.h ?? 3;
      return {
        ...w,
        layout: {
          x: gridX * (PX_PER_COL + GUTTER) + 16,
          y: gridY * (PX_PER_ROW + GUTTER) + 16,
          width: gridW * (PX_PER_COL + GUTTER) - GUTTER,
          height: gridH * PX_PER_ROW + (gridH - 1) * GUTTER,
        },
      };
    }
    return {
      ...w,
      layout: { x: 16, y: 16, width: 320, height: 240 },
    };
  });
  return { ...dashboard, widgets };
}
