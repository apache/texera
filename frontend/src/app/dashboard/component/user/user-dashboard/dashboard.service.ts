/**
 * Storage and CRUD for user-built dashboards. localStorage-backed for the
 * hackathon — no backend required.
 */

import { Injectable } from "@angular/core";
import { BehaviorSubject, Observable } from "rxjs";
import { Dashboard, DashboardWidget, WidgetConfig, WidgetLayout, WidgetType } from "./dashboard.types";
import { buildSeedDashboard } from "./dashboard.seed";

const STORAGE_KEY = "texera.dashboards.v1";

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

  addWidget(dashboardId: string, widget: WidgetConfig): DashboardWidget | undefined {
    const dash = this.get(dashboardId);
    if (!dash) {
      return undefined;
    }
    const layout = this.nextLayout(dash.widgets, widget.type);
    const dw: DashboardWidget = {
      id: this.genId(),
      layout,
      widget,
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

  private nextLayout(existing: DashboardWidget[], type: WidgetType): WidgetLayout {
    const sizes: Record<WidgetType, { w: number; h: number }> = {
      metric: { w: 3, h: 2 },
      bar: { w: 6, h: 4 },
      donut: { w: 4, h: 4 },
      hbar: { w: 6, h: 4 },
      text: { w: 4, h: 3 },
      table: { w: 8, h: 4 },
    };
    const size = sizes[type];
    let maxBottom = 0;
    for (const w of existing) {
      maxBottom = Math.max(maxBottom, w.layout.y + w.layout.h);
    }
    return { x: 0, y: maxBottom, w: size.w, h: size.h };
  }

  private load(): void {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed: Dashboard[] = JSON.parse(raw);
        if (Array.isArray(parsed)) {
          this.dashboards$.next(parsed);
          return;
        }
      }
    } catch (e) {
      console.warn("Failed to load dashboards from localStorage", e);
    }
    // First visit — seed the demo dashboard
    const seed = buildSeedDashboard(this.genId.bind(this));
    this.dashboards$.next([seed]);
    this.persist();
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
