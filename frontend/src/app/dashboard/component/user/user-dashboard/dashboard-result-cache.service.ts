/**
 * Sits next to WorkflowResultService and intercepts the same WebSocket
 * stream that drives the Result Panel. Every time an operator emits a
 * result, we snapshot the first 100 rows to localStorage so the Dashboard
 * Visualizer can read real data without a REST endpoint.
 *
 * Storage shape (per the dashboard contract):
 *   key:   texera.results.{wid}.{operatorId}
 *   value: { columns: string[], rows: any[][], timestamp: string }
 *
 * The dashboard's workflow-data.service reads these entries back.
 */

import { Injectable } from "@angular/core";
import { NavigationEnd, Router } from "@angular/router";
import { filter } from "rxjs/operators";
import { WorkflowResultService } from "../../../../workspace/service/workflow-result/workflow-result.service";
import {
  isWebDataUpdate,
  isWebPaginationUpdate,
  WebResultUpdate,
} from "../../../../workspace/types/execute-workflow.interface";

const MAX_ROWS = 100;
const STORAGE_PREFIX = "texera.results";
const WORKSPACE_URL_REGEX = /\/dashboard\/user\/workflow\/(\d+)/;

@Injectable({ providedIn: "root" })
export class DashboardResultCacheService {
  /** wid of whatever workflow the user is currently viewing in the workspace. */
  private currentWid: number | null = null;
  /** In-flight pagination fetches keyed by `${wid}|${opId}` — debounce. */
  private inflight = new Set<string>();

  constructor(
    private workflowResults: WorkflowResultService,
    private router: Router
  ) {
    this.currentWid = parseWidFromUrl(this.router.url);
    this.router.events
      .pipe(filter(e => e instanceof NavigationEnd))
      .subscribe(event => {
        this.currentWid = parseWidFromUrl((event as NavigationEnd).url);
      });

    this.workflowResults.getResultUpdateStream().subscribe(updates => {
      const wid = this.currentWid;
      if (wid === null) return;
      for (const [opId, update] of Object.entries(updates)) {
        if (!update) {
          this.removeOperatorCache(wid, opId);
          continue;
        }
        this.cacheUpdate(wid, opId, update);
      }
    });
  }

  private cacheUpdate(wid: number, opId: string, update: WebResultUpdate): void {
    if (isWebDataUpdate(update)) {
      // Snapshot mode is the only mode that carries rows. SetDeltaMode is
      // ignored by the frontend already.
      if (update.mode.type === "SetSnapshotMode") {
        const rows = update.table.slice(0, MAX_ROWS) as ReadonlyArray<Record<string, unknown>>;
        const columns = extractColumns(rows);
        this.write(wid, opId, columns, rowsToCells(rows, columns));
      }
      return;
    }

    if (isWebPaginationUpdate(update)) {
      // Paginated results — fetch page 1 to get the actual rows. The service
      // either returns from its own cache or hits the WebSocket; both paths
      // resolve through the same Observable.
      const key = `${wid}|${opId}`;
      if (this.inflight.has(key)) return;
      const service = this.workflowResults.getPaginatedResultService(opId);
      if (!service) return;
      this.inflight.add(key);
      service.selectPage(1, MAX_ROWS).subscribe({
        next: event => {
          const rows = event.table as ReadonlyArray<Record<string, unknown>>;
          const schemaCols = (event.schema ?? []).map(s => s.attributeName);
          const columns = schemaCols.length > 0 ? schemaCols : extractColumns(rows);
          this.write(wid, opId, columns, rowsToCells(rows, columns));
          this.inflight.delete(key);
        },
        error: () => this.inflight.delete(key),
        complete: () => this.inflight.delete(key),
      });
    }
  }

  private write(wid: number, opId: string, columns: string[], rows: (string | number | null)[][]): void {
    if (columns.length === 0 && rows.length === 0) {
      return;
    }
    const payload = {
      columns,
      rows,
      timestamp: new Date().toISOString(),
    };
    try {
      localStorage.setItem(this.key(wid, opId), JSON.stringify(payload));
    } catch (e) {
      // Likely QuotaExceededError — best-effort: drop oldest entries for this
      // workflow and retry once.
      try {
        this.evictOldestForWid(wid);
        localStorage.setItem(this.key(wid, opId), JSON.stringify(payload));
      } catch {
        // Give up silently — the dashboard will just lack this entry.
      }
    }
  }

  private removeOperatorCache(wid: number, opId: string): void {
    try {
      localStorage.removeItem(this.key(wid, opId));
    } catch {
      // ignore
    }
  }

  private evictOldestForWid(wid: number): void {
    const prefix = `${STORAGE_PREFIX}.${wid}.`;
    const entries: Array<{ key: string; ts: number }> = [];
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (!k || !k.startsWith(prefix)) continue;
      try {
        const v = JSON.parse(localStorage.getItem(k) ?? "");
        const ts = v?.timestamp ? Date.parse(v.timestamp) : 0;
        entries.push({ key: k, ts });
      } catch {
        entries.push({ key: k, ts: 0 });
      }
    }
    entries.sort((a, b) => a.ts - b.ts);
    if (entries.length > 0) {
      localStorage.removeItem(entries[0].key);
    }
  }

  private key(wid: number, opId: string): string {
    return `${STORAGE_PREFIX}.${wid}.${opId}`;
  }
}

function parseWidFromUrl(url: string): number | null {
  const m = url.match(WORKSPACE_URL_REGEX);
  return m ? Number(m[1]) : null;
}

function extractColumns(rows: ReadonlyArray<Record<string, unknown>>): string[] {
  if (rows.length === 0) return [];
  const first = rows[0];
  if (first && typeof first === "object") {
    return Object.keys(first);
  }
  return [];
}

function rowsToCells(
  rows: ReadonlyArray<Record<string, unknown>>,
  columns: string[]
): (string | number | null)[][] {
  return rows.map(r =>
    columns.map(c => {
      const v = (r as any)[c];
      if (v === undefined || v === null) return null;
      if (typeof v === "number" || typeof v === "string") return v;
      // For nested objects / arrays / binary, stringify so widgets can render.
      try {
        return JSON.stringify(v);
      } catch {
        return String(v);
      }
    })
  );
}
