/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { BehaviorSubject, Observable, combineLatest, forkJoin, of } from "rxjs";
import { catchError, map, timeout } from "rxjs/operators";
import { SEED_DATASETS } from "./dataset-bank.seed";
import { BankCategory, BankDataset, BankDatasetSource } from "./dataset-bank.types";

interface CachedPayload {
  fetchedAt: number;
  datasets: BankDataset[];
}

const CACHE_KEY = "dataset-bank.live-cache";
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour
const FETCH_TIMEOUT_MS = 6000;

@Injectable({ providedIn: "root" })
export class DatasetBankService {
  private readonly _datasets$ = new BehaviorSubject<BankDataset[]>(SEED_DATASETS);
  private readonly _searchQuery$ = new BehaviorSubject<string>("");
  private readonly _category$ = new BehaviorSubject<BankCategory | "all">("all");
  private readonly _isLoading$ = new BehaviorSubject<boolean>(false);

  readonly datasets$: Observable<BankDataset[]> = this._datasets$.asObservable();
  readonly searchQuery$: Observable<string> = this._searchQuery$.asObservable();
  readonly category$: Observable<BankCategory | "all"> = this._category$.asObservable();
  readonly isLoading$: Observable<boolean> = this._isLoading$.asObservable();

  /**
   * Combined view: filtered + searched datasets. The component subscribes to this.
   */
  readonly filteredDatasets$: Observable<BankDataset[]> = combineLatest([
    this._datasets$,
    this._searchQuery$,
    this._category$,
  ]).pipe(
    map(([datasets, query, category]) => {
      const q = query.trim().toLowerCase();
      return datasets.filter(d => {
        if (category !== "all" && !d.categories.includes(category)) return false;
        if (!q) return true;
        const haystack = (
          d.name +
          " " +
          d.description +
          " " +
          d.tags.join(" ") +
          " " +
          d.source
        ).toLowerCase();
        return haystack.includes(q);
      });
    })
  );

  constructor(private http: HttpClient) {
    // Hydrate from cache if recent — purely a UX optimization; the seed list is
    // always shown immediately so the page is never blank.
    const cached = this.readCache();
    if (cached) {
      this._datasets$.next(this.mergeWithSeed(cached.datasets));
    }
  }

  setSearchQuery(q: string): void {
    this._searchQuery$.next(q);
  }

  setCategory(c: BankCategory | "all"): void {
    this._category$.next(c);
  }

  /**
   * Trigger a live refresh from dkNET + UCI. Kaggle is omitted from the browser
   * fetch because it requires basic auth + CORS that public catalogs typically
   * don't allow. The seed list always remains; live results merge on top.
   *
   * Failures (CORS, timeout, non-JSON, empty result) leave the seed intact and
   * are surfaced via isLoading$ returning to false without an error throw.
   */
  refreshFromApis(query: string = ""): void {
    this._isLoading$.next(true);
    forkJoin({
      dknet: this.fetchDknet(query),
      uci: this.fetchUci(query),
    })
      .pipe(
        map(({ dknet, uci }) => [...dknet, ...uci]),
        catchError(() => of([] as BankDataset[]))
      )
      .subscribe(live => {
        if (live.length > 0) {
          const merged = this.mergeWithSeed(live);
          this._datasets$.next(merged);
          this.writeCache(merged);
        }
        this._isLoading$.next(false);
      });
  }

  // ---------- internals ----------

  private mergeWithSeed(live: BankDataset[]): BankDataset[] {
    const byId = new Map<string, BankDataset>();
    for (const d of SEED_DATASETS) byId.set(d.id, d);
    for (const d of live) byId.set(d.id, d);
    return Array.from(byId.values());
  }

  private readCache(): CachedPayload | null {
    try {
      const raw = localStorage.getItem(CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as CachedPayload;
      if (!parsed.fetchedAt || Date.now() - parsed.fetchedAt > CACHE_TTL_MS) return null;
      if (!Array.isArray(parsed.datasets)) return null;
      return parsed;
    } catch {
      return null;
    }
  }

  private writeCache(datasets: BankDataset[]): void {
    try {
      const payload: CachedPayload = { fetchedAt: Date.now(), datasets };
      localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
    } catch {
      // Swallow quota/JSON errors — caching is best-effort.
    }
  }

  private fetchDknet(query: string): Observable<BankDataset[]> {
    const url = `https://dknet.org/api/search?query=${encodeURIComponent(query)}&type=dataset`;
    return this.http.get<unknown>(url).pipe(
      timeout(FETCH_TIMEOUT_MS),
      map(body => this.parseDknet(body)),
      catchError(() => of([] as BankDataset[]))
    );
  }

  private fetchUci(query: string): Observable<BankDataset[]> {
    const url = `https://archive.ics.uci.edu/api/datasets?search=${encodeURIComponent(query)}`;
    return this.http.get<unknown>(url).pipe(
      timeout(FETCH_TIMEOUT_MS),
      map(body => this.parseUci(body)),
      catchError(() => of([] as BankDataset[]))
    );
  }

  private pickArray(body: unknown, keys: string[]): unknown[] {
    if (Array.isArray(body)) return body;
    if (body && typeof body === "object") {
      for (const k of keys) {
        const v = (body as Record<string, unknown>)[k];
        if (Array.isArray(v)) return v;
      }
    }
    return [];
  }

  private parseDknet(body: unknown): BankDataset[] {
    const items = this.pickArray(body, ["results", "items", "data", "hits"]).slice(0, 20);
    return items.map((raw, i): BankDataset => {
      const r = (raw ?? {}) as Record<string, unknown>;
      const name =
        (typeof r.name === "string" && r.name) ||
        (typeof r.title === "string" && r.title) ||
        `dkNET result ${i + 1}`;
      const description =
        (typeof r.description === "string" && r.description) ||
        (typeof r.summary === "string" && r.summary) ||
        "";
      const url =
        (typeof r.url === "string" && r.url) ||
        (typeof r.link === "string" && r.link) ||
        "https://dknet.org/";
      return {
        id: `live-dknet-${r.id ?? r.identifier ?? i}`,
        name: String(name),
        source: "dknet" as BankDatasetSource,
        description: String(description),
        url: String(url),
        tags: [],
        categories: ["biomedical"],
      };
    });
  }

  private parseUci(body: unknown): BankDataset[] {
    const items = this.pickArray(body, ["datasets", "data", "results"]).slice(0, 20);
    return items.map((raw, i): BankDataset => {
      const r = (raw ?? {}) as Record<string, unknown>;
      const slug =
        (typeof r.slug === "string" && r.slug) ||
        (r.id !== undefined ? String(r.id) : `${i}`);
      const url =
        (typeof r.url === "string" && r.url) ||
        `https://archive.ics.uci.edu/dataset/${slug}`;
      return {
        id: `live-uci-${slug}`,
        name:
          (typeof r.name === "string" && r.name) ||
          (typeof r.title === "string" && r.title) ||
          `UCI dataset ${slug}`,
        source: "uci",
        description:
          (typeof r.abstract === "string" && r.abstract) ||
          (typeof r.description === "string" && r.description) ||
          "",
        url,
        format: typeof r.format === "string" ? r.format : "csv",
        tags: [],
        categories: ["tabular"],
      };
    });
  }
}
