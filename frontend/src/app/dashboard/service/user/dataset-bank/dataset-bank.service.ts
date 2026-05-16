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
import { BehaviorSubject, Observable, combineLatest, forkJoin, from, of, throwError } from "rxjs";
import { catchError, filter, map, switchMap, take, timeout } from "rxjs/operators";
import { SEED_DATASETS } from "./dataset-bank.seed";
import { BankCategory, BankDataset, BankDatasetSource } from "./dataset-bank.types";
import { DatasetService } from "../dataset/dataset.service";
import { UserService } from "../../../../common/service/user/user.service";
import { Dataset } from "../../../../common/type/dataset";

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

  constructor(
    private http: HttpClient,
    private datasetService: DatasetService,
    private userService: UserService
  ) {
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

  /**
   * Fetches the file from the bank dataset's downloadUrl (falling back to its
   * source URL) and imports it as a new Texera dataset under the current user.
   *
   * Steps (mirrors how UserDataset's dataset-detail page does it):
   *   1. createDataset()                         → DashboardDataset with did
   *   2. multipartUpload() until status=finished → stages the file
   *   3. createDatasetVersion(did, "v1")         → publishes
   *
   * The fetch in step 0 is browser-side, so it depends on the source allowing
   * CORS. Most well-known catalogs don't, in which case this fails fast with
   * a clear error and the user can use the Download button instead.
   */
  importToTexera(d: BankDataset): Observable<{ did: number; datasetName: string }> {
    const fetchUrl = d.downloadUrl || d.url;
    if (!fetchUrl) {
      return throwError(() => new Error("No source URL is available for this dataset."));
    }
    const user = this.userService.getCurrentUser();
    if (!user?.email) {
      return throwError(() => new Error("You must be signed in to import datasets."));
    }
    const ownerEmail = user.email;
    const datasetName = this.sanitizeDatasetName(d.name);
    const filename = this.guessFilename(d, fetchUrl);

    const fetchFile$ = from(
      fetch(fetchUrl, { method: "GET" }).then(async resp => {
        if (!resp.ok) throw new Error(`Source responded ${resp.status} ${resp.statusText}`);
        const blob = await resp.blob();
        return new File([blob], filename, { type: blob.type || "application/octet-stream" });
      })
    ).pipe(
      catchError(err => {
        // Browser fetch failures often manifest as TypeError with no body —
        // surface a clear hint for the common CORS case.
        const msg =
          err?.name === "TypeError"
            ? "Couldn't fetch the file directly (likely CORS). Use the Download button to grab it manually."
            : `Couldn't fetch the file: ${err?.message ?? String(err)}`;
        return throwError(() => new Error(msg));
      })
    );

    const ds: Dataset = {
      name: datasetName,
      description: d.description ?? "",
      isPublic: false,
      isDownloadable: true,
      did: undefined,
      ownerUid: undefined,
      storagePath: undefined,
      creationTime: undefined,
      coverImage: undefined,
    };

    return fetchFile$.pipe(
      switchMap(file =>
        this.datasetService.createDataset(ds).pipe(
          map(created => ({ created, file }))
        )
      ),
      switchMap(({ created, file }) => {
        const did = created.dataset?.did;
        if (did === undefined || did === null) {
          return throwError(() => new Error("Dataset was created but the server did not return an ID."));
        }
        const partSize = 5 * 1024 * 1024; // 5 MB
        return this.datasetService
          .multipartUpload(ownerEmail, datasetName, file.name, file, partSize, 4, false)
          .pipe(
            filter(progress => progress.status === "finished"),
            take(1),
            switchMap(() => this.datasetService.createDatasetVersion(did, "v1")),
            map(() => ({ did, datasetName }))
          );
      })
    );
  }

  private sanitizeDatasetName(name: string): string {
    return name
      .trim()
      .replace(/[^a-zA-Z0-9._-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 64) || "imported-dataset";
  }

  private guessFilename(d: BankDataset, fetchUrl: string): string {
    try {
      const path = new URL(fetchUrl).pathname;
      const last = path.split("/").filter(Boolean).pop();
      if (last && /\.[a-zA-Z0-9]+$/.test(last)) return last;
    } catch {
      // fall through
    }
    const ext = (d.format ?? "csv").split("/")[0].trim().toLowerCase();
    return `${this.sanitizeDatasetName(d.name)}.${ext || "csv"}`;
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
