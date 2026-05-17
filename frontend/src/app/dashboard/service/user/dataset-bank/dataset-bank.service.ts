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
import { BehaviorSubject, Observable, combineLatest, forkJoin, of, throwError } from "rxjs";
import { catchError, debounceTime, distinctUntilChanged, map, switchMap, timeout } from "rxjs/operators";
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
  private readonly _pubmedResults$ = new BehaviorSubject<BankDataset[]>([]);
  private readonly _searchQuery$ = new BehaviorSubject<string>("");
  private readonly _category$ = new BehaviorSubject<BankCategory | "all">("all");
  private readonly _isLoading$ = new BehaviorSubject<boolean>(false);

  readonly datasets$: Observable<BankDataset[]> = this._datasets$.asObservable();
  readonly searchQuery$: Observable<string> = this._searchQuery$.asObservable();
  readonly category$: Observable<BankCategory | "all"> = this._category$.asObservable();
  readonly isLoading$: Observable<boolean> = this._isLoading$.asObservable();

  /**
   * Combined view: filtered + searched datasets (including live PubMed results).
   * The component subscribes to this. PubMed papers come from `_pubmedResults$`,
   * which the constructor keeps populated by debouncing the search query and
   * hitting NCBI E-utilities (eSearch + eFetch).
   */
  readonly filteredDatasets$: Observable<BankDataset[]> = combineLatest([
    this._datasets$,
    this._pubmedResults$,
    this._searchQuery$,
    this._category$,
  ]).pipe(
    map(([datasets, pubmed, query, category]) => {
      const q = query.trim().toLowerCase();
      // PubMed results are already a query-specific list; only include them
      // when the user is actively searching — otherwise the seed list would be
      // polluted with results from an old query.
      const combined = q ? [...pubmed, ...datasets] : datasets;
      return combined.filter(d => {
        if (category !== "all" && !d.categories.includes(category)) return false;
        if (!q) return true;
        // PubMed papers are always relevant to the current query (we fetched
        // them with it), so accept them unconditionally.
        if (d.source === "pubmed") return true;
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

    // Live PubMed search on debounced query changes. Failures (network, CORS,
    // empty query) clear results silently so the seed list stays clean.
    this._searchQuery$
      .pipe(
        debounceTime(400),
        distinctUntilChanged(),
        switchMap(q => {
          const trimmed = q.trim();
          if (trimmed.length < 3) return of([] as BankDataset[]);
          return this.searchPubmed(trimmed);
        }),
        catchError(() => of([] as BankDataset[]))
      )
      .subscribe(results => this._pubmedResults$.next(results));
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
   * Calls the agent-service `POST /api/databank/import-from-url` proxy,
   * which fetches the file server-side (no browser CORS) and runs the full
   * `createDataset → multipart-upload → createDatasetVersion` pipeline.
   *
   * The body discriminator (sourceType) tells the proxy where the data comes from:
   *   - "url"    → fetch d.downloadUrl || d.url verbatim (UCI/Kaggle/dkNET)
   *   - "pubmed" → fetch NCBI eFetch for d.externalId, build a 1-row paper CSV
   *   - "who"    → fetch GHO indicator d.externalId, build a (country,year,value) CSV
   */
  importToTexera(d: BankDataset): Observable<{ did: number; datasetName: string }> {
    let body: Record<string, unknown>;
    if (d.source === "pubmed") {
      if (!d.externalId) {
        return throwError(() => new Error("PubMed paper is missing its PMID."));
      }
      body = {
        sourceType: "pubmed",
        pubmedId: d.externalId,
        name: d.name,
        description: d.description ?? "",
      };
    } else if (d.source === "who") {
      if (!d.externalId) {
        return throwError(() => new Error("WHO indicator entry is missing its indicator code."));
      }
      body = {
        sourceType: "who",
        whoIndicator: d.externalId,
        name: d.name,
        description: d.description ?? "",
      };
    } else {
      const fetchUrl = d.downloadUrl || d.url;
      if (!fetchUrl) {
        return throwError(() => new Error("No source URL is available for this dataset."));
      }
      body = {
        sourceType: "url",
        url: fetchUrl,
        name: d.name,
        description: d.description ?? "",
      };
    }
    return this.http
      .post<{ did: number; datasetName: string; fileName: string; fileSize: number }>(
        "/api/databank/import-from-url",
        body
      )
      .pipe(
        map(resp => ({ did: resp.did, datasetName: resp.datasetName })),
        catchError(err => {
          const message = err?.error?.error || err?.message || "Import failed.";
          return throwError(() => new Error(message));
        })
      );
  }

  /**
   * Live PubMed search: NCBI eSearch returns up to 20 PMIDs for the query,
   * eFetch returns paper details as XML which we parse client-side via
   * DOMParser. Each paper becomes a BankDataset card; clicking Import sends
   * the PMID to the backend proxy, which re-fetches and builds the CSV.
   *
   * NCBI E-utilities send `Access-Control-Allow-Origin: *`, so this fetch works
   * directly from the browser. Failures (rate limiting, network, parse error)
   * return [] so the seed list stays usable.
   */
  searchPubmed(query: string, maxResults: number = 10): Observable<BankDataset[]> {
    const esearchUrl =
      `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi` +
      `?db=pubmed&retmode=json&retmax=${maxResults}&term=${encodeURIComponent(query)}`;
    return this.http.get<unknown>(esearchUrl).pipe(
      timeout(FETCH_TIMEOUT_MS),
      switchMap(body => {
        const r = (body ?? {}) as Record<string, any>;
        const ids: string[] = Array.isArray(r?.esearchresult?.idlist) ? r.esearchresult.idlist : [];
        if (ids.length === 0) return of([] as BankDataset[]);
        const efetchUrl =
          `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi` +
          `?db=pubmed&retmode=xml&id=${ids.join(",")}`;
        return this.http
          .get(efetchUrl, { responseType: "text" })
          .pipe(map(xml => this.parsePubmedXml(xml, ids)));
      }),
      catchError(() => of([] as BankDataset[]))
    );
  }

  private parsePubmedXml(xml: string, requestedIds: string[]): BankDataset[] {
    let doc: Document;
    try {
      doc = new DOMParser().parseFromString(xml, "text/xml");
    } catch {
      return [];
    }
    if (doc.getElementsByTagName("parsererror").length > 0) return [];
    const articles = Array.from(doc.getElementsByTagName("PubmedArticle"));
    const out: BankDataset[] = [];
    for (let i = 0; i < articles.length; i++) {
      const a = articles[i];
      const pmid =
        a.getElementsByTagName("PMID")[0]?.textContent?.trim() ?? requestedIds[i] ?? String(i);
      const title = a.getElementsByTagName("ArticleTitle")[0]?.textContent?.trim() ?? "(untitled)";
      const abstractParts = Array.from(a.getElementsByTagName("AbstractText"))
        .map(n => n.textContent?.trim() ?? "")
        .filter(Boolean);
      const abstract = abstractParts.join(" ");
      const journal = a.getElementsByTagName("Journal")[0]?.getElementsByTagName("Title")[0]?.textContent?.trim() ?? "";
      const year =
        a.getElementsByTagName("PubDate")[0]?.getElementsByTagName("Year")[0]?.textContent?.trim() ?? "";
      const authorNodes = Array.from(a.getElementsByTagName("Author")).slice(0, 6);
      const authors = authorNodes
        .map(an => {
          const last = an.getElementsByTagName("LastName")[0]?.textContent?.trim() ?? "";
          const initials = an.getElementsByTagName("Initials")[0]?.textContent?.trim() ?? "";
          return (last + (initials ? " " + initials : "")).trim();
        })
        .filter(Boolean);
      const tags: string[] = [];
      if (year) tags.push(year);
      if (journal) tags.push(journal.length > 30 ? journal.slice(0, 30) + "…" : journal);
      out.push({
        id: `live-pubmed-${pmid}`,
        name: title,
        source: "pubmed",
        externalId: pmid,
        description: abstract || (authors.length > 0 ? `Authors: ${authors.join(", ")}` : "No abstract available."),
        url: `https://pubmed.ncbi.nlm.nih.gov/${pmid}/`,
        format: "csv",
        rows: 1,
        columns: 5,
        sizeLabel: "~2 KB",
        tags,
        categories: ["biomedical", "nlp"],
      });
    }
    return out;
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
