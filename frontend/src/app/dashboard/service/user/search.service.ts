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

import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";
import { forkJoin, Observable, of } from "rxjs";
import { SearchResult, SearchResultBatch, SearchResultItem } from "../../type/search-result";
import { AppSettings } from "../../../common/app-setting";
import { SearchFilterParameters, toQueryStrings } from "../../type/search-filter-parameters";
import { SortMethod } from "../../type/sort-method";
import { DashboardEntry, UserInfo } from "../../type/dashboard-entry";
import {
  AccessResponse,
  ActionType,
  CountResponse,
  EntityType,
  HubService,
  LikedStatus,
} from "../../../hub/service/hub.service";
import { map, switchMap } from "rxjs/operators";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";

const DASHBOARD_SEARCH_URL = "dashboard/search";
const DASHBOARD_PUBLIC_SEARCH_URL = "dashboard/publicSearch";
const DASHBOARD_USER_INFO_URL = "dashboard/resultsOwnersInfo";
export type EnrichActivity = "counts" | "liked" | "access" | "size";

@Injectable({
  providedIn: "root",
})
export class SearchService {
  constructor(
    private http: HttpClient,
    private hubService: HubService,
    private workflowPersistService: WorkflowPersistService
  ) {}

  /**
   * Retrieves a workflow or other resource from the backend database given the specified search parameters.
   * The user in the session must have access to the workflow or resource unless the search is public.
   *
   * @param keywords - Array of search keywords.
   * @param params - Additional search filter parameters.
   * @param start - The starting index for paginated results.
   * @param count - The number of results to retrieve.
   * @param type - The type of resource to search for ("workflow", "dataset", "file", "model", or null (all resource type)).
   * @param orderBy - Specifies the sorting method.
   * @param isLogin - Indicates if the user is logged in.
   *    - `isLogin = true`: Use the authenticated search endpoint, retrieving both user-accessible and public resources based on `includePublic`.
   *    - `isLogin = false`: Use the public search endpoint, limited to public resources only.
   * @param includePublic - Specifies whether to include public resources in the search results.
   *    - If `isLogin` is `true`, `includePublic` controls whether public resources are included alongside user-accessible ones.
   *    - If `isLogin` is `false`, this parameter defaults to `true` to ensure only public resources are fetched.
   */
  public search(
    keywords: string[],
    params: SearchFilterParameters,
    start: number,
    count: number,
    type: "workflow" | "file" | "dataset" | "model" | null,
    orderBy: SortMethod,
    isLogin: boolean,
    includePublic: boolean = false
  ): Observable<SearchResult> {
    const url = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DASHBOARD_SEARCH_URL}`
      : `${AppSettings.getApiEndpoint()}/${DASHBOARD_PUBLIC_SEARCH_URL}`;

    const finalIncludePublic = isLogin ? includePublic : true;

    return this.http
      .get<SearchResult>(
        `${url}?${toQueryStrings(keywords, params, start, count, type, orderBy)}&includePublic=${finalIncludePublic}`
      )
      .pipe(
        map(result => ({
          ...result,
          // The unified-search response can carry resource types this client does not model:
          // rows from a feature being removed server-side later than here, or a type the backend
          // gains first. Both `convertToName` and `DashboardEntry` throw on an unrecognised
          // payload, and the autocomplete subscribes without an error handler, so a single such
          // row would otherwise kill the subscription for the rest of the session. Dropping them
          // at the funnel every consumer calls keeps a stale row merely invisible.
          //
          // Known trade-off: the server counted these rows against offset/limit, and
          // `SearchResultsComponent.loadMore` starts the next page at `entries.length` and appends
          // without dedup. So on the "All" tab a window holding k dropped rows re-fetches k
          // already-shown rows, and a window that is entirely dropped rows leaves `entries.length`
          // unmoved -- "Load more" re-requests the same window while `more` stays true. Typed tabs
          // and the search bar are unaffected. Not corrected here: that belongs in the
          // `LoadMoreFunction` contract shared by all five callers of `SearchResultsComponent.reset`,
          // and removing the backend half (#7461) ends the only condition that produces such rows.
          results: result.results.filter(
            item => item.workflow != null || item.file != null || item.dataset != null || item.model != null
          ),
        }))
      );
  }

  public getUserInfo(userIds: number[]): Observable<{ [key: number]: UserInfo }> {
    const queryString = userIds.map(id => `userIds=${encodeURIComponent(id)}`).join("&");
    return this.http.get<{ [key: number]: UserInfo }>(
      `${AppSettings.getApiEndpoint()}/${DASHBOARD_USER_INFO_URL}?${queryString}`
    );
  }

  /**
   * Executes a search query and returns an observable stream of enriched dashboard entries.
   *
   * This method:
   * - Dispatches a paginated search request (authenticated or public) via `this.search(...)`.
   * - Filters out null or mismatched datasets when `type === 'dataset'` and sets `hasMismatch`.
   * - Fetches owner information (name, Google avatar) in batch for workflows and datasets.
   * - Aggregates view/clone/like counts via the batch counts API.
   * - Constructs `DashboardEntry` instances and attaches owner info and counts.
   *
   * @param keywords      Array of search keywords.
   * @param params        Additional search filter parameters.
   * @param start         The starting index for paginated results.
   * @param count         The number of results to retrieve.
   * @param type          The type of resource to search for ("workflow", "dataset", "file", "model", or null (all resource type)).
   * @param orderBy       Specifies the sorting method.
   * @param isLogin       Indicates if the user is logged in.
   * @param includePublic Specifies whether to include public resources in the search results.
   *
   * @returns An `Observable<SearchResultBatch>` that emits exactly one value containing:
   *   - `entries`: the array of fully populated `DashboardEntry` objects,
   *   - `more`: whether additional pages are available,
   *   - `hasMismatch` (for datasets): true if any dataset entries were dropped due to mismatch.
   */
  public executeSearch(
    keywords: string[],
    params: SearchFilterParameters,
    start: number,
    count: number,
    type: "workflow" | "dataset" | "file" | "model" | null,
    orderBy: SortMethod,
    isLogin: boolean,
    includePublic: boolean
  ): Observable<SearchResultBatch> {
    return this.search(keywords, params, start, count, type, orderBy, isLogin, includePublic).pipe(
      switchMap(results => {
        // A dataset or model whose repository is gone comes back with no payload; the backend flags
        // that as a mismatch and the row is dropped here.
        const mismatchable = type === "dataset" || type === "model";
        const payloadOf = (i: SearchResultItem) => (type === "dataset" ? i.dataset : i.model);
        const hasMismatch = mismatchable ? results.hasMismatch ?? false : undefined;
        const filteredResults = mismatchable
          ? results.results.filter(i => i !== null && payloadOf(i) != null)
          : results.results;

        return this.extendSearchResultsWithHubActivityInfo(filteredResults, isLogin).pipe(
          map(entries => ({
            entries,
            more: results.more,
            hasMismatch,
          }))
        );
      })
    );
  }

  /**
   * Enriches an array of SearchResultItem into DashboardEntry instances.
   *
   * @param items        The SearchResultItem[] to enrich.
   * @param isLogin      Whether the current user is authenticated.
   * @param activities   Which activities to perform: 'counts', 'liked', 'access'.
   *                     Defaults to all three if omitted or empty.
   * @returns            Observable that emits the fully populated DashboardEntry[].
   */
  public extendSearchResultsWithHubActivityInfo(
    items: SearchResultItem[],
    isLogin: boolean,
    activities: EnrichActivity[] = []
  ): Observable<DashboardEntry[]> {
    const acts = activities.length > 0 ? activities : (["counts", "liked", "access", "size"] as EnrichActivity[]);

    const doCounts = acts.includes("counts");
    const doLiked = acts.includes("liked") && isLogin;
    const doAccess = acts.includes("access");
    const doSize = acts.includes("size");

    const userIds = new Set<number>();
    items.forEach(i => {
      if (i.workflow) userIds.add(i.workflow.ownerId);
      else if (i.dataset?.dataset?.ownerUid != null) userIds.add(i.dataset.dataset.ownerUid);
      else if (i.model?.model?.ownerUid != null) userIds.add(i.model.model.ownerUid);
    });
    const userInfo$ = userIds.size ? this.getUserInfo(Array.from(userIds)) : of({} as Record<number, UserInfo>);

    const entityTypes: EntityType[] = [];
    const entityIds: number[] = [];
    items.forEach(i => {
      if (i.workflow?.workflow?.wid != null) {
        entityTypes.push(EntityType.Workflow);
        entityIds.push(i.workflow.workflow.wid);
      } else if (i.dataset?.dataset?.did != null) {
        entityTypes.push(EntityType.Dataset);
        entityIds.push(i.dataset.dataset.did);
      } else if (i.model?.model?.mid != null) {
        entityTypes.push(EntityType.Model);
        entityIds.push(i.model.model.mid);
      }
    });

    const counts$ =
      doCounts && entityTypes.length > 0
        ? this.hubService.getCounts(entityTypes, entityIds)
        : of([] as CountResponse[]);
    const liked$ =
      doLiked && entityTypes.length > 0 ? this.hubService.isLiked(entityIds, entityTypes) : of([] as LikedStatus[]);
    const access$ =
      doAccess && entityTypes.length > 0
        ? this.hubService.getUserAccess(entityTypes, entityIds)
        : of([] as AccessResponse[]);

    const workflowIds = items.map(i => i.workflow?.workflow?.wid).filter((wid): wid is number => wid != null);
    const sizes$ =
      doSize && workflowIds.length > 0
        ? this.workflowPersistService.getSizes(workflowIds)
        : of({} as Record<number, number>);

    return forkJoin([userInfo$, counts$, liked$, access$, sizes$]).pipe(
      map(([userMap, counts, liked, access, sizesMap]) => {
        const countsMap: Record<string, Partial<Record<ActionType, number>>> = {};
        counts.forEach(r => (countsMap[`${r.entityType}:${r.entityId}`] = r.counts));

        const likedMap: Record<string, boolean> = {};
        liked.forEach(r => (likedMap[`${r.entityType}:${r.entityId}`] = r.isLiked));

        const accessMap: Record<string, number[]> = {};
        access.forEach(r => (accessMap[`${r.entityType}:${r.entityId}`] = r.userIds));

        return items.map(i => {
          const payload = i.workflow ?? i.dataset ?? i.model;
          if (!payload) {
            throw new Error(`Search result carries no payload for resource type ${i.resourceType}.`);
          }
          const entry = new DashboardEntry(payload);

          const key = `${entry.type}:${entry.id}`;
          const ui = entry.ownerId != null ? (userMap as any)[entry.ownerId] : undefined;
          if (ui) {
            entry.setOwnerName(ui.userName);
            entry.setOwnerAvatar(ui.avatar ?? "");
          }

          if (doCounts) {
            const c = countsMap[key] ?? {};
            entry.setCount(c.view ?? 0, c.clone ?? 0, c.like ?? 0);
          }
          if (doLiked) {
            entry.setIsLiked(likedMap[key] ?? false);
          }
          if (doAccess) {
            entry.setAccessUsers(accessMap[key] ?? []);
          }

          if (doSize && entry.type === EntityType.Workflow && entry.id != null) {
            entry.setSize(sizesMap[entry.id] ?? 0);
          }

          return entry;
        });
      })
    );
  }
}
