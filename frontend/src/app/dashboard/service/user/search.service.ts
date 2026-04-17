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
   * @param type - The type of resource to search for ("workflow", "project", "dataset", "file", or null (all resource type)).
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
    type: "workflow" | "project" | "file" | "dataset" | null,
    orderBy: SortMethod,
    isLogin: boolean,
    includePublic: boolean = false
  ): Observable<SearchResult> {
    const url = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DASHBOARD_SEARCH_URL}`
      : `${AppSettings.getApiEndpoint()}/${DASHBOARD_PUBLIC_SEARCH_URL}`;

    const finalIncludePublic = isLogin ? includePublic : true;

    return this.http.get<SearchResult>(
      `${url}?${toQueryStrings(keywords, params, start, count, type, orderBy)}&includePublic=${finalIncludePublic}`
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
   * - Fetches owner information (name, Google avatar) in batch for workflows, projects, and datasets.
   * - Aggregates view/clone/like counts via the batch counts API.
   * - Constructs `DashboardEntry` instances and attaches owner info and counts.
   *
   * @param keywords      Array of search keywords.
   * @param params        Additional search filter parameters.
   * @param start         The starting index for paginated results.
   * @param count         The number of results to retrieve.
   * @param type          The type of resource to search for ("workflow", "project", "dataset", "file", or null (all resource type)).
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
    type: "workflow" | "project" | "dataset" | "file" | null,
    orderBy: SortMethod,
    isLogin: boolean,
    includePublic: boolean
  ): Observable<SearchResultBatch> {
    return this.search(keywords, params, start, count, type, orderBy, isLogin, includePublic).pipe(
      switchMap(results => {
        const hasMismatch = type === "dataset" ? results.hasMismatch ?? false : undefined;
        const filteredResults =
          type === "dataset" ? results.results.filter(i => i !== null && i.dataset != null) : results.results;

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
      if (i.project) userIds.add(i.project.ownerId);
      else if (i.workflow) userIds.add(i.workflow.ownerId);
      else if (i.dataset?.dataset?.ownerUid != null) userIds.add(i.dataset.dataset.ownerUid);
    });
    const userInfo$ = userIds.size ? this.getUserInfo(Array.from(userIds)) : of({} as Record<number, UserInfo>);

    const entityTypes: EntityType[] = [];
    const entityIds: number[] = [];
    items.forEach(i => {
      if (i.workflow?.workflow?.wid != null) {
        entityTypes.push(EntityType.Workflow);
        entityIds.push(i.workflow.workflow.wid);
      } else if (i.project) {
        entityTypes.push(EntityType.Project);
        entityIds.push(i.project.pid);
      } else if (i.dataset?.dataset?.did != null) {
        entityTypes.push(EntityType.Dataset);
        entityIds.push(i.dataset.dataset.did);
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
          const entry = i.workflow
            ? new DashboardEntry(i.workflow)
            : i.project
              ? new DashboardEntry(i.project)
              : new DashboardEntry(i.dataset!);

          const key = `${entry.type}:${entry.id}`;
          const ownerId = i.workflow
            ? i.workflow.ownerId
            : i.project
              ? i.project.ownerId
              : i.dataset!.dataset!.ownerUid!;
          const ui = (userMap as any)[ownerId];
          if (ui) {
            entry.setOwnerName(ui.userName);
            entry.setOwnerGoogleAvatar(ui.googleAvatar ?? "");
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
