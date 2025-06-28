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
import { SearchResult, SearchResultBatch } from "../../type/search-result";
import { AppSettings } from "../../../common/app-setting";
import { SearchFilterParameters, toQueryStrings } from "../../type/search-filter-parameters";
import { SortMethod } from "../../type/sort-method";
import { DashboardEntry, UserInfo } from "../../type/dashboard-entry";
import { CountResponse, EntityType, HubService } from "../../../hub/service/hub.service";
import { map, switchMap } from "rxjs/operators";

const DASHBOARD_SEARCH_URL = "dashboard/search";
const DASHBOARD_PUBLIC_SEARCH_URL = "dashboard/publicSearch";
const DASHBOARD_USER_INFO_URL = "dashboard/resultsOwnersInfo";

@Injectable({
  providedIn: "root",
})
export class SearchService {
  constructor(
    private http: HttpClient,
    private hubService: HubService
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

        const userIds = new Set<number>();
        filteredResults.forEach(i => {
          if (i.project) userIds.add(i.project.ownerId);
          else if (i.workflow) userIds.add(i.workflow.ownerId);
          else if (i.dataset?.dataset?.ownerUid !== undefined) userIds.add(i.dataset.dataset.ownerUid);
        });

        const userInfo$ =
          userIds.size > 0 ? this.getUserInfo(Array.from(userIds)) : of({} as { [key: number]: UserInfo });

        const entityTypes: EntityType[] = [];
        const entityIds: number[] = [];
        filteredResults.forEach(i => {
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
          entityTypes.length > 0 ? this.hubService.getCounts(entityTypes, entityIds) : of([] as CountResponse[]);

        return forkJoin([userInfo$, counts$]).pipe(
          map(([userIdToInfoMap, responses]) => {
            const countsMap: { [key: string]: { [action: string]: number } } = {};
            responses.forEach(r => {
              countsMap[`${r.entityType}:${r.entityId}`] = r.counts;
            });

            const entries: DashboardEntry[] = filteredResults.map(i => {
              let entry: DashboardEntry;
              if (i.workflow) {
                entry = new DashboardEntry(i.workflow);
                const ui = userIdToInfoMap[i.workflow.ownerId];
                if (ui) {
                  entry.setOwnerName(ui.userName);
                  entry.setOwnerGoogleAvatar(ui.googleAvatar ?? "");
                }
              } else if (i.project) {
                entry = new DashboardEntry(i.project);
                const ui = userIdToInfoMap[i.project.ownerId];
                if (ui) {
                  entry.setOwnerName(ui.userName);
                  entry.setOwnerGoogleAvatar(ui.googleAvatar ?? "");
                }
              } else {
                entry = new DashboardEntry(i.dataset!);
                const ownerUid = i.dataset!.dataset!.ownerUid!;
                const ui = userIdToInfoMap[ownerUid];
                if (ui) {
                  entry.setOwnerName(ui.userName);
                  entry.setOwnerGoogleAvatar(ui.googleAvatar ?? "");
                }
              }
              return entry;
            });

            entries.forEach(entry => {
              if (entry.id == null || entry.type == null) return;
              const key = `${entry.type}:${entry.id}`;
              const c = countsMap[key] || {};
              entry.setCount(c.view ?? 0, c.clone ?? 0, c.like ?? 0);
            });

            return { entries, more: results.more, hasMismatch };
          })
        );
      })
    );
  }
}
