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
import { firstValueFrom, Observable } from "rxjs";
import { SearchResult } from "../../type/search-result";
import { AppSettings } from "../../../common/app-setting";
import { SearchFilterParameters, toQueryStrings } from "../../type/search-filter-parameters";
import { SortMethod } from "../../type/sort-method";
import { DashboardEntry, UserInfo } from "../../type/dashboard-entry";
import { CountRequest, CountResponse, HubService } from "../../../hub/service/hub.service";

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
   * Executes a search query and constructs dashboard entries from the results.
   *
   * This function handles:
   * - Dispatching a search request to the backend (authenticated or public),
   * - Mapping search results to `DashboardEntry` objects,
   * - Fetching user info (e.g., owner name/avatar) for workflows, projects, and datasets,
   * - Aggregating counts (view, clone, like) via batch count API,
   * - Filtering results (e.g., remove mismatched datasets) and returning metadata such as `hasMismatch`.
   *
   * @param keywords - Array of search keywords.
   * @param params - Additional search filter parameters.
   * @param start - The starting index for paginated results.
   * @param count - The number of results to retrieve.
   * @param type - The type of resource to search for ("workflow", "project", "dataset", "file", or null (all resource type)).
   * @param orderBy - Specifies the sorting method.
   * @param isLogin - Indicates if the user is logged in.
   * @param includePublic - Specifies whether to include public resources in the search results.
   *
   * @returns A promise that resolves to:
   *  - `entries`: Array of dashboard entries constructed from matched results.
   *  - `more`: Whether there are more results beyond the current page.
   *  - `hasMismatch`: (Only for dataset type) whether there were mismatches between DB and LakeFS.
   */

  public async executeSearch(
    keywords: string[],
    params: SearchFilterParameters,
    start: number,
    count: number,
    type: "workflow" | "project" | "dataset" | "file" | null,
    orderBy: SortMethod,
    isLogin: boolean,
    includePublic: boolean
  ): Promise<{ entries: DashboardEntry[]; more: boolean; hasMismatch?: boolean }> {
    const results = await firstValueFrom(
      this.search(keywords, params, start, count, type, orderBy, isLogin, includePublic)
    );

    const hasMismatch = type === "dataset" ? results.hasMismatch ?? false : undefined;
    const filteredResults =
      type === "dataset" ? results.results.filter(i => i !== null && i.dataset != null) : results.results;

    const userIds = new Set<number>();
    filteredResults.forEach(i => {
      if (i.project) userIds.add(i.project.ownerId);
      else if (i.workflow) userIds.add(i.workflow.ownerId);
      else if (i.dataset?.dataset?.ownerUid !== undefined) userIds.add(i.dataset.dataset.ownerUid);
    });

    let userIdToInfoMap: { [key: number]: UserInfo } = {};
    if (userIds.size > 0) {
      userIdToInfoMap = await firstValueFrom(this.getUserInfo(Array.from(userIds)));
    }

    const entries: DashboardEntry[] = filteredResults.map(i => {
      let entry: DashboardEntry;
      if (i.workflow) {
        entry = new DashboardEntry(i.workflow);
        const userInfo = userIdToInfoMap[i.workflow.ownerId];
        if (userInfo) {
          entry.setOwnerName(userInfo.userName);
          entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
        }
      } else if (i.project) {
        entry = new DashboardEntry(i.project);
        const userInfo = userIdToInfoMap[i.project.ownerId];
        if (userInfo) {
          entry.setOwnerName(userInfo.userName);
          entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
        }
      } else if (i.dataset) {
        entry = new DashboardEntry(i.dataset);
        const ownerUid = i.dataset.dataset?.ownerUid;
        if (ownerUid !== undefined) {
          const userInfo = userIdToInfoMap[ownerUid];
          if (userInfo) {
            entry.setOwnerName(userInfo.userName);
            entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
          }
        }
      } else {
        throw new Error("Unexpected type in search result");
      }
      return entry;
    });

    const countRequests: CountRequest[] = filteredResults.flatMap(i => {
      if (i.workflow?.workflow?.wid != null) {
        return [{ entityId: i.workflow.workflow.wid, entityType: "workflow" }];
      } else if (i.project) {
        return [{ entityId: i.project.pid, entityType: "project" }];
      } else if (i.dataset?.dataset?.did != null) {
        return [{ entityId: i.dataset.dataset.did, entityType: "dataset" }];
      }
      return [];
    });

    let countsMap: { [compositeKey: string]: { [action: string]: number } } = {};
    if (countRequests.length > 0) {
      const responses: CountResponse[] = await firstValueFrom(this.hubService.getBatchCounts(countRequests));
      responses.forEach(r => {
        const key = `${r.entityType}:${r.entityId}`;
        countsMap[key] = r.counts;
      });
    }

    entries.forEach(entry => {
      if (entry.id == null || entry.type == null) return;
      const key = `${entry.type}:${entry.id}`;
      const c = countsMap[key] || {};
      entry.setCount(c.view ?? 0, c.clone ?? 0, c.like ?? 0);
    });

    return { entries, more: results.more, hasMismatch };
  }
}
