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
import { Observable } from "rxjs";
import { SearchResult } from "../../type/search-result";
import { AppSettings } from "../../../common/app-setting";
import { SearchFilterParameters, toQueryStrings } from "../../type/search-filter-parameters";
import { SortMethod } from "../../type/sort-method";
import { UserInfo } from "../../type/dashboard-entry";

const DASHBOARD_SEARCH_URL = "dashboard/search";
const DASHBOARD_PUBLIC_SEARCH_URL = "dashboard/publicSearch";
const DASHBOARD_USER_INFO_URL = "dashboard/resultsOwnersInfo";

@Injectable({
  providedIn: "root",
})
export class SearchService {
  constructor(private http: HttpClient) {}

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
}
