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
import { Observable, of } from "rxjs";
import { SearchResult } from "../../type/search-result";
import { SearchFilterParameters, searchTestEntries } from "../../type/search-filter-parameters";
import { DashboardEntry, UserInfo } from "../../type/dashboard-entry";
import { SortMethod } from "../../type/sort-method";

@Injectable({
  providedIn: "root",
})
export class StubSearchService {
  constructor(
    private testEntries: DashboardEntry[],
    private mockUserInfo: { [key: number]: UserInfo }
  ) {}

  /**
   * retrieves a workflow from backend database given its id. The user in the session must have access to the workflow.
   * @param wid, the workflow id.
   */
  public search(
    keywords: string[],
    params: SearchFilterParameters,
    start: number,
    count: number,
    type: "workflow" | "project" | "file" | "dataset" | null,
    orderBy: SortMethod
  ): Observable<SearchResult> {
    // Igoring start count and orderBy as they are not tested in the unit tests.
    return new Observable(observer => {
      observer.next({
        results: searchTestEntries(keywords, params, this.testEntries, type).map(i => ({
          resourceType: i.type,
          workflow: i.type === "workflow" ? i.workflow : undefined,
          project: i.type === "project" ? i.project : undefined,
        })),
        more: false,
      });
    });
  }

  public getUserInfo(userIds: number[]): Observable<{ [key: number]: UserInfo }> {
    const result = userIds.reduce(
      (acc, id) => {
        if (this.mockUserInfo[id]) {
          acc[id] = this.mockUserInfo[id];
        }
        return acc;
      },
      {} as { [key: number]: UserInfo }
    );

    return of(result);
  }
}
