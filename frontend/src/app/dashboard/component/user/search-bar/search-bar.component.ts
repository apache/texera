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

import { Component } from "@angular/core";
import { Router } from "@angular/router";
import { SearchService } from "../../../service/user/search.service";
import { SearchFilterParameters } from "../../../type/search-filter-parameters";
import { SortMethod } from "../../../type/sort-method";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SearchResult, SearchResultItem, searchResultPayload } from "../../../type/search-result";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { EMPTY, Observable, of, Subject } from "rxjs";
import { catchError, debounceTime, defaultIfEmpty, switchMap } from "rxjs/operators";
import { UserService } from "../../../../common/service/user/user.service";
import { SEARCH } from "../../../../app-routing.constant";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzInputGroupComponent, NzInputDirective } from "ng-zorro-antd/input";
import { FormsModule } from "@angular/forms";
import { NzAutocompleteTriggerDirective, NzAutocompleteComponent } from "ng-zorro-antd/auto-complete";

@UntilDestroy()
@Component({
  selector: "texera-search-bar",
  templateUrl: "./search-bar.component.html",
  styleUrls: ["./search-bar.component.scss"],
  imports: [
    ɵNzTransitionPatchDirective,
    NzSpaceCompactItemDirective,
    NzInputGroupComponent,
    NzInputDirective,
    FormsModule,
    NzAutocompleteTriggerDirective,
    NzAutocompleteComponent,
  ],
})
export class SearchBarComponent {
  private includePublic = true;
  public searchParam: string = "";
  public listOfResult: string[] = [];
  private searchSubject = new Subject<string>();
  isLogin = this.userService.isLogin();

  private params: SearchFilterParameters = {
    createDateStart: null,
    createDateEnd: null,
    modifiedDateStart: null,
    modifiedDateEnd: null,
    owners: [],
    ids: [],
    operators: [],
  };

  private searchCache = new Map<string, string[]>();

  constructor(
    private router: Router,
    private searchService: SearchService,
    private userService: UserService
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
      });
    this.searchSubject
      .pipe(
        debounceTime(200),
        switchMap(query => this.getSearchResults(query)),
        untilDestroyed(this)
      )
      .subscribe((results: string[]) => {
        this.listOfResult = results;
      });
  }

  // Method to get search results with caching and limit cache size
  private getSearchResults(query: string): Observable<string[]> {
    if (this.searchCache.has(query)) {
      return of(this.searchCache.get(query)!);
    } else {
      const searchObservable = this.searchService.search(
        [query],
        this.params,
        0,
        5,
        null,
        SortMethod.NameAsc,
        this.isLogin,
        this.includePublic
      );

      // Only the request is guarded, as in FiltersComponent: a failed search clears the suggestions and caches
      // nothing, so typing the query again retries. A row convertToName cannot name is a bug, so it is not
      // caught: it ends the subscription in the constructor and reaches the console.
      return searchObservable.pipe(
        catchError(() => EMPTY),
        switchMap((result: SearchResult) => {
          const uniqueResults = Array.from(new Set(result.results.map(item => this.convertToName(item))));
          this.addToCache(query, uniqueResults);
          return of(uniqueResults);
        }),
        defaultIfEmpty([] as string[])
      );
    }
  }

  // A Map iterates in insertion order, and addToCache is only reached on a cache
  // miss, so the oldest key is always the first one.
  private addToCache(query: string, results: string[]): void {
    if (this.searchCache.size >= 20) {
      const oldestQuery = this.searchCache.keys().next().value;
      this.searchCache.delete(oldestQuery!);
    }
    this.searchCache.set(query, results);
  }

  onSearchInputChange(query: string): void {
    if (query) {
      this.searchSubject.next(query);
    } else {
      this.listOfResult = [];
    }
  }

  performSearch(keyword: string) {
    this.router.navigate([SEARCH], { queryParams: { q: keyword } });
  }

  convertToName(resultItem: SearchResultItem): string {
    const payload = searchResultPayload(resultItem);
    if (payload === undefined) {
      throw new Error("Unexpected type in SearchResult.");
    }
    return new DashboardEntry(payload).name;
  }
}
