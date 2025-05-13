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

import { AfterViewInit, Component, Input, OnInit, ViewChild } from "@angular/core";
import { Router } from "@angular/router";
import { SearchResultsComponent } from "../../../dashboard/component/user/search-results/search-results.component";
import { FiltersComponent } from "../../../dashboard/component/user/filters/filters.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SortMethod } from "../../../dashboard/type/sort-method";
import { UserService } from "../../../common/service/user/user.service";
import { SearchService } from "../../../dashboard/service/user/search.service";
import { isDefined } from "../../../common/util/predicate";
import { firstValueFrom } from "rxjs";
import { DashboardEntry, UserInfo } from "../../../dashboard/type/dashboard-entry";

@UntilDestroy()
@Component({
  selector: "texera-hub-search",
  templateUrl: "./hub-search-result.component.html",
  styleUrls: ["./hub-search-result.component.scss"],
})
export class HubSearchResultComponent implements OnInit, AfterViewInit {
  public searchType: "dataset" | "workflow" = "workflow";
  currentUid = this.userService.getCurrentUser()?.uid;

  private isLogin = false;
  private includePublic = true;
  private _searchResultsComponent?: SearchResultsComponent;
  @ViewChild(SearchResultsComponent) get searchResultsComponent(): SearchResultsComponent {
    if (this._searchResultsComponent) {
      return this._searchResultsComponent;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set searchResultsComponent(value: SearchResultsComponent) {
    this._searchResultsComponent = value;
  }
  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent) get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }
  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.search() });
    this._filters = value;
  }
  private masterFilterList: ReadonlyArray<string> | null = null;

  @Input() public pid?: number = undefined;
  @Input() public accessLevel?: string = undefined;
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;

  constructor(
    private userService: UserService,
    private searchService: SearchService,
    private router: Router
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngOnInit() {
    const url = this.router.url;
    if (url.includes("dataset")) {
      this.searchType = "dataset";
    } else if (url.includes("workflow")) {
      this.searchType = "workflow";
    }
  }

  ngAfterViewInit() {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search());
  }

  /**
   * Searches dataset or workflow based on the `searchType` determined from the full URL.
   * @returns
   *
   * todo: Integrate the search functions from different interfaces into a single method.
   */
  async search(forced: boolean = false): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    let filterParams = this.filters.getSearchFilterParameters();
    if (isDefined(this.pid)) {
      // force the project id in the search query to be the current pid.
      filterParams.projectIds = [this.pid];
    }
    this.searchResultsComponent.reset(async (start, count) => {
      const results = await firstValueFrom(
        this.searchService.search(
          [""],
          filterParams,
          start,
          count,
          this.searchType,
          this.sortMethod,
          this.isLogin,
          this.includePublic
        )
      );

      const userIds = new Set<number>();
      results.results.forEach(i => {
        const ownerUid = this.searchType === "workflow" ? i.workflow?.ownerId : i.dataset?.dataset?.ownerUid;
        if (ownerUid !== undefined) {
          userIds.add(ownerUid);
        }
      });

      let userIdToInfoMap: { [key: number]: UserInfo } = {};
      if (userIds.size > 0) {
        userIdToInfoMap = await firstValueFrom(this.searchService.getUserInfo(Array.from(userIds)));
      }

      return {
        entries: results.results.map(i => {
          let entry;
          if (this.searchType === "workflow" && i.workflow) {
            entry = new DashboardEntry(i.workflow);
          } else if (this.searchType === "dataset" && i.dataset) {
            entry = new DashboardEntry(i.dataset);
          } else {
            throw new Error("Unexpected type in SearchResult.");
          }

          const ownerUid = this.searchType === "workflow" ? i.workflow?.ownerId : i.dataset?.dataset?.ownerUid;

          if (ownerUid !== undefined) {
            const userInfo = userIdToInfoMap[ownerUid] || { userName: "", googleAvatar: "" };
            entry.setOwnerName(userInfo.userName);
            entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
          }

          return entry;
        }),
        more: results.more,
      };
    });
    await this.searchResultsComponent.loadMore();
  }
}
