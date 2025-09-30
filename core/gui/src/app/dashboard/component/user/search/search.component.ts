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

import { Component, AfterViewInit, ViewChild, ChangeDetectorRef } from "@angular/core";
import { SearchService } from "../../../service/user/search.service";
import { FiltersComponent } from "../filters/filters.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SortMethod } from "../../../type/sort-method";
import { Location } from "@angular/common";
import { ActivatedRoute } from "@angular/router";
import { UserService } from "../../../../common/service/user/user.service";
import { firstValueFrom } from "rxjs";
import { map } from "rxjs/operators";

@UntilDestroy()
@Component({
  selector: "texera-search",
  templateUrl: "./search.component.html",
  styleUrls: ["./search.component.scss"],
})
export class SearchComponent implements AfterViewInit {
  public searchParam: string = "";
  sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;
  private isLogin = this.userService.isLogin();
  private includePublic = true;
  currentUid = this.userService.getCurrentUser()?.uid;
  searchKeywords: string[] = [];

  selectedType: "project" | "workflow" | "dataset" | null = null;
  lastSelectedType: "project" | "workflow" | "dataset" | null = null;

  public masterFilterList: ReadonlyArray<string> = [];
  @ViewChild(SearchResultsComponent) searchResultsComponent?: SearchResultsComponent;
  private _filters?: FiltersComponent;
  @ViewChild(FiltersComponent)
  get filters(): FiltersComponent {
    if (this._filters) {
      return this._filters;
    }
    throw new Error("Property cannot be accessed before it is initialized.");
  }

  set filters(value: FiltersComponent) {
    value.masterFilterListChange.pipe(untilDestroyed(this)).subscribe({ next: () => this.search() });
    this._filters = value;
  }

  constructor(
    private location: Location,
    private searchService: SearchService,
    private userService: UserService,
    private activatedRoute: ActivatedRoute,
    private cdr: ChangeDetectorRef
  ) {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.isLogin = this.userService.isLogin();
        this.currentUid = this.userService.getCurrentUser()?.uid;
      });
  }

  ngAfterViewInit() {
    this.activatedRoute.queryParams.pipe(untilDestroyed(this)).subscribe(params => {
      const keyword = params["q"];
      if (keyword) {
        this.searchParam = keyword;
        this.updateMasterFilterList();
      }

      this.searchKeywords = this.filters.getSearchKeywords();
      this.cdr.detectChanges();
    });
  }

  async search(): Promise<void> {
    const sameList =
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList[i]);
    if (sameList && this.sortMethod === this.lastSortMethod && this.selectedType === this.lastSelectedType) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.masterFilterList = this.filters.masterFilterList;
    this.lastSortMethod = this.sortMethod;
    this.lastSelectedType = this.selectedType;
    if (!this.searchResultsComponent) {
      throw new Error("searchResultsComponent is undefined.");
    }
    this.searchResultsComponent.reset((start, count) => {
      return firstValueFrom(
        this.searchService
          .executeSearch(
            this.filters.getSearchKeywords(),
            this.filters.getSearchFilterParameters(),
            start,
            count,
            this.selectedType,
            this.sortMethod,
            this.isLogin,
            this.includePublic
          )
          .pipe(map(({ entries, more }) => ({ entries, more })))
      );
    });
    await this.searchResultsComponent.loadMore();
  }

  filterByType(type: "project" | "workflow" | "dataset" | null): void {
    this.selectedType = type;
    this.search();
  }

  goBack(): void {
    this.location.back();
  }

  updateMasterFilterList() {
    this.filters.masterFilterList = this.searchParam.split(/\s+/);
  }
}
