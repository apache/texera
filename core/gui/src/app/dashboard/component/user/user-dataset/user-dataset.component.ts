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

import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AfterViewInit, Component, ViewChild } from "@angular/core";
import { UserService } from "../../../../common/service/user/user.service";
import { Router } from "@angular/router";
import { SearchService } from "../../../service/user/search.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { SortMethod } from "../../../type/sort-method";
import { DashboardEntry, UserInfo } from "../../../type/dashboard-entry";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { FiltersComponent } from "../filters/filters.component";
import { firstValueFrom } from "rxjs";
import { DASHBOARD_USER_DATASET, DASHBOARD_USER_DATASET_CREATE } from "../../../../app-routing.constant";
import { NzModalService } from "ng-zorro-antd/modal";
import { FileSelectionComponent } from "../../../../workspace/component/file-selection/file-selection.component";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { UserDatasetVersionCreatorComponent } from "./user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { NzMessageService } from "ng-zorro-antd/message";

@UntilDestroy()
@Component({
  selector: "texera-dataset-section",
  templateUrl: "user-dataset.component.html",
  styleUrls: ["user-dataset.component.scss"],
})
export class UserDatasetComponent implements AfterViewInit {
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
  public hasMismatch = false; // Display warning when there are mismatched datasets

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
  constructor(
    private modalService: NzModalService,
    private userService: UserService,
    private router: Router,
    private searchService: SearchService,
    private datasetService: DatasetService,
    private message: NzMessageService
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
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search());
  }

  /*
   * Executes a dataset search with filtering, sorting.
   *
   * Parameters:
   * - filterScope = "all" | "public" | "private" - Determines visibility scope for search:
   *  - "all": includes all datasets, public and private
   *  - "public": limits the search to public datasets
   *  - "private": limits the search to dataset where the user has direct access rights.
   */
  async search(forced: Boolean = false, filterScope: "all" | "public" | "private" = "private"): Promise<void> {
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
    if (!this.searchResultsComponent) {
      throw new Error("searchResultsComponent is undefined.");
    }
    let filterParams = this.filters.getSearchFilterParameters();

    // if the filter requires only public datasets, the public search should be invoked, and the search method should
    // set the isLogin parameter to false in this case
    const isLogin = filterScope === "public" ? false : this.isLogin;
    const includePublic = filterScope === "all" || filterScope === "public";

    this.searchResultsComponent.reset(async (start, count) => {
      const results = await firstValueFrom(
        this.searchService.search(
          this.filters.getSearchKeywords(),
          filterParams,
          start,
          count,
          "dataset",
          this.sortMethod,
          isLogin,
          includePublic
        )
      );

      this.hasMismatch = results.hasMismatch ?? false;
      const filteredResults = results.results.filter(i => i !== null && i.dataset != null);

      if (this.hasMismatch) {
        this.message.warning(
          "There is a mismatch between some datasets in the database and LakeFS. Only matched datasets are displayed.",
          { nzDuration: 4000 }
        );
      }

      const userIds = new Set<number>();
      filteredResults.forEach(i => {
        const ownerUid = i.dataset?.dataset?.ownerUid;
        if (ownerUid !== undefined) {
          userIds.add(ownerUid);
        }
      });

      let userIdToInfoMap: { [key: number]: UserInfo } = {};
      if (userIds.size > 0) {
        userIdToInfoMap = await firstValueFrom(this.searchService.getUserInfo(Array.from(userIds)));
      }

      return {
        entries: filteredResults.map(i => {
          if (i.dataset) {
            const entry = new DashboardEntry(i.dataset);

            const ownerUid = i.dataset.dataset?.ownerUid;
            if (ownerUid !== undefined) {
              const userInfo = userIdToInfoMap[ownerUid] || { userName: "", googleAvatar: "" };
              entry.setOwnerName(userInfo.userName);
              entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
            }

            return entry;
          } else {
            throw new Error("Unexpected type in SearchResult.");
          }
        }),
        more: results.more,
      };
    });
    await this.searchResultsComponent.loadMore();
  }

  public onClickOpenDatasetAddComponent(): void {
    const modal = this.modalService.create({
      nzTitle: "Create New Dataset",
      nzContent: UserDatasetVersionCreatorComponent,
      nzFooter: null,
      nzData: {
        isCreatingVersion: false,
      },
      nzBodyStyle: {
        resize: "both",
        overflow: "auto",
        minHeight: "200px",
        minWidth: "550px",
        maxWidth: "90vw",
        maxHeight: "80vh",
      },
      nzWidth: "fit-content",
    });
    // Handle the selection from the modal
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result != null) {
        const dashboardDataset: DashboardDataset = result as DashboardDataset;
        this.router.navigate([`${DASHBOARD_USER_DATASET}/${dashboardDataset.dataset.did}`]);
      }
    });
  }

  public deleteDataset(entry: DashboardEntry): void {
    if (entry.dataset.dataset.did == undefined) {
      return;
    }
    this.datasetService
      .deleteDatasets(entry.dataset.dataset.did)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          datasetEntry => datasetEntry.dataset.dataset.did !== entry.dataset.dataset.did
        );
      });
  }
}
