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
import { DashboardEntry } from "../../../type/dashboard-entry";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { FiltersComponent } from "../filters/filters.component";
import { firstValueFrom } from "rxjs";
import { DASHBOARD_USER_ML_MODEL } from "../../../../app-routing.constant";
import { NzModalService } from "ng-zorro-antd/modal";
import { UserDatasetVersionCreatorComponent } from "../user-dataset/user-dataset-explorer/user-dataset-version-creator/user-dataset-version-creator.component";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { NzMessageService } from "ng-zorro-antd/message";
import { map, tap } from "rxjs/operators";

@UntilDestroy()
@Component({
  selector: "texera-ml-model-section",
  templateUrl: "user-ml-model.component.html",
  styleUrls: ["user-ml-model.component.scss"],
})
export class UserMlModelComponent implements AfterViewInit {
  public sortMethod = SortMethod.EditTimeDesc;
  lastSortMethod: SortMethod | null = null;
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
  public hasMismatch = false;

  protected readonly DASHBOARD_USER_ML_MODEL = DASHBOARD_USER_ML_MODEL;

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

  async search(forced: Boolean = false, filterScope: "all" | "public" | "private" = "private"): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    if (!this.searchResultsComponent) {
      throw new Error("searchResultsComponent is undefined.");
    }
    const filterParams = this.filters.getSearchFilterParameters();
    const isLogin = filterScope === "public" ? false : this.isLogin;
    const includePublic = filterScope === "all" || filterScope === "public";

    this.searchResultsComponent.reset((start, count) => {
      return firstValueFrom(
        this.searchService
          .executeSearch(
            this.filters.getSearchKeywords(),
            filterParams,
            start,
            count,
            "dataset",
            this.sortMethod,
            isLogin,
            includePublic
          )
          .pipe(
            tap(({ hasMismatch }) => {
              this.hasMismatch = hasMismatch ?? false;
              if (this.hasMismatch) {
                this.message.warning(
                  "There is a mismatch between some ML models in the database and LakeFS. Only matched items are displayed.",
                  { nzDuration: 4000 }
                );
              }
            }),
            map(({ entries, more }) => ({ entries, more }))
          )
      );
    });
    await this.searchResultsComponent.loadMore();
  }

  public onClickOpenMlModelAddComponent(): void {
    const modal = this.modalService.create({
      nzTitle: "Create New ML Model",
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
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result != null) {
        const dashboardDataset: DashboardDataset = result as DashboardDataset;
        this.router.navigate([`${DASHBOARD_USER_ML_MODEL}/${dashboardDataset.asset.aid}`]);
      }
    });
  }

  public deleteMlModel(entry: DashboardEntry): void {
    if (entry.dataset?.asset?.aid == undefined) {
      return;
    }
    this.datasetService
      .deleteDatasets(entry.dataset.asset.aid)
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          datasetEntry => datasetEntry.dataset?.asset?.aid !== entry.dataset?.asset?.aid
        );
      });
  }
}
