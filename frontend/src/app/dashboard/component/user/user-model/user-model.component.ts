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
import { firstValueFrom } from "rxjs";
import { map, tap } from "rxjs/operators";
import { NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzSpaceCompactItemDirective, NzSpaceCompactComponent } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSelectComponent } from "ng-zorro-antd/select";
import { FormsModule } from "@angular/forms";
import { UserService } from "../../../../common/service/user/user.service";
import { SearchService } from "../../../service/user/search.service";
import { ModelService } from "../../../service/user/model/model.service";
import { SortMethod } from "../../../type/sort-method";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { CardItemComponent } from "../list-item/card-item/card-item.component";
import { FiltersComponent } from "../filters/filters.component";
import { FiltersInstructionsComponent } from "../filters-instructions/filters-instructions.component";
import { SortButtonComponent } from "../sort-button/sort-button.component";
import { UserModelCreatorComponent } from "./user-model-creator/user-model-creator.component";
import { EntityType } from "../../../../hub/service/hub.service";

@UntilDestroy()
@Component({
  selector: "texera-model-section",
  templateUrl: "user-model.component.html",
  styleUrls: ["user-model.component.scss"],
  imports: [
    NzCardComponent,
    NzSpaceCompactItemDirective,
    NzSpaceCompactComponent,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    FiltersComponent,
    FiltersInstructionsComponent,
    SortButtonComponent,
    NzSelectComponent,
    FormsModule,
    SearchResultsComponent,
    CardItemComponent,
  ],
})
export class UserModelComponent implements AfterViewInit {
  public readonly entityType = EntityType.Model;
  private static readonly VIEW_MODE_STORAGE_KEY = "texera.userModel.viewMode";
  // Models carry no "last modified" timestamp, so sorting by edit time would leave the key NULL on
  // every row. Newest-first is the useful default, as on the Datasets page.
  public sortMethod = SortMethod.CreateTimeDesc;
  lastSortMethod: SortMethod | null = null;
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
  public hasMismatch = false; // Display a warning when some models could not be matched in LakeFS
  public viewType: "list" | "card" =
    localStorage.getItem(UserModelComponent.VIEW_MODE_STORAGE_KEY) === "list" ? "list" : "card";

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
    private searchService: SearchService,
    private modelService: ModelService,
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

  public setViewType(viewType: "list" | "card"): void {
    if (this.viewType === viewType) {
      return;
    }
    this.viewType = viewType;
    localStorage.setItem(UserModelComponent.VIEW_MODE_STORAGE_KEY, viewType);
  }

  /**
   * Runs a model search, with the keywords and filter parameters the filters bar holds.
   *
   * @param forced searches again even when neither the filter list nor the sort method changed
   */
  async search(forced: Boolean = false): Promise<void> {
    const sameList =
      this.masterFilterList !== null &&
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList![i]);
    if (!forced && sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do not make the same request again.
      return;
    }
    this.lastSortMethod = this.sortMethod;
    this.masterFilterList = this.filters.masterFilterList;
    const filterParams = this.filters.getSearchFilterParameters();

    this.searchResultsComponent.reset((start, count) => {
      return firstValueFrom(
        this.searchService
          .executeSearch(
            this.filters.getSearchKeywords(),
            filterParams,
            start,
            count,
            "model",
            this.sortMethod,
            this.isLogin,
            // This page shows what you own or were granted, never what is merely public.
            false
          )
          .pipe(
            tap(({ hasMismatch }) => {
              this.hasMismatch = hasMismatch ?? false;
              if (this.hasMismatch) {
                this.message.warning(
                  "There is a mismatch between some models in the database and LakeFS. Only matched models are displayed.",
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

  public onClickOpenModelAddComponent(): void {
    const modal = this.modalService.create({
      nzTitle: "Create New Model",
      nzContent: UserModelCreatorComponent,
      nzFooter: null,
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

    // Refreshes rather than opening the new model: it has no detail page yet.
    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result != null) {
        void this.search(true);
      }
    });
  }

  public deleteModel(entry: DashboardEntry): void {
    const mid = entry.model.model.mid;
    if (mid === undefined) {
      return;
    }
    this.modelService
      .deleteModel(mid)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          modelEntry => modelEntry.model.model.mid !== mid
        );
      });
  }
}
