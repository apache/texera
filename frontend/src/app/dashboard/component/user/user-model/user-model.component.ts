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
import { Router } from "@angular/router";
import { firstValueFrom } from "rxjs";
import { isDefined } from "../../../../common/util/predicate";
import { NzModalService } from "ng-zorro-antd/modal";
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
import { DashboardModel } from "../../../type/dashboard-model.interface";
import { USER_MODEL } from "../../../../app-routing.constant";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { CardItemComponent } from "../list-item/card-item/card-item.component";
import { SortButtonComponent } from "../sort-button/sort-button.component";
import { UserModelCreatorComponent } from "./user-model-creator/user-model-creator.component";

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
    SortButtonComponent,
    NzSelectComponent,
    FormsModule,
    SearchResultsComponent,
    CardItemComponent,
  ],
})
export class UserModelComponent implements AfterViewInit {
  private static readonly VIEW_MODE_STORAGE_KEY = "texera.userModel.viewMode";
  // Models carry no "last modified" timestamp, so sorting by edit time would leave the key NULL on
  // every row. Newest-first is the useful default, as on the Datasets page.
  public sortMethod = SortMethod.CreateTimeDesc;
  public isLogin = this.userService.isLogin();
  public currentUid = this.userService.getCurrentUser()?.uid;
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

  /** Free-text terms from the search box; matched client-side against the listed models. */
  public searchKeywords: string[] = [];

  private cachedModels: DashboardModel[] | null = null;

  constructor(
    private modalService: NzModalService,
    private userService: UserService,
    private router: Router,
    private searchService: SearchService,
    private modelService: ModelService
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
   * Re-filters and re-sorts the accessible models.
   *
   * Models are listed from /model/list and filtered here rather than through unified search: the
   * backend does not accept `resourceType=model` until the hub work lands, and models must not
   * surface in global search before then.
   *
   * @param forced discards the cached list and refetches
   */
  async search(forced: Boolean = false): Promise<void> {
    if (forced) {
      this.cachedModels = null;
    }

    this.searchResultsComponent.reset(async (start, count) => {
      const models = await this.accessibleModels();
      const matching = this.sortModels(models.filter(model => this.matchesKeyword(model)));
      const entries = matching.slice(start, start + count).map(model => new DashboardEntry(model));
      await this.attachOwnerNames(entries);
      return { entries, more: start + count < matching.length };
    });
    await this.searchResultsComponent.loadMore();
  }

  private async accessibleModels(): Promise<DashboardModel[]> {
    if (this.cachedModels === null) {
      this.cachedModels = await firstValueFrom(this.modelService.retrieveAccessibleModels());
    }
    return this.cachedModels;
  }

  private matchesKeyword(model: DashboardModel): boolean {
    const keywords = this.searchKeywords.map(keyword => keyword.trim().toLowerCase()).filter(keyword => keyword !== "");
    if (keywords.length === 0) {
      return true;
    }
    const haystack = [
      model.model.name,
      model.model.description,
      model.model.framework,
      model.model.format,
      model.ownerEmail,
    ]
      .filter((field): field is string => typeof field === "string")
      .map(field => field.toLowerCase());

    return keywords.every(keyword => haystack.some(field => field.includes(keyword)));
  }

  private sortModels(models: DashboardModel[]): DashboardModel[] {
    const byName = (a: DashboardModel, b: DashboardModel) => a.model.name.localeCompare(b.model.name);
    const byCreation = (a: DashboardModel, b: DashboardModel) =>
      (a.model.creationTime ?? 0) - (b.model.creationTime ?? 0);

    switch (this.sortMethod) {
      case SortMethod.NameAsc:
        return [...models].sort(byName);
      case SortMethod.NameDesc:
        return [...models].sort((a, b) => byName(b, a));
      default:
        return [...models].sort((a, b) => byCreation(b, a));
    }
  }

  /** Resolves owner uids to display names, so cards show a username rather than the placeholder. */
  private async attachOwnerNames(entries: DashboardEntry[]): Promise<void> {
    const ownerIds = Array.from(new Set(entries.map(entry => entry.model.model.ownerUid).filter(isDefined)));
    if (ownerIds.length === 0) {
      return;
    }
    try {
      const userInfo = await firstValueFrom(this.searchService.getUserInfo(ownerIds));
      entries.forEach(entry => {
        const info = userInfo[entry.model.model.ownerUid!];
        if (info) {
          entry.setOwnerName(info.userName);
          entry.setOwnerAvatar(info.avatar ?? "");
        }
      });
    } catch {
      // Decoration only: a lookup failure must not empty the list.
    }
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

    modal.afterClose.pipe(untilDestroyed(this)).subscribe(result => {
      if (result != null) {
        const created = result as DashboardModel;
        this.router.navigate([`${USER_MODEL}/${created.model.mid}`]);
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
        this.cachedModels = this.cachedModels?.filter(model => model.model.mid !== mid) ?? null;
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          modelEntry => modelEntry.model.model.mid !== mid
        );
      });
  }
}
