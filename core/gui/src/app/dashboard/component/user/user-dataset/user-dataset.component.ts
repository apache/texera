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
import { DASHBOARD_USER_DATASET_CREATE } from "../../../../app-routing.constant";

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
    private userService: UserService,
    private router: Router,
    private searchService: SearchService,
    private datasetService: DatasetService
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
  async search(forced: Boolean = false, filterScope: "all" | "public" | "private" = "all"): Promise<void> {
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

      const userIds = new Set<number>();
      results.results.forEach(i => {
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
        entries: results.results.map(i => {
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
    this.router.navigate([DASHBOARD_USER_DATASET_CREATE]);
  }

  public deleteDataset(entry: DashboardEntry): void {
    if (entry.dataset.dataset.did == undefined) {
      return;
    }
    this.datasetService
      .deleteDatasets([entry.dataset.dataset.did])
      .pipe(untilDestroyed(this))
      .subscribe(_ => {
        this.searchResultsComponent.entries = this.searchResultsComponent.entries.filter(
          datasetEntry => datasetEntry.dataset.dataset.did !== entry.dataset.dataset.did
        );
      });
  }
}
