import { Component, AfterViewInit, ViewChild, ChangeDetectorRef } from "@angular/core";
import { DashboardEntry, UserInfo } from "../../../type/dashboard-entry";
import { SearchService } from "../../../service/user/search.service";
import { FiltersComponent } from "../filters/filters.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { firstValueFrom } from "rxjs";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SortMethod } from "../../../type/sort-method";
import { Location } from "@angular/common";
import { ActivatedRoute } from "@angular/router";
import { UserService } from "../../../../common/service/user/user.service";

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
    if (this.filters.masterFilterList.length === 0) {
      return;
    }
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
    this.searchResultsComponent.reset(async (start, count) => {
      const results = await firstValueFrom(
        this.searchService.search(
          this.filters.getSearchKeywords(),
          this.filters.getSearchFilterParameters(),
          start,
          count,
          this.selectedType,
          this.sortMethod,
          this.isLogin,
          this.includePublic
        )
      );

      const userIds = new Set<number>();
      results.results.forEach(i => {
        if (i.project) {
          userIds.add(i.project.ownerId);
        } else if (i.dataset) {
          const ownerUid = i.dataset.dataset?.ownerUid;
          if (ownerUid !== undefined) {
            userIds.add(ownerUid);
          }
        } else if (i.workflow) {
          userIds.add(i.workflow.ownerId);
        }
      });

      let userIdToInfoMap: { [key: number]: UserInfo } = {};
      if (userIds.size > 0) {
        userIdToInfoMap = await firstValueFrom(this.searchService.getUserInfo(Array.from(userIds)));
      }

      return {
        entries: results.results.map(i => {
          let entry: DashboardEntry;

          if (i.workflow) {
            entry = new DashboardEntry(i.workflow);
            const userInfo = userIdToInfoMap[i.workflow.ownerId];
            if (userInfo) {
              entry.setOwnerName(userInfo.userName);
              entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
            }
          } else if (i.project) {
            entry = new DashboardEntry(i.project);
            const userInfo = userIdToInfoMap[i.project.ownerId];
            if (userInfo) {
              entry.setOwnerName(userInfo.userName);
              entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
            }
          } else if (i.dataset) {
            entry = new DashboardEntry(i.dataset);
            const ownerUid = i.dataset.dataset?.ownerUid;
            if (ownerUid !== undefined) {
              const userInfo = userIdToInfoMap[ownerUid];
              if (userInfo) {
                entry.setOwnerName(userInfo.userName);
                entry.setOwnerGoogleAvatar(userInfo.googleAvatar ?? "");
              }
            }
          } else {
            throw new Error("Unexpected type in SearchResult.");
          }

          return entry;
        }),
        more: results.more,
      };
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
