import { Component, AfterViewInit, ViewChild } from "@angular/core";
import { DashboardEntry } from "../../../type/dashboard-entry";
import { SearchService } from "../../../service/user/search.service";
import { FiltersComponent } from "../filters/filters.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { firstValueFrom } from "rxjs";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { SortMethod } from "../../../type/sort-method";
import { Location } from "@angular/common";
import { ActivatedRoute } from "@angular/router";

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
    private activatedRoute: ActivatedRoute
  ) {}

  ngAfterViewInit() {
    this.activatedRoute.queryParams.pipe(untilDestroyed(this)).subscribe(params => {
      const keyword = params["q"];
      if (keyword) {
        this.searchParam = keyword;
        this.updateMasterFilterList();
      }
    });
  }

  async search(): Promise<void> {
    if (this.filters.masterFilterList.length === 0) {
      return;
    }
    const sameList =
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList[i]);
    if (sameList && this.sortMethod === this.lastSortMethod) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.masterFilterList = this.filters.masterFilterList;
    this.lastSortMethod = this.sortMethod;
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
          null,
          this.sortMethod
        )
      );
      return {
        entries: results.results.map(i => {
          if (i.workflow) {
            return new DashboardEntry(i.workflow);
          } else if (i.project) {
            return new DashboardEntry(i.project);
          } else if (i.file) {
            return new DashboardEntry(i.file);
          } else if (i.dataset) {
            return new DashboardEntry(i.dataset);
          } else {
            throw new Error("Unexpected type in SearchResult.");
          }
        }),
        more: results.more,
      };
    });
    await this.searchResultsComponent.loadMore();
  }

  goBack(): void {
    this.location.back();
  }

  updateMasterFilterList() {
    this.filters.masterFilterList = this.searchParam.split(/\s+/);
  }
}
