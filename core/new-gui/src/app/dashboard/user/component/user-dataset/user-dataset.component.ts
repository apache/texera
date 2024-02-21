import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AfterViewInit, Component, ViewChild } from "@angular/core";
import { SearchResultsComponent } from "../search-results/search-results.component";
import { FiltersComponent } from "../filters/filters.component";
import { UserService } from "../../../../common/service/user/user.service";
import { Router } from "@angular/router";
import { SearchService } from "../../service/search.service";
import { DatasetService } from "../../service/user-dataset/dataset.service";
import { firstValueFrom } from "rxjs";
import { DashboardEntry } from "../../type/dashboard-entry";
import { SortMethod } from "../../type/sort-method";

@UntilDestroy()
@Component({
  selector: "texera-dataset-section",
  templateUrl: "user-dataset.component.html",
  styleUrls: ["user-dataset.component.scss"],
})
export class UserDatasetComponent implements AfterViewInit {
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

  public sortMethod = SortMethod.NameAsc;
  lastSortMethod: SortMethod | null = null;

  constructor(
    private userService: UserService,
    private router: Router,
    private datasetService: DatasetService,
    private searchService: SearchService
  ) {}

  ngAfterViewInit() {
    this.reloadDashboardDatasetEntries();
  }

  /**
   * Searches datasets with keywords and filters given in the masterFilterList.
   * @returns
   */
  async search(forced: Boolean = false): Promise<void> {
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
    this.searchResultsComponent.reset(async (start, count) => {
      const results = await firstValueFrom(
        this.searchService.search(
          this.filters.getSearchKeywords(),
          filterParams,
          start,
          count,
          "dataset",
          this.sortMethod
        )
      );
      return {
        entries: results.results.map(i => {
          if (i.dataset) {
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

  public onClickOpenDatasetAddComponent(): void {
    this.router.navigate(["/dashboard/dataset/create"]);
  }

  private reloadDashboardDatasetEntries(forced: boolean = false): void {
    this.userService
      .userChanged()
      .pipe(untilDestroyed(this))
      .subscribe(() => this.search(forced));
  }

  public deleteDataset(entry: DashboardEntry) {
    if (entry.dataset.dataset.did) {
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

  public refreshDatasetItems(entry: DashboardEntry) {
    this.reloadDashboardDatasetEntries(true);
  }
}
