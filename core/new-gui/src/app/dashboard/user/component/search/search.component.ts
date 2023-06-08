import { Component, OnInit, ViewChild } from "@angular/core";
import { DashboardEntry } from "../../type/dashboard-entry";
import { SearchService } from "../../service/search.service";
import { FiltersComponent } from "../filters/filters.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { firstValueFrom } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-search",
  templateUrl: "./search.component.html",
  styleUrls: ["./search.component.scss"],
})
export class SearchComponent {
  entries: ReadonlyArray<DashboardEntry> = [];
  private masterFilterList: string[] = [];
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

  constructor(private searchService: SearchService) {}

  async search(): Promise<void> {
    if (this.filters.masterFilterList.length === 0) {
      return;
    }
    const sameList =
      this.filters.masterFilterList.length === this.masterFilterList.length &&
      this.filters.masterFilterList.every((v, i) => v === this.masterFilterList[i]);
    if (sameList) {
      // If the filter lists are the same, do no make the same request again.
      return;
    }
    this.masterFilterList = this.filters.masterFilterList;
    const results = await firstValueFrom(
      this.searchService.search(this.filters.getSearchKeywords(), this.filters.getSearchFilterParameters())
    );
    this.entries = results
      .filter(i => i.resourceType !== "file")
      .map(i => {
        if (i.workflow) {
          return new DashboardEntry(i.workflow);
        } else if (i.project) {
          return new DashboardEntry(i.project);
        } else {
          throw new Error("Unexpected type in SearchResult.");
        }
      });
  }
}
