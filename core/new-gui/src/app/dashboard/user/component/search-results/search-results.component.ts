import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DashboardEntry } from "../../type/dashboard-entry";

export type LoadMoreFunction = (start: number, count: number) => Promise<{ entries: DashboardEntry[]; more: boolean }>;

@Component({
  selector: "texera-search-results",
  templateUrl: "./search-results.component.html",
  styleUrls: ["./search-results.component.scss"],
})
export class SearchResultsComponent {
  loadMoreFunction: LoadMoreFunction | null = null;
  loading = false;
  more = false;
  entries: ReadonlyArray<DashboardEntry> = [];
  @Input() public pid: number = 0;
  @Input() editable = false;
  @Output() deleted = new EventEmitter<DashboardEntry>();
  @Output() duplicated = new EventEmitter<DashboardEntry>();
  constructor() {}

  reset(loadMoreFunction: LoadMoreFunction): void {
    this.entries = [];
    this.loadMoreFunction = loadMoreFunction;
  }

  async loadMore(): Promise<void> {
    if (!this.loadMoreFunction) {
      throw new Error("This is an empty list and cannot load more entries.");
    }
    this.loading = true;
    try {
      const results = await this.loadMoreFunction(this.entries.length, 20);
      this.entries = [...this.entries, ...results.entries];
      this.more = results.more;
    } finally {
      this.loading = false;
    }
  }
}
